# sync_etsy_to_sheets.py
import os, re, json, time, random
from datetime import datetime
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse

import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# ========= Config =========
ETSY_SHOP_URL  = os.getenv("ETSY_SHOP_URL", "").strip()   # ej: https://www.etsy.com/shop/TuTienda
SHEET_ID       = os.getenv("SHEET_ID", "").strip()
SHEET_TAB      = os.getenv("SHEET_TAB", "etsy_items").strip()
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON", "").strip()

if not (ETSY_SHOP_URL and SHEET_ID and GOOGLE_SA_JSON):
    raise SystemExit("Faltan variables: ETSY_SHOP_URL, SHEET_ID o GOOGLE_SA_JSON")

ORIGIN = f"{urlparse(ETSY_SHOP_URL).scheme}://{urlparse(ETSY_SHOP_URL).netloc}"

# Columnas
HEADERS = [
    "id", "title", "price", "currency", "availability",
    "category", "tags", "url", "image", "description",
    "shop_name", "shop_url", "timestamp_utc",
]

# ========= Sheets =========
def get_ws():
    creds = Credentials.from_service_account_info(
        json.loads(GOOGLE_SA_JSON),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet(SHEET_TAB)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_TAB, rows=2, cols=len(HEADERS) + 5)
    return ws

def write_headers(ws):
    ws.clear()
    ws.update(range_name=f"A1:{_col_letter(len(HEADERS))}1", values=[HEADERS])

def write_rows(ws, rows: List[Dict[str, Any]]):
    if not rows:
        return
    values = [[r.get(k, "") for k in HEADERS] for r in rows]
    need = len(values) - (ws.row_count - 1)
    if need > 0:
        ws.add_rows(need)
    ws.update(range_name=f"A2:{_col_letter(len(HEADERS))}{len(values)+1}", values=values, value_input_option="RAW")

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

# ========= Helpers Etsy =========
LISTING_ID_RE = re.compile(r"/listing/(\d+)")
RATE_LIMIT_MARKERS = ("robot check", "verify you are a human", "are you a human", "unusual traffic")

def accept_cookies_if_any(page):
    # Intenta varios textos comunes (ES/EN)
    candidates = [
        "Accept", "Aceptar", "Allow essential", "Permitir", "Aceptar todo",
        "Aceptar cookies", "Accept all cookies"
    ]
    for label in candidates:
        try:
            page.locator(f"button:has-text('{label}')").first.click(timeout=1500)
            page.wait_for_timeout(300)
            return
        except Exception:
            pass
    # Banner moderno: data-gdpr-* (best-effort)
    try:
        page.locator("form[data-gdpr] button[type=submit]").first.click(timeout=1000)
        page.wait_for_timeout(300)
    except Exception:
        pass

def is_blocked(title: str) -> bool:
    t = (title or "").lower()
    return any(x in t for x in RATE_LIMIT_MARKERS)

def backoff(attempt: int):
    # 1-> ~3-5s, 2-> ~8-10s, 3-> ~16-20s
    base = min(20, 3 * (2 ** (attempt - 1)))
    time.sleep(base + random.uniform(0, 2))

def parse_json_ld(text: str) -> Dict[str, Any]:
    """Devuelve dict normalizado a partir de JSON-LD de Etsy (Product/AggregateOffer)."""
    out: Dict[str, Any] = {}
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return out
    nodes = data if isinstance(data, list) else [data]
    for node in nodes:
        if not isinstance(node, dict):
            continue
        t = node.get("@type")
        # Product node
        if (isinstance(t, list) and "Product" in t) or t == "Product":
            out["title"] = node.get("name", "")
            out["description"] = node.get("description", "")
            img = node.get("image", "")
            if isinstance(img, list):
                img = img[0] if img else ""
            out["image"] = img or ""

            # Offers: Offer o AggregateOffer
            offers = node.get("offers") or {}
            if isinstance(offers, list):
                offers = offers[0] if offers else {}
            price = ""
            currency = ""
            availability = ""
            if offers:
                availability = offers.get("availability", "")
                if "AggregateOffer" in str(offers.get("@type")):
                    price = str(offers.get("lowPrice") or offers.get("highPrice") or "")
                    currency = offers.get("priceCurrency", "") or ""
                else:
                    price = str(offers.get("price", "") or "")
                    currency = offers.get("priceCurrency", "") or ""
            out["price"] = price
            out["currency"] = currency
            out["availability"] = availability

            # Others
            out["url"] = node.get("url", "")
            out["category"] = ""
            cats = node.get("category")
            if isinstance(cats, list):
                out["category"] = " > ".join(cats)
            elif isinstance(cats, str):
                out["category"] = cats
            out["tags"] = ""
            kws = node.get("keywords")
            if isinstance(kws, list):
                out["tags"] = ", ".join(kws)
            elif isinstance(kws, str):
                out["tags"] = kws
            # listing id sometimes appears as sku/gtin/productID
            out["id"] = str(node.get("sku") or node.get("productID") or "")
            return out
    return out

def extract_text(page, sel: str, timeout=1200) -> str:
    try:
        el = page.wait_for_selector(sel, timeout=timeout)
        return (el.inner_text() or "").strip()
    except Exception:
        return ""

def extract_attr(page, sel: str, attr: str, timeout=1200) -> str:
    try:
        el = page.wait_for_selector(sel, timeout=timeout)
        return (el.get_attribute(attr) or "").strip()
    except Exception:
        return ""

def fallback_from_dom(page) -> Dict[str, Any]:
    data = {}
    # Título
    for s in ["h1[data-buy-box-listing-title]", "h1[data-listing-page-title]", "h1"]:
        t = extract_text(page, s)
        if t:
            data["title"] = t
            break
    # Precio (varía según plantilla)
    for s in [
        "[data-buy-box-region='price'] p",
        "p[data-buy-box-price]",
        "span.wt-text-title-03",
        "[data-appears-component-name='price'] span"
    ]:
        p = extract_text(page, s)
        if p:
            data["price"] = re.sub(r"[^\d,.\-]", "", p).replace(",", ".")
            break
    # Moneda
    txt = extract_text(page, "[data-buy-box-region='price'], p[data-buy-box-price], .wt-text-title-03")
    if "€" in txt:
        data["currency"] = "EUR"
    elif "$" in txt:
        data["currency"] = "USD"
    elif "£" in txt:
        data["currency"] = "GBP"

    # Imagen
    data["image"] = (
        extract_attr(page, "img[data-listing-image]", "src")
        or extract_attr(page, "img[data-palette-listing-image]", "src")
        or extract_attr(page, "figure img", "src")
    )

    # Disponibilidad / categoría / tags (best-effort)
    if not data.get("availability"):
        if "Sold out" in page.content() or "Agotado" in page.content():
            data["availability"] = "SoldOut"

    cat = extract_text(page, "nav[aria-label*='Breadcrumb'] li:last-child a, nav[aria-label*='Migas'] li:last-child a")
    if cat:
        data["category"] = cat

    return data

def collect_shop_item_urls(page, shop_url: str) -> (str, str, List[str]):
    """
    Recorre las páginas de la tienda (?page=2,3,...) y extrae los enlaces a /listing/<id>/
    """
    urls: set[str] = set()
    page_num = 1
    shop_name = ""
    while True:
        url = shop_url if page_num == 1 else (shop_url + (("&" if "?" in shop_url else "?") + f"page={page_num}"))
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        if page_num == 1:
            accept_cookies_if_any(page)
            try:
                shop_name = extract_text(page, "h1[data-ui='shop-name'], h1.wt-text-heading-01, h1")
            except Exception:
                shop_name = ""
        # recoge listings
        hrefs = []
        try:
            hrefs = page.eval_on_selector_all("a[href*='/listing/']", "els => els.map(e => e.getAttribute('href'))")
        except Exception:
            pass
        added = 0
        for h in hrefs or []:
            if not h:
                continue
            # normaliza a URL absoluta
            if h.startswith("/"):
                full = ORIGIN + h
            elif h.startswith("http"):
                full = h
            else:
                full = ORIGIN + "/" + h
            # limpiamos query
            full = full.split("?")[0]
            if "/listing/" in full:
                if full not in urls:
                    urls.add(full)
                    added += 1

        # ¿hay siguiente página?
        has_next = False
        try:
            # botón/enlace "Next"
            nxt = page.locator("a[aria-label*='Next'], a[aria-label*='Siguiente'], nav ul li a[rel='next']").first
            if nxt and nxt.is_enabled():
                has_next = True
        except Exception:
            pass

        # criterio de parada
        if added == 0 and not has_next:
            break

        page_num += 1
        if page_num > 50:  # tope seguridad
            break

        page.wait_for_timeout(800 + int(random.uniform(0, 400)))

    return (shop_name or ""), shop_url, sorted(urls)

def fetch_item(page, url: str, shop_name: str, shop_url: str) -> Dict[str, Any]:
    # reintentos por bloqueo
    for attempt in range(1, 4):
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        title_page = page.title()
        if is_blocked(title_page):
            if attempt < 3:
                backoff(attempt)
                continue
        break

    # JSON-LD
    parsed: Dict[str, Any] = {}
    try:
        scripts = page.query_selector_all('script[type="application/ld+json"]')
        for s in scripts:
            txt = s.text_content()
            if not txt:
                continue
            parsed = parse_json_ld(txt)
            if parsed.get("title"):
                break
    except Exception:
        parsed = {}

    # Fallback DOM
    if not parsed.get("title"):
        parsed = {**fallback_from_dom(page), **parsed}

    # ID desde URL si falta
    listing_id = parsed.get("id") or ""
    if not listing_id:
        m = LISTING_ID_RE.search(url)
        if m:
            listing_id = m.group(1)

    row = {
        "id": listing_id,
        "title": parsed.get("title", ""),
        "price": parsed.get("price", ""),
        "currency": parsed.get("currency", ""),
        "availability": parsed.get("availability", ""),
        "category": parsed.get("category", ""),
        "tags": parsed.get("tags", ""),
        "url": parsed.get("url") or url,
        "image": parsed.get("image", ""),
        "description": parsed.get("description", ""),
        "shop_name": shop_name,
        "shop_url": shop_url,
        "timestamp_utc": datetime.utcnow().isoformat(),
    }
    return row

# ========= Main =========
def run():
    ws = get_ws()
    write_headers(ws)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"),
            locale="es-ES",
            viewport={"width": 1280, "height": 2000},
        )
        page = context.new_page()

        shop_name, shop_url, item_urls = collect_shop_item_urls(page, ETSY_SHOP_URL)
        print(f"Encontrados {len(item_urls)} listings en la tienda '{shop_name or 'N/D'}'.")

        rows: List[Dict[str, Any]] = []
        for i, url in enumerate(item_urls, 1):
            try:
                row = fetch_item(page, url, shop_name, shop_url)
                rows.append(row)
            except Exception as e:
                print(f"Error en {url}: {e}")
            # pausas para no disparar bloqueos
            time.sleep(random.uniform(0.7, 1.4))
            if i % 25 == 0:
                time.sleep(random.uniform(5, 8))

        context.close()
        browser.close()

    write_rows(ws, rows)
    print("Finalizado.")

if __name__ == "__main__":
    run()
