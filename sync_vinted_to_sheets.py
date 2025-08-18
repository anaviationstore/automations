# sync_vinted_to_sheets.py
import os, re, json, time
import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright

# ---------- Config ----------
VINTED_DOMAIN = os.getenv("VINTED_DOMAIN", "es").strip()
ENV_USER_ID   = os.getenv("VINTED_USER_ID", "").strip()             # opcional
ENV_PROFILE   = os.getenv("VINTED_PROFILE_URL", "").strip()         # ej: https://www.vinted.es/member/279020986
SHEET_ID      = os.getenv("SHEET_ID")
SHEET_TAB     = os.getenv("SHEET_TAB", "vinted_items")
GOOGLE_SA_JSON= os.getenv("GOOGLE_SA_JSON")

if not (SHEET_ID and GOOGLE_SA_JSON):
    raise SystemExit("Faltan variables: SHEET_ID o GOOGLE_SA_JSON")

# ---------- Google Sheets ----------
creds = Credentials.from_service_account_info(json.loads(GOOGLE_SA_JSON),
                                              scopes=["https://www.googleapis.com/auth/spreadsheets"])
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
try:
    ws = sh.worksheet(SHEET_TAB)
except gspread.exceptions.WorksheetNotFound:
    ws = sh.add_worksheet(title=SHEET_TAB, rows=2, cols=8)

HEADERS = ["id","title","price","currency","url","brand","size","status"]

def write_headers():
    ws.clear()
    ws.update(range_name="A1:H1", values=[HEADERS])

def write_rows(items):
    if not items:
        return
    rows = [[
        it.get("id",""), it.get("title",""), it.get("price",""), it.get("currency",""),
        it.get("url",""), it.get("brand",""), it.get("size",""), it.get("status",""),
    ] for it in items]
    need = len(rows) - (ws.row_count - 1)
    if need > 0:
        ws.add_rows(need)
    ws.update(range_name=f"A2:H{len(rows)+1}", values=rows)

# ---------- Utilidades precio/moneda ----------
CURRENCY_MAP = {
    "€": "EUR", "EUR": "EUR",
    "$": "USD", "USD": "USD",
    "£": "GBP", "GBP": "GBP",
    "zł": "PLN", "PLN": "PLN",
    "Kč": "CZK", "CZK": "CZK",
    "Ft": "HUF", "HUF": "HUF",
    "lei": "RON", "RON": "RON",
    "CHF": "CHF",
    "SEK": "SEK", "NOK": "NOK", "DKK": "DKK",
}

def default_currency_for_domain(domain: str) -> str:
    d = domain.split(".")[-1].lower()
    return {
        "es":"EUR","fr":"EUR","de":"EUR","it":"EUR","pt":"EUR","nl":"EUR","be":"EUR","ie":"EUR","lt":"EUR","lv":"EUR","ee":"EUR",
        "pl":"PLN","cz":"CZK","hu":"HUF","ro":"RON",
        "uk":"GBP","gb":"GBP",
        "se":"SEK","dk":"DKK","no":"NOK",
        "ch":"CHF",
    }.get(d, "EUR")

PRICE_PATTERNS = [
    re.compile(r'(\d{1,3}(?:[.\s]\d{3})*(?:[.,]\d{1,2})?)\s*(€|EUR|\$|USD|£|GBP|zł|PLN|Kč|CZK|Ft|HUF|lei|RON|CHF|SEK|NOK|DKK)', re.I),
    re.compile(r'(€|EUR|\$|USD|£|GBP|zł|PLN|Kč|CZK|Ft|HUF|lei|RON|CHF|SEK|NOK|DKK)\s*(\d{1,3}(?:[.\s]\d{3})*(?:[.,]\d{1,2})?)', re.I),
]

def parse_price_currency_from_text(text: str, domain_hint: str):
    t = (text or "").replace("\xa0"," ").strip()
    if not t: 
        return "", ""
    for pat in PRICE_PATTERNS:
        m = pat.search(t)
        if m:
            if len(m.groups()) == 2:
                a, b = m.group(1), m.group(2)
                if any(sym in a for sym in ("€","$","£")) or a.upper() in CURRENCY_MAP:
                    # patrón símbolo primero
                    curr_raw, val_raw = a, b
                else:
                    val_raw, curr_raw = a, b
            else:
                continue
            # normaliza valor: quitar separadores de miles y usar '.' decimal
            val = val_raw.replace(" ", "").replace("\u202f","").replace(".", "").replace(",", ".")
            # si tenía formato 1.234,56 y nos hemos cargado miles, arriba ya hicimos replace(".", "")
            # si tenía 1234.56 no se rompe (ya no tiene '.')
            try:
                # validar que es numérico
                float(val)
            except Exception:
                pass
            curr = CURRENCY_MAP.get(curr_raw, CURRENCY_MAP.get(curr_raw.upper(), ""))
            if not curr:
                curr = default_currency_for_domain(domain_hint)
            return val, curr
    return "", ""

# ---------- Playwright helpers ----------
ITEM_ID_RE = re.compile(r'(?:^|/)(?:items)/(\d+)(?:-|$)')

def collect_item_ids_with_browser(profile_url: str, domain: str) -> list[str]:
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

        url = profile_url
        if not url.startswith("http"):
            url = f"https://www.vinted.{domain}/member/{url}"
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}status=active&order=newest_first"

        print("[pw] goto", url)
        page.goto(url, wait_until="networkidle", timeout=60_000)

        try:
            page.locator("role=button[name=/Activos/i]").first.click(timeout=2_000)
        except Exception:
            pass

        seen_ids: set[str] = set()
        stable_rounds = 0
        for i in range(100):
            hrefs = page.eval_on_selector_all("a[href]", "els => els.map(e => e.getAttribute('href'))")
            added = 0
            for h in hrefs:
                if not h: 
                    continue
                m = ITEM_ID_RE.search(h)
                if m:
                    iid = m.group(1)
                    if iid not in seen_ids:
                        seen_ids.add(iid)
                        added += 1
            print(f"[pw] scroll {i+1}: total_ids={len(seen_ids)} (+{added})")

            if added == 0:
                stable_rounds += 1
            else:
                stable_rounds = 0
            if stable_rounds >= 4:
                break

            page.evaluate("""
                const el = document.scrollingElement || document.documentElement || document.body;
                el.scrollTo(0, el.scrollHeight);
            """)
            page.wait_for_timeout(1200)
            try:
                page.wait_for_load_state("networkidle", timeout=5_000)
            except Exception:
                pass

        context.storage_state(path="playwright_state.json")
        browser.close()
        return list(seen_ids)

def _get_meta(page, prop):
    try:
        return page.eval_on_selector(f'meta[property="{prop}"]', "el => el ? el.content : null")
    except Exception:
        return None

def _parse_attributes_map(page):
    try:
        return page.evaluate("""
() => {
  const out = {};
  document.querySelectorAll('dt').forEach(dt => {
    const key = (dt.textContent || '').trim().toLowerCase();
    const dd = dt.nextElementSibling;
    if (!key || !dd) return;
    let val = (dd.textContent || '').trim();
    const a = dd.querySelector('a'); if (a) val = (a.textContent || '').trim();
    const span = dd.querySelector('span'); if (span && (!val || val.length < span.textContent.length)) val = (span.textContent || '').trim();
    out[key] = val;
  });
  return out;
}
""")
    except Exception:
        return {}

def _pick_attr(attr_map, variants):
    variants = [v.lower() for v in variants]
    for k, v in attr_map.items():
        if k.lower() in variants:
            return v
    for k, v in attr_map.items():
        lk = k.lower()
        if any(lk.startswith(pfx) or pfx in lk for pfx in variants):
            return v
    return ""

def _price_from_dom(page, domain_hint: str):
    """
    Fallback final: busca nodos con 'price' en data-testid / class o cualquier nodo que contenga símbolo/ISO.
    Devuelve (valor, moneda) ya normalizados si encuentra algo.
    """
    texts = []
    try:
        # candidatos obvios
        texts += page.eval_on_selector_all('[data-testid*="price" i]', "els => els.map(e => (e.textContent||'').trim())")
    except Exception:
        pass
    try:
        texts += page.eval_on_selector_all('[class*="price" i]', "els => els.map(e => (e.textContent||'').trim())")
    except Exception:
        pass
    # si nada, escanea algunos spans/divs visibles
    if not any(texts):
        try:
            texts += page.eval_on_selector_all('span,div', "els => els.slice(0,400).map(e => (e.textContent||'').trim()).filter(Boolean)")
        except Exception:
            pass

    for t in texts:
        val, curr = parse_price_currency_from_text(t, domain_hint)
        if val or curr:
            return val, curr
    return "", ""

def fetch_item_detail_with_browser(item_id: str, domain: str) -> dict:
    """
    1) Intenta API JSON con cookies del navegador.
    2) Si no, JSON-LD.
    3) Si no, metatags OpenGraph/product.
    4) Si no, DOM (clases/testid con 'price' o cualquier nodo con símbolo/ISO).
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(storage_state="playwright_state.json")
        req = context.request

        # ---- Intento 1: API JSON
        for url in (
            f"https://www.vinted.{domain}/api/v2/items/{item_id}",
            f"https://www.vinted.{domain}/web/api/v2/items/{item_id}",
        ):
            r = req.get(url, timeout=30_000)
            if r.ok:
                try:
                    data = r.json()
                    obj = data.get("item") or data.get("data") or data
                    if isinstance(obj, dict):
                        price_field = obj.get("price", "")
                        if isinstance(price_field, dict):
                            price_val = price_field.get("amount", "")
                            currency = price_field.get("currency_code", "")
                        else:
                            price_val = price_field
                            currency = obj.get("currency") or obj.get("currency_code", "")
                        url_item = obj.get("url") or f"https://www.vinted.{domain}/items/{item_id}"
                        browser.close()
                        return {
                            "id": obj.get("id", item_id),
                            "title": obj.get("title",""),
                            "price": price_val,
                            "currency": currency or default_currency_for_domain(domain),
                            "url": url_item,
                            "brand": obj.get("brand_title",""),
                            "size": obj.get("size_title",""),
                            "status": obj.get("status",""),
                        }
                except Exception:
                    pass

        # ---- Intento 2 y 3: HTML
        page = context.new_page()
        item_url = f"https://www.vinted.{domain}/items/{item_id}"
        page.goto(item_url, wait_until="domcontentloaded", timeout=30_000)

        title = ""
        price_val = ""
        currency = ""
        brand = ""
        size = ""
        status = ""

        # 2) JSON-LD si existe
        try:
            els = page.query_selector_all('script[type="application/ld+json"]')
            for el in els:
                data = el.text_content()
                if not data:
                    continue
                ld = json.loads(data)
                if isinstance(ld, dict):
                    if not title:
                        title = ld.get("name","") or title
                    offers = ld.get("offers") or {}
                    if isinstance(offers, dict):
                        price_val = price_val or offers.get("price","") or ""
                        currency  = currency  or offers.get("priceCurrency","") or ""
                    if isinstance(ld.get("brand"), dict) and not brand:
                        brand = ld["brand"].get("name","") or brand
                    # si ya tenemos precio/moneda, no seguimos
                    if price_val and currency:
                        break
        except Exception:
            pass

        # 3) Metatags
        if not title:
            title = _get_meta(page, "og:title") or page.title()
        if not price_val:
            price_val = _get_meta(page, "product:price:amount") or _get_meta(page, "og:price:amount") or ""
        if not currency:
            currency = _get_meta(page, "product:price:currency") or _get_meta(page, "og:price:currency") or ""

        # 4) DOM directo (clases/testid con 'price' o cualquier nodo con símbolo/ISO)
        if not price_val or not currency:
            p_dom, c_dom = _price_from_dom(page, domain)
            price_val = price_val or p_dom
            currency  = currency  or c_dom

        # 5) Atributos del DOM (Marca/Talla/Estado en varios idiomas)
        attr_map = _parse_attributes_map(page)
        if not brand:
            brand = _pick_attr(attr_map, ["marca","brand","marque","marke","merk","marca de"])
        if not size:
            size = _pick_attr(attr_map, ["talla","size","taille","größe","maat","taglia","rozmiar"])
        if not status:
            status = _pick_attr(attr_map, ["estado","condition","état","zustand","condición"])

        if not currency:
            currency = default_currency_for_domain(domain)

        browser.close()
        return {
            "id": item_id,
            "title": (title or "").strip(),
            "price": (price_val or "").strip(),
            "currency": (currency or "").strip(),
            "url": item_url,
            "brand": (brand or "").strip(),
            "size": (size or "").strip(),
            "status": (status or "").strip(),
        }

# ---------- Main ----------
def main():
    write_headers()

    profile_url = ENV_PROFILE
    if not profile_url:
        if ENV_USER_ID.isdigit():
            profile_url = f"https://www.vinted.{VINTED_DOMAIN}/member/{ENV_USER_ID}"
        else:
            raise SystemExit("Necesito VINTED_PROFILE_URL o VINTED_USER_ID numérico.")

    print("CONFIG:", "DOMAIN=", VINTED_DOMAIN, "PROFILE_URL=", profile_url, "SHEET_ID=", SHEET_ID)

    ids = collect_item_ids_with_browser(profile_url, VINTED_DOMAIN)
    print(f"[pw] total item ids found: {len(ids)}")
    if not ids:
        print("No hay IDs visibles (¿perfil con artículos ocultos/vacaciones?).")
        return

    items = []
    for i, iid in enumerate(ids, 1):
        items.append(fetch_item_detail_with_browser(iid, VINTED_DOMAIN))
        if i % 10 == 0:
            print(f"[detail] fetched {i}/{len(ids)}")
        time.sleep(0.05)

    print(f"Total artículos extraídos: {len(items)}")
    if items:
        write_rows(items)

if __name__ == "__main__":
    main()
