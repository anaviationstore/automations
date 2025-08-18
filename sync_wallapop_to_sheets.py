# sync_wallapop_to_sheets.py
import os
import json
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


# ============ Config ============
WALLAPOP_PROFILE_URL = os.getenv("WALLAPOP_PROFILE_URL", "").strip()  # URL pública del perfil/tienda
SHEET_ID = os.getenv("SHEET_ID", "").strip()
SHEET_TAB = os.getenv("SHEET_TAB", "wallapop_items").strip()
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON", "").strip()

if not (WALLAPOP_PROFILE_URL and SHEET_ID and GOOGLE_SA_JSON):
    raise SystemExit("Faltan variables: WALLAPOP_PROFILE_URL, SHEET_ID o GOOGLE_SA_JSON")

# Columnas de salida
OUTPUT_COLUMNS = [
    "id",
    "title",
    "price",
    "currency",
    "condition",
    "brand",
    "category",
    "location",
    "url",
    "image",
    "description",
    "seller_name",
    "seller_url",
    "timestamp_utc",
]


# ============ Google Sheets helpers ============
def get_sheet():
    creds = Credentials.from_service_account_info(
        json.loads(GOOGLE_SA_JSON),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)

    sh = gc.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet(SHEET_TAB)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_TAB, rows="2000", cols=str(len(OUTPUT_COLUMNS) + 5))

    # Encabezados coherentes
    existing = ws.row_values(1)
    if existing != OUTPUT_COLUMNS:
        ws.clear()
        ws.update(range_name="A1", values=[OUTPUT_COLUMNS])

    print(f"Spreadsheet URL: {sh.url}")
    print(f"Worksheet title: {ws.title}")
    return ws


def write_rows(ws, rows: List[Dict[str, Any]]):
    if not rows:
        return
    values = [[row.get(col, "") for col in OUTPUT_COLUMNS] for row in rows]

    # Calcula rango de escritura (append determinista)
    current_values = ws.get_all_values()
    start_row = len(current_values) + 1
    end_row = start_row + len(values) - 1
    end_col = len(OUTPUT_COLUMNS)

    def col_letter(n: int) -> str:
        s = ""
        while n:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s

    range_name = f"A{start_row}:{col_letter(end_col)}{end_row}"
    ws.update(range_name=range_name, values=values, value_input_option="RAW")
    print(f"Escritas {len(values)} filas en rango {range_name}.")


# ============ Scraping helpers ============
def normalize_price(raw: Optional[str]) -> (str, str):
    """Separa cantidad y divisa de textos tipo '25 €' o '€25'."""
    if not raw:
        return "", ""
    txt = raw.strip()
    currency = "€" if "€" in txt else ""
    digits = []
    for ch in txt:
        if ch.isdigit() or ch in [".", ","]:
            digits.append(ch)
    num = "".join(digits).replace(",", ".")
    return (num, currency)


def parse_json_ld(block_text: str) -> Dict[str, Any]:
    out = {}
    try:
        data = json.loads(block_text)
    except json.JSONDecodeError:
        return out

    nodes = data if isinstance(data, list) else [data]
    for node in nodes:
        t = node.get("@type") or node.get("@type".lower()) if isinstance(node, dict) else None
        if isinstance(t, list) and "Product" in t:
            product = node
        elif t == "Product":
            product = node
        else:
            continue

        out["title"] = product.get("name", "")
        out["description"] = product.get("description", "")

        images = product.get("image")
        if isinstance(images, list) and images:
            out["image"] = images[0]
        elif isinstance(images, str):
            out["image"] = images
        else:
            out["image"] = ""

        offers = product.get("offers") or {}
        if isinstance(offers, list):
            offers = offers[0] if offers else {}
        out["price"] = str(offers.get("price", "")) if offers else ""
        out["currency"] = offers.get("priceCurrency", "") if offers else ""

        out["url"] = product.get("url", "")
        out["id"] = product.get("sku", "") or product.get("productID", "") or ""

        brand = product.get("brand", "")
        if isinstance(brand, dict):
            out["brand"] = brand.get("name", "")
        else:
            out["brand"] = brand or ""

        out["condition"] = product.get("itemCondition", "")

        category = product.get("category", "")
        if isinstance(category, list):
            out["category"] = ", ".join(category)
        else:
            out["category"] = category or ""

        area = product.get("areaServed") or product.get("productionPlace") or ""
        if isinstance(area, dict):
            out["location"] = area.get("name", "")
        else:
            out["location"] = area or ""

        return out
    return out


def extract_with_selectors(page) -> Dict[str, Any]:
    data = {}

    def safe_text(selector: str, timeout=1200) -> str:
        try:
            el = page.wait_for_selector(selector, timeout=timeout)
            return (el.inner_text() or "").strip()
        except Exception:
            return ""

    def safe_attr(selector: str, attr: str, timeout=1200) -> str:
        try:
            el = page.wait_for_selector(selector, timeout=timeout)
            return (el.get_attribute(attr) or "").strip()
        except Exception:
            return ""

    data["title"] = safe_text("h1, h1[itemprop='name'], [data-e2e='product-title']")
    raw_price = safe_text("[data-e2e='product-price'], [itemprop='price'], .MoneyAmount__amount, .price")
    price, currency = normalize_price(raw_price)
    data["price"] = price
    data["currency"] = currency

    data["description"] = safe_text("[data-e2e='product-description'], [itemprop='description'], .description")
    data["image"] = safe_attr("img[itemprop='image'], .swiper img, .product-image img, .Image__img", "src")
    data["condition"] = safe_text("text=Estado") or safe_text("text=Condición")
    data["brand"] = safe_text("text=Marca")
    data["category"] = safe_text("a[href*='/categoria/'], [data-e2e='product-category']")
    data["location"] = safe_text("[data-e2e='product-location'], .location")

    return data


def fetch_item_detail(page, url: str, seller_name: str, seller_url: str) -> Dict[str, Any]:
    page.goto(url, wait_until="domcontentloaded", timeout=30000)

    # JSON-LD primero
    jsonld_blocks = []
    try:
        els = page.query_selector_all('script[type="application/ld+json"]')
        jsonld_blocks = [el.text_content() for el in els if el and el.text_content()]
    except Exception:
        pass

    parsed = {}
    for block in jsonld_blocks:
        parsed = parse_json_ld(block)
        if parsed:
            break

    # Fallback por selectores
    if not parsed or not parsed.get("title"):
        sel_parsed = extract_with_selectors(page)
        parsed = {**sel_parsed, **parsed} if parsed else sel_parsed

    # ID desde URL si no vino
    item_id = parsed.get("id") or ""
    if not item_id:
        try:
            path = url.split("?")[0].rstrip("/").split("/")
            if path:
                item_id = path[-1]
        except Exception:
            item_id = ""

    parsed["url"] = parsed.get("url") or url
    parsed["seller_name"] = seller_name
    parsed["seller_url"] = seller_url
    parsed["timestamp_utc"] = datetime.utcnow().isoformat()

    normalized = {k: parsed.get(k, "") for k in OUTPUT_COLUMNS}
    normalized["id"] = item_id
    return normalized


def collect_profile_item_urls(page, profile_url: str) -> (str, str, List[str]):
    page.goto(profile_url, wait_until="domcontentloaded", timeout=30000)

    try:
        seller_name = page.evaluate("""() => {
            const el = document.querySelector('h1, [data-e2e="user-name"], .user__name');
            return el ? el.textContent.trim() : "";
        }""")
    except Exception:
        seller_name = ""

    seller_url = profile_url

    seen = set()
    stable_rounds = 0
    last_count = 0

    for _ in range(60):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(0.7)

        try:
            urls = page.evaluate("""() => {
                const anchors = Array.from(document.querySelectorAll('a[href*="/item/"], a[href*="/product/"], a[href*="/producto/"]'));
                return anchors.map(a => new URL(a.href, location.origin).href);
            }""")
        except Exception:
            urls = []

        for u in urls:
            seen.add(u.split("?")[0])

        if len(seen) == last_count:
            stable_rounds += 1
        else:
            stable_rounds = 0
            last_count = len(seen)

        if stable_rounds >= 3:
            break

    return seller_name, seller_url, sorted(seen)


def run():
    ws = get_sheet()

    rows_to_write: List[Dict[str, Any]] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(locale="es-ES")
        page = context.new_page()

        seller_name, seller_url, item_urls = collect_profile_item_urls(page, WALLAPOP_PROFILE_URL)
        print(f"Encontrados {len(item_urls)} items en el perfil.")

        for idx, url in enumerate(item_urls, 1):
            try:
                item_row = fetch_item_detail(page, url, seller_name, seller_url)
                rows_to_write.append(item_row)

                if len(rows_to_write) >= 30:
                    write_rows(ws, rows_to_write)
                    rows_to_write = []
                    print(f"[{idx}/{len(item_urls)}] Guardados 30 ítems...")
            except Exception as e:
                print(f"Error al procesar {url}: {e}")

        if rows_to_write:
            write_rows(ws, rows_to_write)

        context.close()
        browser.close()

    print("Finalizado.")


if __name__ == "__main__":
    run()
