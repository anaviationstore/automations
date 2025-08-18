import os
import json
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


### ==============================
### Config por variables de entorno
### ==============================
WALLAPOP_PROFILE_URL = os.getenv("WALLAPOP_PROFILE_URL", "").strip()  # p.ej. https://es.wallapop.com/app/user/XXXX
SHEET_ID = os.getenv("SHEET_ID", "").strip()
SHEET_TAB = os.getenv("SHEET_TAB", "wallapop_items").strip()
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS", "").strip()  # contenido JSON de la service account

# Columnas de salida: añade/quita si quieres
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


### ==============================
### Google Sheets helpers
### ==============================
def get_sheet():
    if not GOOGLE_CREDENTIALS_JSON:
        raise RuntimeError("Falta GOOGLE_CREDENTIALS (JSON de service account).")

    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    gc = gspread.authorize(credentials)

    if not SHEET_ID:
        raise RuntimeError("Falta SHEET_ID.")
    sh = gc.open_by_key(SHEET_ID)

    try:
        ws = sh.worksheet(SHEET_TAB)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_TAB, rows="2000", cols=str(len(OUTPUT_COLUMNS) + 5))

    # Encabezados
    existing = ws.row_values(1)
    if existing != OUTPUT_COLUMNS:
        ws.resize(rows=2)  # limpia si cambia la estructura
        ws.update("A1", [OUTPUT_COLUMNS])

    return ws


def write_rows(ws, rows: List[Dict[str, Any]]):
    if not rows:
        return
    values = [[row.get(col, "") for col in OUTPUT_COLUMNS] for row in rows]
    ws.append_rows(values, value_input_option="RAW")


### ==============================
### Scraping Wallapop
### ==============================
def normalize_price(raw: Optional[str]) -> (str, str):
    """ Intenta separar cantidad y divisa; Wallapop suele mostrar '25 €' o '€25' """
    if not raw:
        return "", ""
    txt = raw.strip()
    # Básico: extrae números y divisa alrededor
    currency = "€" if "€" in txt else ""
    # Quita todo excepto dígitos y . ,  (luego reemplaza coma por punto)
    digits = []
    for ch in txt:
        if ch.isdigit() or ch in [".", ","]:
            digits.append(ch)
    num = "".join(digits).replace(",", ".")
    return (num, currency)


def parse_json_ld(block_text: str) -> Dict[str, Any]:
    """
    Devuelve dict con los campos normalizados si el JSON-LD es de tipo Product.
    Wallapop puede tener múltiples bloques; el script probará varios.
    """
    out = {}
    try:
        data = json.loads(block_text)
    except json.JSONDecodeError:
        return out

    # A veces es una lista de objetos JSON-LD
    nodes = data if isinstance(data, list) else [data]
    for node in nodes:
        t = node.get("@type") or node.get("@type".lower())
        if isinstance(t, list) and "Product" in t:
            product = node
        elif t == "Product":
            product = node
        else:
            continue

        out["title"] = product.get("name", "")
        out["description"] = product.get("description", "")
        out["image"] = ""
        images = product.get("image")
        if isinstance(images, list) and images:
            out["image"] = images[0]
        elif isinstance(images, str):
            out["image"] = images

        # offers -> price & currency
        offers = product.get("offers") or {}
        if isinstance(offers, list):
            offers = offers[0] if offers else {}
        out["price"] = str(offers.get("price", "")) if offers else ""
        out["currency"] = offers.get("priceCurrency", "") if offers else ""

        # id/url
        out["url"] = product.get("url", "")
        out["id"] = product.get("sku", "") or product.get("productID", "") or ""

        # brand/condition/category
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

        # location (si viene)
        area = product.get("areaServed") or product.get("productionPlace") or ""
        if isinstance(area, dict):
            out["location"] = area.get("name", "")
        else:
            out["location"] = area or ""

        return out  # usamos el primero que encaje

    return out


def extract_with_selectors(page) -> Dict[str, Any]:
    """
    Fallback cuando no hay JSON-LD fiable. Los selectores pueden cambiar con el tiempo;
    aquí usamos clases/atributos comunes y texto.
    """
    data = {}

    def safe_text(selector: str, timeout=1000) -> str:
        try:
            el = page.wait_for_selector(selector, timeout=timeout)
            return (el.inner_text() or "").strip()
        except PlaywrightTimeoutError:
            return ""
        except Exception:
            return ""

    def safe_attr(selector: str, attr: str, timeout=1000) -> str:
        try:
            el = page.wait_for_selector(selector, timeout=timeout)
            return (el.get_attribute(attr) or "").strip()
        except PlaywrightTimeoutError:
            return ""
        except Exception:
            return ""

    # Título
    data["title"] = safe_text("h1, h1[itemprop='name'], [data-e2e='product-title']")

    # Precio
    raw_price = safe_text("[data-e2e='product-price'], [itemprop='price'], .MoneyAmount__amount, .price")
    price, currency = normalize_price(raw_price)
    data["price"] = price
    data["currency"] = currency

    # Descripción
    data["description"] = safe_text("[data-e2e='product-description'], [itemprop='description'], .description")

    # Imagen principal
    img = safe_attr("img[itemprop='image'], .swiper img, .product-image img, .Image__img", "src")
    data["image"] = img

    # Condición / marca / categoría (best-effort)
    data["condition"] = safe_text("text=Estado") or safe_text("text=Condición")
    data["brand"] = safe_text("text=Marca")  # muchas fichas no lo tienen
    data["category"] = safe_text("a[href*='/categoria/'], [data-e2e='product-category']")

    # Localidad
    data["location"] = safe_text("[data-e2e='product-location'], .location")

    return data


def fetch_item_detail(page, url: str, seller_name: str, seller_url: str) -> Dict[str, Any]:
    page.goto(url, wait_until="domcontentloaded", timeout=30000)

    # Intenta JSON-LD primero (como hacíamos con Vinted)
    jsonld = page.evaluate("""() => {
        const nodes = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
        return nodes.map(n => n.textContent).filter(Boolean);
    }""")
    parsed = {}
    for block in jsonld:
        parsed = parse_json_ld(block)
        if parsed:
            break

    # Fallback por selectores
    if not parsed or not parsed.get("title"):
        sel_parsed = extract_with_selectors(page)
        # Combina: prioriza JSON-LD si existía, rellena huecos con selectores
        parsed = {**sel_parsed, **parsed} if parsed else sel_parsed

    # ID: si no vino del JSON-LD, intenta desde URL
    item_id = parsed.get("id") or ""
    if not item_id:
        # último segmento de la URL sin query
        try:
            path = url.split("?")[0].rstrip("/").split("/")
            if path:
                item_id = path[-1]
        except Exception:
            item_id = ""

    # URL
    parsed["url"] = parsed.get("url") or url

    # Seller
    parsed["seller_name"] = seller_name
    parsed["seller_url"] = seller_url

    # Timestamp
    parsed["timestamp_utc"] = datetime.utcnow().isoformat()

    # Asegura todas las columnas
    normalized = {k: parsed.get(k, "") for k in OUTPUT_COLUMNS}
    return normalized


def collect_profile_item_urls(page, profile_url: str) -> (str, List[str]):
    """
    Abre el perfil del vendedor y hace scroll para cargar todos los productos.
    Devuelve (seller_name, item_urls).
    """
    page.goto(profile_url, wait_until="domcontentloaded", timeout=30000)

    # Nombre del vendedor (best-effort)
    try:
        seller_name = page.evaluate("""() => {
            const el = document.querySelector('h1, [data-e2e="user-name"], .user__name');
            return el ? el.textContent.trim() : "";
        }""")
    except Exception:
        seller_name = ""

    # Normaliza seller_url
    seller_url = profile_url

    # Scroll infinito
    seen = set()
    stable_rounds = 0
    last_count = 0

    for _ in range(60):  # máx ~60 tandas de scroll
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(0.7)

        urls = page.evaluate("""() => {
            const anchors = Array.from(document.querySelectorAll('a[href*="/item/"], a[href*="/product/"], a[href*="/producto/"]'));
            return anchors.map(a => new URL(a.href, location.origin).href);
        }""")

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
    if not WALLAPOP_PROFILE_URL:
        raise RuntimeError("Falta WALLAPOP_PROFILE_URL (URL del perfil del vendedor en Wallapop).")

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

                # Escribe por lotes cada 30
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
