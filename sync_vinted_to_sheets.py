# sync_vinted_to_sheets.py
import os, re, json, time
import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright

# ---------- Config ----------
VINTED_DOMAIN = os.getenv("VINTED_DOMAIN", "es").strip()
VINTED_USER_ID = os.getenv("VINTED_USER_ID", "").strip()              # opcional
VINTED_PROFILE_URL = os.getenv("VINTED_PROFILE_URL", "").strip()      # ej: https://www.vinted.es/member/279020986
SHEET_ID       = os.getenv("SHEET_ID")
SHEET_TAB      = os.getenv("SHEET_TAB", "vinted_items")
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")

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

# ---------- Scraping con Playwright ----------
ITEM_HREF_RE = re.compile(r"/items/(\d+)(?:-|$)")

def collect_item_ids_with_browser(profile_url: str, domain: str) -> list[str]:
    """
    Abre el perfil, hace scroll para cargar todos los anuncios y extrae los IDs
    de los enlaces a /items/<id>-...
    """
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

        # Siempre orden más nuevo primero
        url = profile_url
        if "?order=" not in url:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}order=newest_first"

        print("[pw] goto", url)
        page.goto(url, wait_until="networkidle", timeout=60_000)

        seen_ids = set()
        stable_rounds = 0

        for i in range(60):  # ~60 scrolls máx (ajustable)
            # capturar todos los href visibles
            hrefs = page.eval_on_selector_all('a[href^="/items/"]', "els => els.map(e => e.getAttribute('href'))")
            added = 0
            for h in hrefs:
                if not h:
                    continue
                m = ITEM_HREF_RE.search(h)
                if m:
                    iid = m.group(1)
                    if iid not in seen_ids:
                        seen_ids.add(iid)
                        added += 1

            print(f"[pw] scroll {i+1}: total_ids={len(seen_ids)} (+{added})")

            # si no aparecen enlaces nuevos en 3 rondas, paramos
            if added == 0:
                stable_rounds += 1
            else:
                stable_rounds = 0
            if stable_rounds >= 3:
                break

            # scroll suave al fondo y espera
            page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            page.wait_for_timeout(900)

        context.storage_state(path="playwright_state.json")  # por si hiciera falta depurar
        browser.close()
        return list(seen_ids)

def fetch_item_detail_with_browser(item_id: str, domain: str) -> dict:
    """
    Pide el detalle vía API con las cookies del navegador.
    Si falla, recupera lo básico desde el HTML del propio item.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(storage_state="playwright_state.json")
        # Request context hereda cookies de la sesión anterior
        req = context.request

        # Intento 1: API JSON (varias variantes)
        candidates = [
            f"https://www.vinted.{domain}/api/v2/items/{item_id}",
            f"https://www.vinted.{domain}/web/api/v2/items/{item_id}",
        ]
        for url in candidates:
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
                            "currency": currency,
                            "url": url_item,
                            "brand": obj.get("brand_title",""),
                            "size": obj.get("size_title",""),
                            "status": obj.get("status",""),
                        }
                except Exception:
                    pass

        # Intento 2: HTML del item (JSON-LD)
        page = context.new_page()
        item_url = f"https://www.vinted.{domain}/items/{item_id}"
        page.goto(item_url, wait_until="domcontentloaded", timeout=30_000)

        # extrae JSON-LD si existe
        data = page.eval_on_selector(
            'script[type="application/ld+json"]',
            "el => el ? el.textContent : null"
        )
        title = ""
        price_val = ""
        currency = ""
        brand = ""
        size = ""
        if data:
            try:
                ld = json.loads(data)
                if isinstance(ld, dict):
                    title = ld.get("name","") or title
                    offers = ld.get("offers") or {}
                    if isinstance(offers, dict):
                        price_val = offers.get("price","") or price_val
                        currency = offers.get("priceCurrency","") or currency
                    brand = (ld.get("brand") or {}).get("name","") if isinstance(ld.get("brand"), dict) else brand
            except Exception:
                pass

        browser.close()
        return {
            "id": item_id,
            "title": title,
            "price": price_val,
            "currency": currency,
            "url": item_url,
            "brand": brand,
            "size": size,
            "status": "",
        }

def main():
    write_headers()

    # Determinar URL de perfil e ID
    if not VINTED_PROFILE_URL:
        if VINTED_USER_ID.isdigit():
            VINTED_PROFILE_URL = f"https://www.vinted.{VINTED_DOMAIN}/member/{VINTED_USER_ID}"
        else:
            raise SystemExit("Necesito VINTED_PROFILE_URL (o VINTED_USER_ID numérico).")

    print("CONFIG:", "DOMAIN=", VINTED_DOMAIN, "PROFILE_URL=", VINTED_PROFILE_URL, "SHEET_ID=", SHEET_ID)

    # 1) Recoger IDs de tus anuncios navegando el perfil
    ids = collect_item_ids_with_browser(VINTED_PROFILE_URL, VINTED_DOMAIN)
    print(f"[pw] total item ids found: {len(ids)}")
    if not ids:
        print("No hay IDs visibles en el perfil (¿sin artículos públicos, ocultos o vacaciones activas?).")
        return

    # 2) Obtener detalles de cada item con las cookies del navegador
    items = []
    for i, iid in enumerate(ids, 1):
        items.append(fetch_item_detail_with_browser(iid, VINTED_DOMAIN))
        if i % 10 == 0:
            print(f"[detail] fetched {i}/{len(ids)}")
        time.sleep(0.1)

    print(f"Total artículos extraídos: {len(items)}")
    if items:
        write_rows(items)

if __name__ == "__main__":
    main()
