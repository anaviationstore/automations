# sync_vinted_to_sheets.py
import os, json, time
import urllib.request, ssl
import gspread
from google.oauth2.service_account import Credentials

# -------- Config desde secrets/entorno --------
VINTED_DOMAIN = os.getenv("VINTED_DOMAIN", "es")      # ej: es, fr, de, it...
VINTED_USER_ID = os.getenv("VINTED_USER_ID")          # número en tu URL: https://www.vinted.es/member/123456-usuario
SHEET_ID       = os.getenv("SHEET_ID")                # ID del Google Sheet
SHEET_TAB      = os.getenv("SHEET_TAB", "vinted_items")
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")          # contenido JSON (string)

if not (VINTED_USER_ID and SHEET_ID and GOOGLE_SA_JSON):
    raise SystemExit("Faltan variables: VINTED_USER_ID, SHEET_ID o GOOGLE_SA_JSON")

print("CONFIG:",
      "DOMAIN=", VINTED_DOMAIN,
      "USER_ID=", VINTED_USER_ID,
      "SHEET_ID=", SHEET_ID)

# -------- Google Sheets auth --------
creds_info = json.loads(GOOGLE_SA_JSON)
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
try:
    ws = sh.worksheet(SHEET_TAB)
except gspread.exceptions.WorksheetNotFound:
    ws = sh.add_worksheet(title=SHEET_TAB, rows=2, cols=8)

# -------- Helpers de hoja --------
HEADERS = ["id","title","price","currency","url","brand","size","status"]

def write_headers():
    ws.clear()
    ws.update("A1:H1", [HEADERS])

def write_rows(items):
    if not items:
        return
    rows = []
    for it in items:
        rows.append([
            it.get("id",""),
            it.get("title",""),
            it.get("price",""),
            it.get("currency",""),
            it.get("url",""),
            it.get("brand",""),
            it.get("size",""),
            it.get("status",""),
        ])
    # Asegura filas suficientes
    extra = max(0, len(rows) - (ws.row_count - 1))
    if extra:
        ws.add_rows(extra)
    ws.update(f"A2:H{len(rows)+1}", rows)

# -------- Fetch por wrapper (con search_url) --------
def fetch_items_via_wrapper(user_id:int, domain:str):
    """
    Usa el cliente oficial del wrapper 'vinted' con una URL de búsqueda
    que filtra por user_id. El wrapper se encarga de cookies/tokens.
    """
    try:
        from vinted import Vinted
        v = Vinted(domain=domain)
        search_url = f"https://www.vinted.{domain}/catalog?user_id={user_id}&status=active&order=newest_first"
        results = []
        page, per_page = 1, 100
        while True:
            # IMPORTANTE: este wrapper acepta search_url, page y per_page
            resp = v.search(search_url=search_url, page=page, per_page=per_page, order="newest_first")
            items = getattr(resp, "items", resp) or []
            print(f"[wrapper] page={page} items={len(items)}")
            if not items:
                break
            for x in items:
                # El wrapper devuelve objetos; usamos getattr para no romper si falta algo
                results.append({
                    "id": getattr(x, "id", ""),
                    "title": getattr(x, "title", ""),
                    # Algunos wrappers devuelven price como número/str y currency aparte
                    "price": getattr(x, "price", "") or getattr(x, "price_numeric", ""),
                    "currency": getattr(x, "currency", "") or getattr(x, "currency_code", ""),
                    "url": getattr(x, "url", ""),
                    "brand": getattr(x, "brand_title", ""),
                    "size": getattr(x, "size_title", ""),
                    "status": getattr(x, "status", ""),
                })
            page += 1
            time.sleep(0.4)
        return results
    except Exception as e:
        print("[wrapper] error:", repr(e))
        return []

# -------- Main --------
def main():
    write_headers()
    items = fetch_items_via_wrapper(int(VINTED_USER_ID), VINTED_DOMAIN)
    print(f"Total artículos: {len(items)}")
    if items:
        write_rows(items)

if __name__ == "__main__":
    main()
