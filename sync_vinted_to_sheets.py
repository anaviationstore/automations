# sync_vinted_to_sheets.py
import os, json, time
import gspread
from google.oauth2.service_account import Credentials

# ---------- Config ----------
VINTED_DOMAIN = os.getenv("VINTED_DOMAIN", "es")      # es, fr, de, it...
VINTED_USER_ID = os.getenv("VINTED_USER_ID")          # número en tu URL: https://www.vinted.es/member/123456-usuario
SHEET_ID       = os.getenv("SHEET_ID")
SHEET_TAB      = os.getenv("SHEET_TAB", "vinted_items")
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")

if not (VINTED_USER_ID and SHEET_ID and GOOGLE_SA_JSON):
    raise SystemExit("Faltan variables: VINTED_USER_ID, SHEET_ID o GOOGLE_SA_JSON")

print("CONFIG:", "DOMAIN=", VINTED_DOMAIN, "USER_ID=", VINTED_USER_ID, "SHEET_ID=", SHEET_ID)

# ---------- Google Sheets ----------
creds = Credentials.from_service_account_info(json.loads(GOOGLE_SA_JSON),
                                              scopes=["https://www.googleapis.com/auth/spreadsheets"])
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
try:
    ws = sh.worksheet(SHEET_TAB)
except gspread.exceptions.WorksheetNotFound:
    ws = sh.add_worksheet(title=SHEET_TAB, rows=2, cols=8)

HEADERS = ["id", "title", "price", "currency", "url", "brand", "size", "status"]

def write_headers():
    ws.clear()
    ws.update(range_name="A1:H1", values=[HEADERS])  # evita el warning deprecado

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
    # añadir filas si faltan
    needed = len(rows) - (ws.row_count - 1)
    if needed > 0:
        ws.add_rows(needed)
    ws.update(range_name=f"A2:H{len(rows)+1}", values=rows)

# ---------- Fetch con wrapper (URL como argumento posicional) ----------
def fetch_items(user_id:int, domain:str):
    """
    Usa el wrapper 'vinted' pasando una URL de búsqueda como PRIMER argumento.
    Paginamos añadiendo &page=&per_page= a la URL.
    """
    from vinted import Vinted
    v = Vinted(domain=domain)

    base_url = f"https://www.vinted.{domain}/catalog?user_id={user_id}&status=active&order=newest_first"
    page, per_page = 1, 100
    results = []

    while True:
        url = f"{base_url}&page={page}&per_page={per_page}"
        # OJO: el wrapper espera el primer parámetro posicional (sin nombre)
        resp = v.search(url)
        # resp puede ser lista de objetos, o un objeto con .items
        items = getattr(resp, "items", None)
        if items is None:
            items = resp if isinstance(resp, list) else (resp.get("items", []) if isinstance(resp, dict) else [])
        print(f"[wrapper-url] page={page} items={len(items)}")
        if not items:
            break

        for x in items:
            if isinstance(x, dict):
                price = x.get("price", {})
                price_val = price.get("amount", "") if isinstance(price, dict) else price
                currency = price.get("currency_code", "") if isinstance(price, dict) else (x.get("currency") or "")
                url_item = x.get("url") or (f"https://www.vinted.{domain}{x.get('path')}" if x.get("path") else "")
                results.append({
                    "id": x.get("id", ""),
                    "title": x.get("title", ""),
                    "price": price_val,
                    "currency": currency,
                    "url": url_item,
                    "brand": x.get("brand_title", ""),
                    "size": x.get("size_title", ""),
                    "status": x.get("status", ""),
                })
            else:
                # objeto del wrapper
                results.append({
                    "id": getattr(x, "id", ""),
                    "title": getattr(x, "title", ""),
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

# ---------- Main ----------
def main():
    write_headers()
    items = fetch_items(int(VINTED_USER_ID), VINTED_DOMAIN)
    print(f"Total artículos: {len(items)}")
    if items:
        write_rows(items)

if __name__ == "__main__":
    main()
