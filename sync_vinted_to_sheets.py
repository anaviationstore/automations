# sync_vinted_to_sheets.py
import os, json, time
from datetime import datetime
import urllib.parse, urllib.request, ssl

import gspread
from google.oauth2.service_account import Credentials

# -------- Config desde secrets/entorno --------
VINTED_DOMAIN = os.getenv("VINTED_DOMAIN", "es")      # ej: es, fr, de, it, nl...
VINTED_USER_ID = os.getenv("VINTED_USER_ID")          # número del perfil: https://www.vinted.es/member/123456-usuario
SHEET_ID       = os.getenv("SHEET_ID")                # ID del Google Sheet
SHEET_TAB      = os.getenv("SHEET_TAB", "vinted_items")
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")          # contenido JSON (string entero)

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
    # Limpia desde la fila 2 y deja las cabeceras
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
    ws.add_rows(max(0, len(rows) - (ws.row_count - 1)))
    ws.update(f"A2:H{len(rows)+1}", rows)

# -------- Fetchers --------
# Desactiva verificación SSL en algunos runners (evita errores de cert en urllib)
ssl._create_default_https_context = ssl._create_unverified_context

def http_json(url):
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/plain, */*",
            "Referer": f"https://www.vinted.{VINTED_DOMAIN}/",
        },
    )
    with urllib.request.urlopen(req, timeout=25) as r:
        txt = r.read().decode("utf-8")
        return json.loads(txt)

def fetch_with_wrapper(user_id: int, domain: str):
    """Intenta usar la librería no-oficial si está disponible."""
    try:
        from vinted import Vinted
        v = Vinted(domain=domain)
        results = []
        page, per_page = 1, 100
        while True:
            # Muchos wrappers aceptan estos argumentos
            resp = v.search(user_id=user_id, page=page, per_page=per_page, order="newest_first")
            items = getattr(resp, "items", []) or []
            print(f"[wrapper] page={page} items={len(items)}")
            if not items:
                break
            for x in items:
                results.append({
                    "id": getattr(x, "id", ""),
                    "title": getattr(x, "title", ""),
                    "price": getattr(x, "price", ""),
                    "currency": getattr(x, "currency", ""),
                    "url": getattr(x, "url", ""),
                    "brand": getattr(x, "brand_title", ""),
                    "size": getattr(x, "size_title", ""),
                    "status": getattr(x, "status", ""),
                })
            page += 1
            time.sleep(0.5)
        return results
    except Exception as e:
        print("[wrapper] error:", repr(e))
        return None

def fetch_with_fallback(user_id: int, domain: str):
    """Usa endpoints públicos no documentados como respaldo."""
    results = []

    # Endpoint A: /api/v2/users/<id>/items
    try:
        page, per_page = 1, 100
        while True:
            url = f"https://www.vinted.{domain}/api/v2/users/{user_id}/items?status=active&order=newest_first&page={page}&per_page={per_page}"
            data = http_json(url)
            items = data.get("items", [])
            print(f"[fallback A] page={page} items={len(items)}")
            if not items:
                break
            for x in items:
                price = x.get("price", {})
                results.append({
                    "id": x.get("id"),
                    "title": x.get("title"),
                    "price": price.get("amount", ""),
                    "currency": price.get("currency_code", ""),
                    "url": x.get("url"),
                    "brand": x.get("brand_title", ""),
                    "size": x.get("size_title", ""),
                    "status": x.get("status", ""),
                })
            page += 1
            time.sleep(0.4)
        if results:
            return results
    except Exception as e:
        print("[fallback A] error:", repr(e))

    # Endpoint B: /api/v2/catalog/items?user_id=...
    try:
        page, per_page = 1, 100
        while True:
            url = f"https://www.vinted.{domain}/api/v2/catalog/items?user_id={user_id}&order=newest_first&page={page}&per_page={per_page}"
            data = http_json(url)
            items = data.get("items", [])
            print(f"[fallback B] page={page} items={len(items)}")
            if not items:
                break
            for x in items:
                price = x.get("price", {})
                results.append({
                    "id": x.get("id"),
                    "title": x.get("title"),
                    "price": (price.get("amount") if isinstance(price, dict) else price),
                    "currency": (price.get("currency_code") if isinstance(price, dict) else ""),
                    "url": x.get("url"),
                    "brand": x.get("brand_title", ""),
                    "size": x.get("size_title", ""),
                    "status": x.get("status", ""),
                })
            page += 1
            time.sleep(0.4)
    except Exception as e:
        print("[fallback B] error:", repr(e))

    return results

# -------- Main --------
def main():
    write_headers()

    # 1º intento: wrapper
    items = fetch_with_wrapper(int(VINTED_USER_ID), VINTED_DOMAIN)

    # Respaldo: endpoints
    if not items:
        items = fetch_with_fallback(int(VINTED_USER_ID), VINTED_DOMAIN)

    total = len(items) if items else 0
    print(f"Total artículos: {total}")

    if total:
        write_rows(items)

if __name__ == "__main__":
    main()
