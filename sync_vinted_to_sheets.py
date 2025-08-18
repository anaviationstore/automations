import os, json, time
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

# ---- Config de entorno ----
VINTED_DOMAIN = os.getenv("VINTED_DOMAIN", "es")      # es, fr, de, ...
VINTED_USER_ID = os.getenv("VINTED_USER_ID")          # ej: 123456
SHEET_ID       = os.getenv("SHEET_ID")                # ID del Spreadsheet
SHEET_TAB      = os.getenv("SHEET_TAB", "vinted_items")
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")          # contenido JSON (string)

if not (VINTED_USER_ID and SHEET_ID and GOOGLE_SA_JSON):
    raise SystemExit("Faltan variables: VINTED_USER_ID, SHEET_ID o GOOGLE_SA_JSON")

# ---- Autenticación Google Sheets ----
creds_info = json.loads(GOOGLE_SA_JSON)
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
ws = sh.worksheet(SHEET_TAB)

# ---- Helpers ----
def write_headers():
    headers = ["id","title","price","currency","url","brand","size","status"]
    ws.resize(1)  # limpia datos desde la fila 2 sin tocar formato
    ws.update("A1:H1", [headers])

def upsert_rows(items):
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
    if rows:
        ws.add_rows(len(rows))
        ws.update(f"A2:H{len(rows)+1}", rows)

# ---- 1ª ESTRATEGIA: librería no-oficial (PyPI: vinted-api-wrapper) ----
# Docs/capacidades: búsqueda por filtros o por URL de búsqueda de Vinted. :contentReference[oaicite:1]{index=1}
def fetch_with_wrapper(user_id:int, domain:str):
    # La mayoría de wrappers aceptan URL de búsqueda de Vinted. Construimos una que filtra por usuario.
    # Esta URL "catálogo" funciona como búsqueda y suele aceptar filtros (incl. user_id) en países soportados.
    search_url = f"https://www.vinted.{domain}/catalog?user_id={user_id}&order=newest_first"
    try:
        from vinted import Vinted
        v = Vinted(domain=domain)
        # Recuperamos varias páginas si hace falta:
        per_page = 100
        page = 1
        results = []
        while True:
            resp = v.search(search_url=search_url, page=page, per_page=per_page)
            items = getattr(resp, "items", []) or []
            if not items:
                break
            for x in items:
                results.append({
                    "id": x.id,
                    "title": x.title,
                    "price": x.price,
                    "currency": getattr(x, "currency", ""),
                    "url": x.url,
                    "brand": getattr(x, "brand_title", ""),
                    "size": getattr(x, "size_title", ""),
                    "status": getattr(x, "status", ""),
                })
            page += 1
            # cortesía para no abusar
            time.sleep(0.6)
        return results
    except Exception as e:
        print("Wrapper falló:", repr(e))
        return None

# ---- 2ª ESTRATEGIA (respaldo): endpoint móvil conocido (puede requerir tokens) ----
# ATENCIÓN: puede devolver 401/403 si cambian tokens/antibot. Lo intentamos suave y seguimos.
# Referencia de que Vinted usa endpoints internos y tokens en móvil/web. :contentReference[oaicite:2]{index=2}
import urllib.parse, urllib.request
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

def fetch_with_fallback(user_id:int, domain:str):
    # Probamos un endpoint frecuente por usuario (no documentado/oficial):
    # /api/v2/users/<id>/items?per_page=...&page=...
    base = f"https://www.vinted.{domain}/api/v2/users/{user_id}/items"
    per_page = 100
    page = 1
    results = []
    while True:
        q = urllib.parse.urlencode({"per_page": per_page, "page": page})
        url = f"{base}?{q}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                data = json.loads(r.read().decode("utf-8"))
            items = data.get("items", [])
            if not items:
                break
            for x in items:
                results.append({
                    "id": x.get("id"),
                    "title": x.get("title"),
                    "price": x.get("price", {}).get("amount", ""),
                    "currency": x.get("price", {}).get("currency_code", ""),
                    "url": x.get("url"),
                    "brand": x.get("brand_title", ""),
                    "size": x.get("size_title", ""),
                    "status": x.get("status", ""),
                })
            page += 1
            time.sleep(0.6)
        except Exception as e:
            print("Fallback falló en página", page, "->", repr(e))
            break
    return results

def main():
    write_headers()
    items = fetch_with_wrapper(int(VINTED_USER_ID), VINTED_DOMAIN)
    if not items:
        items = fetch_with_fallback(int(VINTED_USER_ID), VINTED_DOMAIN)
    print(f"Total artículos: {len(items)}")
    upsert_rows(items)

if __name__ == "__main__":
    main()
