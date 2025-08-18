# sync_vinted_to_sheets.py
import os, re, json, time
import requests
import gspread
from google.oauth2.service_account import Credentials

# ---------- Config ----------
VINTED_DOMAIN = os.getenv("VINTED_DOMAIN", "es")         # es, fr, de, it...
VINTED_USER_ID = os.getenv("VINTED_USER_ID", "").strip() # número opcional
VINTED_PROFILE_URL = os.getenv("VINTED_PROFILE_URL", "").strip()
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

HEADERS = ["id", "title", "price", "currency", "url", "brand", "size", "status"]

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

# ---------- Utilidades ----------
def detect_domain_from_url(url: str, fallback: str) -> str:
    m = re.search(r"https?://www\.vinted\.([a-z.]+)/", url)
    return m.group(1) if m else fallback

def detect_user_id_from_profile(sess: requests.Session, url: str) -> int | None:
    """
    Intenta extraer el user_id de la URL o del HTML de la página de perfil.
    Soporta:
      - /member/123456-algo
      - /member/algo-123456
      - HTML con ... "user_id":123456 ... o ..."\"user\":{\"id\":123456}"...
    """
    # 1) Intento desde la propia URL
    for pat in (r"/member/(\d+)-", r"-([0-9]+)(?:$|[/?])"):
        m = re.search(pat, url)
        if m:
            return int(m.group(1))

    # 2) Descargar HTML y rascar
    r = sess.get(url, timeout=20)
    if r.status_code != 200:
        return None
    html = r.text

    pats = [
        r'"user_id"\s*:\s*(\d+)',
        r'"user"\s*:\s*\{\s*"id"\s*:\s*(\d+)',
        r'"id"\s*:\s*(\d+)\s*,\s*"login"\s*:',
        r'/member/(\d+)-',   # por si está incrustado en algún enlace
    ]
    for p in pats:
        m = re.search(p, html)
        if m:
            return int(m.group(1))
    return None

def fetch_items_requests(user_id:int, domain:str) -> list[dict]:
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": f"https://www.vinted.{domain}/",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "X-Requested-With": "XMLHttpRequest",
    })

    # Home para cookies
    home = f"https://www.vinted.{domain}/"
    r_home = sess.get(home, timeout=20)
    print("[requests] home status:", r_home.status_code)

    # CSRF si existe
    csrf = next((v for k, v in sess.cookies.get_dict().items() if "csrf" in k.lower()), None)
    if csrf:
        sess.headers["x-csrf-token"] = csrf
        print("[requests] csrf token present")

    results = []
    per_page = 96

    # 1º: endpoint de usuario (si existe para este dominio)
    url_user = f"https://www.vinted.{domain}/api/v2/users/{user_id}/items"
    page = 1
    while True:
        params = {"order": "newest_first", "status": "active", "page": page, "per_page": per_page}
        r = sess.get(url_user, params=params, timeout=25)
        print(f"[requests user_items] page={page} status={r.status_code} len={len(r.content)}")
        if r.status_code == 404:
            print("[requests user_items] not found, switching to catalog/items")
            break
        if r.status_code != 200:
            time.sleep(0.8)
            r = sess.get(url_user, params=params, timeout=25)
            print(f"[requests user_items] retry page={page} status={r.status_code}")
            if r.status_code != 200:
                break
        data = r.json()
        items = data.get("items", [])
        print(f"[requests user_items] items this page: {len(items)}")
        if not items:
            break
        for x in items:
            price = x.get("price", {})
            price_val = price.get("amount", "") if isinstance(price, dict) else price
            currency = price.get("currency_code", "") if isinstance(price, dict) else (x.get("currency") or x.get("currency_code", ""))
            url_item = x.get("url") or (f"https://www.vinted.{domain}{x.get('path')}" if x.get("path") else "")
            results.append({
                "id": x.get("id",""),
                "title": x.get("title",""),
                "price": price_val,
                "currency": currency,
                "url": url_item,
                "brand": x.get("brand_title",""),
                "size": x.get("size_title",""),
                "status": x.get("status",""),
            })
        page += 1
        time.sleep(0.25)

    if results:
        return results

    # 2º: catalog/items + filtro estricto por user_id
    url_cat = f"https://www.vinted.{domain}/api/v2/catalog/items"
    page = 1
    while True:
        params = {
            "order": "newest_first", "status": "active", "user_id": user_id,
            "search_text": "", "page": page, "per_page": per_page,
        }
        r = sess.get(url_cat, params=params, timeout=25)
        print(f"[requests catalog] page={page} status={r.status_code} len={len(r.content)}")
        if r.status_code != 200:
            time.sleep(0.8)
            r = sess.get(url_cat, params=params, timeout=25)
            print(f"[requests catalog] retry page={page} status={r.status_code}")
            if r.status_code != 200:
                break
        data = r.json()
        items = data.get("items", [])
        print(f"[requests catalog] items this page (raw): {len(items)}")
        if not items:
            break
        for x in items:
            uid = x.get("user_id") or (x.get("user") or {}).get("id")
            if uid != user_id:
                continue  # filtro duro por tu usuario
            price = x.get("price", {})
            price_val = price.get("amount", "") if isinstance(price, dict) else price
            currency = price.get("currency_code", "") if isinstance(price, dict) else (x.get("currency") or x.get("currency_code", ""))
            url_item = x.get("url") or (f"https://www.vinted.{domain}{x.get('path')}" if x.get("path") else "")
            results.append({
                "id": x.get("id",""),
                "title": x.get("title",""),
                "price": price_val,
                "currency": currency,
                "url": url_item,
                "brand": x.get("brand_title",""),
                "size": x.get("size_title",""),
                "status": x.get("status",""),
            })
        page += 1
        time.sleep(0.25)

    return results

def main():
    write_headers()

    # Detectar user_id si no es un número válido
    user_id = None
    if VINTED_USER_ID.isdigit():
        user_id = int(VINTED_USER_ID)

    # Si no tenemos número, intentar deducirlo desde la URL de perfil
    domain = VINTED_DOMAIN
    if user_id is None:
        if not VINTED_PROFILE_URL:
            raise SystemExit("No tengo VINTED_USER_ID válido ni VINTED_PROFILE_URL para detectar el ID.")
        # si la URL trae dominio distinto, úsalo
        domain = detect_domain_from_url(VINTED_PROFILE_URL, VINTED_DOMAIN)
        sess = requests.Session()
        detected = detect_user_id_from_profile(sess, VINTED_PROFILE_URL)
        if detected:
            user_id = detected
        else:
            raise SystemExit("No pude detectar tu user_id desde VINTED_PROFILE_URL. Revisa la URL de perfil.")

    print("CONFIG:", "DOMAIN=", domain, "USER_ID=", user_id, "SHEET_ID=", SHEET_ID)

    items = fetch_items_requests(user_id, domain)
    print(f"Total artículos (filtrados por tu user_id): {len(items)}")
    if items:
        write_rows(items)

if __name__ == "__main__":
    main()
