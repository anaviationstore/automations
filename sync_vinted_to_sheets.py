# sync_vinted_to_sheets.py
import os, re, json, time
import requests
import gspread
from google.oauth2.service_account import Credentials

# ---------- Config ----------
VINTED_DOMAIN = os.getenv("VINTED_DOMAIN", "es").strip()       # es, fr, de...
VINTED_USER_ID = os.getenv("VINTED_USER_ID", "").strip()       # opcional, numérico
VINTED_PROFILE_URL = os.getenv("VINTED_PROFILE_URL", "").strip()  # ej: https://www.vinted.es/member/279020986
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

# ---------- HTTP helpers ----------
def make_session(domain:str):
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
        "Referer": f"https://www.vinted.{domain}/",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "X-Requested-With": "XMLHttpRequest",
    })
    # visita home para cookies anti-bot
    home = f"https://www.vinted.{domain}/"
    r = s.get(home, timeout=20)
    print("[session] home", r.status_code)
    # añade CSRF si está
    csrf = next((v for k, v in s.cookies.get_dict().items() if "csrf" in k.lower()), None)
    if csrf:
        s.headers["x-csrf-token"] = csrf
        print("[session] csrf present")
    return s

# ---------- 1) Extraer IDs de tu perfil ----------
def collect_item_ids_from_profile(sess:requests.Session, user_id:int, domain:str):
    ids = set()
    last_new = 0
    # probamos paginación ?page=1..40; si no hay paginado, no pasa nada (deduplicamos)
    for page in range(1, 41):
        url = f"https://www.vinted.{domain}/member/{user_id}?order=newest_first&page={page}"
        r = sess.get(url, timeout=25)
        print(f"[profile] GET {url} -> {r.status_code}, len={len(r.text)}")
        if r.status_code != 200:
            break
        html = r.text
        # busca enlaces /items/123456789-...
        found = set(re.findall(r'/items/(\d+)-', html))
        print(f"[profile] page {page}: found {len(found)} ids")
        before = len(ids)
        ids |= found
        last_new = len(ids) - before
        # si no aparecen IDs nuevos, paramos
        if last_new == 0:
            break
        time.sleep(0.25)
    print(f"[profile] total unique ids: {len(ids)}")
    return list(ids)

# ---------- 2) Cargar detalles de cada ID ----------
def fetch_item_detail(sess:requests.Session, item_id:str, domain:str):
    # intentamos varios endpoints de detalle
    candidates = [
        f"https://www.vinted.{domain}/api/v2/items/{item_id}",
        f"https://www.vinted.{domain}/api/v2/items/{item_id}?include=user",
    ]
    for url in candidates:
        r = sess.get(url, timeout=20)
        if r.status_code != 200:
            continue
        try:
            data = r.json()
        except Exception:
            continue
        obj = data.get("item") or data.get("data") or data  # intenta varios contenedores
        if not isinstance(obj, dict):
            continue
        # normaliza campos
        price_field = obj.get("price", "")
        if isinstance(price_field, dict):
            price_val = price_field.get("amount", "")
            currency = price_field.get("currency_code", "")
        else:
            price_val = price_field
            currency = obj.get("currency") or obj.get("currency_code", "")
        url_item = obj.get("url") or f"https://www.vinted.{domain}/items/{item_id}"
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
    return {
        # si el detalle falla, al menos devolvemos el id y la URL básica
        "id": item_id, "title": "", "price": "", "currency": "",
        "url": f"https://www.vinted.{domain}/items/{item_id}",
        "brand": "", "size": "", "status": "",
    }

def fetch_all_details(sess:requests.Session, item_ids:list[str], domain:str):
    out = []
    for i, iid in enumerate(item_ids, 1):
        out.append(fetch_item_detail(sess, iid, domain))
        if i % 20 == 0:
            print(f"[detail] fetched {i}/{len(item_ids)}")
        time.sleep(0.15)
    return out

# ---------- Main ----------
def main():
    # determinar user_id
    domain = VINTED_DOMAIN
    user_id = None
    if VINTED_USER_ID.isdigit():
        user_id = int(VINTED_USER_ID)
    if user_id is None:
        if not VINTED_PROFILE_URL:
            raise SystemExit("Pon VINTED_USER_ID (número) o VINTED_PROFILE_URL.")
        # extrae número directamente de la URL si viene así
        m = re.search(r"/member/(\d+)", VINTED_PROFILE_URL)
        if m:
            user_id = int(m.group(1))
        else:
            raise SystemExit("La VINTED_PROFILE_URL no contiene el número de usuario.")

    print("CONFIG:", "DOMAIN=", domain, "USER_ID=", user_id, "SHEET_ID=", SHEET_ID)

    write_headers()

    sess = make_session(domain)
    item_ids = collect_item_ids_from_profile(sess, user_id, domain)

    if not item_ids:
        print("No se encontraron IDs en tu perfil (¿perfil privado o sin artículos?).")
        return

    items = fetch_all_details(sess, item_ids, domain)
    print(f"Total artículos (via profile scrape): {len(items)}")
    write_rows(items)

if __name__ == "__main__":
    main()
