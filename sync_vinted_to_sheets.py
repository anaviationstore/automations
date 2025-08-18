# sync_vinted_to_sheets.py
import os, re, json, time
import requests
import gspread
from google.oauth2.service_account import Credentials

# ---------- Config ----------
VINTED_DOMAIN = os.getenv("VINTED_DOMAIN", "es")         # es, fr, de, it...
VINTED_USER_ID = os.getenv("VINTED_USER_ID", "").strip() # si lo pones, debe ser número
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

# ---------- Utils ----------
def detect_domain_from_url(url: str, fallback: str) -> str:
    m = re.search(r"https?://www\.vinted\.([a-z.]+)/", url)
    return m.group(1) if m else fallback

def detect_user_id_from_profile(sess: requests.Session, url: str) -> int | None:
    # intentos: /member/123456-..., ...-123456, y JSON embebido
    for pat in (r"/member/(\d+)(?:[-/]|$)", r"-([0-9]+)(?:$|[/?])"):
        m = re.search(pat, url)
        if m:
            return int(m.group(1))
    r = sess.get(url, timeout=20)
    if r.status_code != 200:
        return None
    html = r.text
    for p in (r'"user_id"\s*:\s*(\d+)',
              r'"user"\s*:\s*\{\s*"id"\s*:\s*(\d+)',
              r'"id"\s*:\s*(\d+)\s*,\s*"login"\s*:'):
        m = re.search(p, html)
        if m:
            return int(m.group(1))
    # último recurso: buscar enlaces a /member/123...
    m = re.search(r'/member/(\d+)-', html)
    return int(m.group(1)) if m else None

def normalize_price_currency(price_field, x):
    if isinstance(price_field, dict):
        return price_field.get("amount", ""), price_field.get("currency_code", "")
    return price_field, (x.get("currency") or x.get("currency_code", ""))

def get_item_user_id(x):
    uid = x.get("user_id")
    if uid is None and isinstance(x.get("user"), dict):
        uid = x["user"].get("id")
    # homogeneizar a str para comparar
    return str(uid) if uid is not None else None

# ---------- Core fetch ----------
def fetch_items_requests(user_id:int, domain:str) -> list[dict]:
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": f"https://www.vinted.{domain}/",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "X-Requested-With": "XMLHttpRequest",
    })

    home = f"https://www.vinted.{domain}/"
    r_home = sess.get(home, timeout=20)
    print("[requests] home status:", r_home.status_code)

    csrf = next((v for k, v in sess.cookies.get_dict().items() if "csrf" in k.lower()), None)
    if csrf:
        sess.headers["x-csrf-token"] = csrf
        print("[requests] csrf token present")

    results = []
    per_page = 96

    # 1) users/{id}/items (si está disponible)
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
            time.sleep(0.6)
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
            price_val, currency = normalize_price_currency(x.get("price",""), x)
            url_item = x.get("url") or (f"https://www.vinted.{domain}{x.get('path')}" if x.get("path") else "")
            results.append({
                "id": x.get("id",""), "title": x.get("title",""),
                "price": price_val, "currency": currency, "url": url_item,
                "brand": x.get("brand_title",""), "size": x.get("size_title",""),
                "status": x.get("status",""),
            })
        page += 1
        time.sleep(0.25)

    if results:
        return results

    # 2) catalog/items con variantes de parámetro de usuario
    url_cat = f"https://www.vinted.{domain}/api/v2/catalog/items"
    param_variants = [
        {"user_id": user_id},
        {"user_id[]": user_id},
        {"user_ids[]": user_id},
        {"user_ids": user_id},
    ]

    user_id_str = str(user_id)

    for variant in param_variants:
        print("[requests catalog] trying variant:", variant)
        page = 1
        found_for_variant = 0
        while True:
            params = {
                "order": "newest_first", "status": "active", "search_text": "",
                "page": page, "per_page": per_page,
                **variant
            }
            r = sess.get(url_cat, params=params, timeout=25)
            print(f"[requests catalog] page={page} status={r.status_code} len={len(r.content)}")
            if r.status_code != 200:
                if r.status_code in (401,403,429,500,502,503):
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

            # filtro duro por user_id (comparación como string)
            for x in items:
                uid = get_item_user_id(x)
                if uid != user_id_str:
                    continue
                price_val, currency = normalize_price_currency(x.get("price",""), x)
                url_item = x.get("url") or (f"https://www.vinted.{domain}{x.get('path')}" if x.get("path") else "")
                results.append({
                    "id": x.get("id",""), "title": x.get("title",""),
                    "price": price_val, "currency": currency, "url": url_item,
                    "brand": x.get("brand_title",""), "size": x.get("size_title",""),
                    "status": x.get("status",""),
                })
                found_for_variant += 1

            page += 1
            # Evitar "Page offset invalid"
            if page > 30:
                break
            time.sleep(0.25)

        if found_for_variant:
            print(f"[requests catalog] matched items with variant {variant}: {found_for_variant}")
            break

    # dedup por id
    unique = {}
    for it in results:
        unique[str(it["id"])] = it
    return list(unique.values())

def main():
    write_headers()

    # Determinar user_id/domino correctos
    domain = VINTED_DOMAIN
    user_id = None
    if VINTED_USER_ID.isdigit():
        user_id = int(VINTED_USER_ID)

    if user_id is None:
        if not VINTED_PROFILE_URL:
            raise SystemExit("No tengo VINTED_USER_ID válido ni VINTED_PROFILE_URL para detectar el ID.")
        domain = detect_domain_from_url(VINTED_PROFILE_URL, VINTED_DOMAIN)
        sess = requests.Session()
        detected = detect_user_id_from_profile(sess, VINTED_PROFILE_URL)
        if not detected:
            raise SystemExit("No pude detectar tu user_id desde VINTED_PROFILE_URL. Revisa la URL de perfil.")
        user_id = detected

    print("CONFIG:", "DOMAIN=", domain, "USER_ID=", user_id, "SHEET_ID=", SHEET_ID)

    items = fetch_items_requests(user_id, domain)
    print(f"Total artículos (filtrados por tu user_id): {len(items)}")
    if items:
        write_rows(items)

if __name__ == "__main__":
    main()
