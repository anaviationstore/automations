# sync_vinted_to_sheets.py
import os, json, time
import requests
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

USER_ID_INT = int(VINTED_USER_ID)
print("CONFIG:", "DOMAIN=", VINTED_DOMAIN, "USER_ID=", USER_ID_INT, "SHEET_ID=", SHEET_ID)

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
    need = len(rows) - (ws.row_count - 1)
    if need > 0:
        ws.add_rows(need)
    ws.update(range_name=f"A2:H{len(rows)+1}", values=rows)

# ---------- Fetch directo con requests ----------
def fetch_items_requests(user_id:int, domain:str):
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": f"https://www.vinted.{domain}/",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "X-Requested-With": "XMLHttpRequest",
    })

    # 1) Visita home para cookies anti-bot
    home = f"https://www.vinted.{domain}/"
    r_home = sess.get(home, timeout=20)
    print("[requests] home status:", r_home.status_code)

    # 2) Token CSRF (si hay)
    csrf = None
    for k, v in sess.cookies.get_dict().items():
        if "csrf" in k.lower():
            csrf = v
            break
    if csrf:
        sess.headers["x-csrf-token"] = csrf
        print("[requests] csrf token present")

    results = []
    per_page = 96

    # Probar primero /users/{id}/items (más fiable), luego /catalog/items
    endpoints = [
        ("user_items", f"https://www.vinted.{domain}/api/v2/users/{user_id}/items",
         {"order": "newest_first", "status": "active"}),
        ("catalog",    f"https://www.vinted.{domain}/api/v2/catalog/items",
         {"order": "newest_first", "status": "active", "user_id": user_id, "search_text": ""}),
    ]

    for tag, url, base_params in endpoints:
        page = 1
        endpoint_results = []
        while True:
            params = dict(base_params, page=page, per_page=per_page)
            r = sess.get(url, params=params, timeout=25)
            print(f"[requests {tag}] page={page} status={r.status_code} len={len(r.content)}")

            if r.status_code in (401, 403, 429, 500, 502, 503):
                time.sleep(1.0)
                r = sess.get(url, params=params, timeout=25)
                print(f"[requests {tag}] retry page={page} status={r.status_code}")

            if r.status_code != 200:
                print(f"[requests {tag}] abort: status {r.status_code} snippet: {r.text[:160]}")
                break

            try:
                data = r.json()
            except Exception as e:
                print(f"[requests {tag}] json error:", repr(e), "snippet:", r.text[:200])
                break

            items = data.get("items", [])
            print(f"[requests {tag}] items this page: {len(items)}")
            if not items:
                break

            # --- Filtrar estrictamente por el usuario ---
            for x in items:
                uid = x.get("user_id") or (x.get("user") or {}).get("id")
                if uid != user_id:
                    continue  # descartamos lo que no sea tuyo

                price_field = x.get("price", "")
                if isinstance(price_field, dict):
                    price_val = price_field.get("amount", "")
                    currency = price_field.get("currency_code", "")
                else:
                    price_val = price_field
                    currency = x.get("currency") or x.get("currency_code", "")

                url_item = x.get("url") or (f"https://www.vinted.{domain}{x.get('path')}" if x.get("path") else "")
                endpoint_results.append({
                    "id": x.get("id", ""),
                    "title": x.get("title", ""),
                    "price": price_val,
                    "currency": currency,
                    "url": url_item,
                    "brand": x.get("brand_title", ""),
                    "size": x.get("size_title", ""),
                    "status": x.get("status", ""),
                })

            page += 1
            time.sleep(0.3)

        if endpoint_results:
            results.extend(endpoint_results)
            break  # ya tenemos datos correctos con este endpoint

    return results

# ---------- Main ----------
def main():
    write_headers()
    items = fetch_items_requests(USER_ID_INT, VINTED_DOMAIN)
    print(f"Total artículos (filtrados por tu user_id): {len(items)}")
    if items:
        write_rows(items)

if __name__ == "__main__":
    main()
