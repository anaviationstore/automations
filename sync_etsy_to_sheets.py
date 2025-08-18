import os, re, json, time, requests
from datetime import datetime, timezone
import gspread
from google.oauth2.service_account import Credentials

# ---------- Config ----------
SHEET_ID       = os.getenv("SHEET_ID")
SHEET_TAB      = os.getenv("SHEET_TAB_ETSY", "etsy_items")
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")

ETSY_PROFILE_URL   = os.getenv("ETSY_PROFILE_URL") or os.getenv("ETSY_SHOP_URL") or ""
ETSY_SHOP_ID       = os.getenv("ETSY_SHOP_ID", "").strip()
ETSY_SHOP_NAME     = os.getenv("ETSY_SHOP_NAME", "").strip()  # opcional si ya tienes la URL de la shop
ETSY_CLIENT_ID     = os.getenv("ETSY_CLIENT_ID", "").strip()  # "keystring"
ETSY_REFRESH_TOKEN = os.getenv("ETSY_REFRESH_TOKEN", "").strip()

if not (SHEET_ID and GOOGLE_SA_JSON and ETSY_CLIENT_ID and ETSY_REFRESH_TOKEN):
    raise SystemExit("Faltan variables: SHEET_ID, GOOGLE_SA_JSON, ETSY_CLIENT_ID o ETSY_REFRESH_TOKEN")

# ---------- Google Sheets ----------
creds = Credentials.from_service_account_info(
    json.loads(GOOGLE_SA_JSON),
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
try:
    ws = sh.worksheet(SHEET_TAB)
except gspread.exceptions.WorksheetNotFound:
    ws = sh.add_worksheet(title=SHEET_TAB, rows=2, cols=12)

HEADERS = [
    "id","title","price","currency","url","state",
    "tags","description","shop_name","shop_url","timestamp_utc"
]

def a1(end_col, end_row):
    # end_col: número -> A, B, C ...
    def col_name(n):
        s=""
        while n>0:
            n, r = divmod(n-1, 26)
            s = chr(65+r)+s
        return s
    return f"A1:{col_name(end_col)}{end_row}"

def write_headers():
    ws.clear()
    ws.update(range_name=a1(len(HEADERS), 1), values=[HEADERS])

def write_rows(rows):
    if not rows:
        return
    need = len(rows) - (ws.row_count - 1)
    if need > 0:
        ws.add_rows(need)
    ws.update(range_name=a1(len(HEADERS), len(rows)+1), values=rows)

# ---------- Etsy API helpers ----------
API_BASE = "https://api.etsy.com/v3"

def oauth_refresh():
    """Intercambia el refresh token por un access token."""
    url = f"{API_BASE}/public/oauth/token"
    body = {
        "grant_type": "refresh_token",
        "client_id": ETSY_CLIENT_ID,
        "refresh_token": ETSY_REFRESH_TOKEN,
    }
    r = requests.post(url, json=body, timeout=30)
    if not r.ok:
        raise SystemExit(f"OAuth refresh failed: {r.status_code} {r.text}")
    data = r.json()
    # Etsy devuelve un access_token en formato "<user_id>.<token>"
    return data["access_token"]

def auth_headers(access_token: str):
    return {
        "x-api-key": ETSY_CLIENT_ID,
        "Authorization": f"Bearer {access_token}",
    }

def shop_name_from_url(url: str) -> str:
    if not url: return ""
    m = re.search(r"/shop/([^/?#]+)", url)
    return m.group(1) if m else ""

def resolve_shop_id(access_token: str):
    """Devuelve (shop_id, shop_name)"""
    if ETSY_SHOP_ID:
        # si no conocemos el nombre, lo intentamos consultar
        try:
            r = requests.get(f"{API_BASE}/application/shops/{ETSY_SHOP_ID}",
                             headers=auth_headers(access_token), timeout=30)
            name = ""
            if r.ok:
                sj = r.json()
                name = sj.get("shop_name") or sj.get("shop_name_full") or ""
            return ETSY_SHOP_ID, name
        except Exception:
            return ETSY_SHOP_ID, ""
    name = ETSY_SHOP_NAME or shop_name_from_url(ETSY_PROFILE_URL)
    if not name:
        raise SystemExit("Necesito ETSY_SHOP_ID o ETSY_SHOP_NAME o ETSY_PROFILE_URL para resolver la tienda.")
    # Endpoint de búsqueda por nombre
    url = f"{API_BASE}/application/shops?shop_name={name}"
    r = requests.get(url, headers=auth_headers(access_token), timeout=30)
    if not r.ok:
        raise SystemExit(f"No pude resolver shop_id para '{name}': {r.status_code} {r.text}")
    data = r.json()
    # distintos wrappers posibles: "results", "shops", "data"...
    shop = None
    for key in ("results","shops","data"):
        if isinstance(data.get(key), list) and data[key]:
            shop = data[key][0]; break
    if not shop and "shop_id" in data:
        shop = data
    if not shop or "shop_id" not in shop:
        raise SystemExit(f"Respuesta inesperada al resolver '{name}': {data}")
    return str(shop["shop_id"]), shop.get("shop_name", name)

def fetch_active_listings(access_token: str, shop_id: str):
    """Pagina por todas las publicaciones activas."""
    all_items = []
    limit, offset = 100, 0
    base = f"{API_BASE}/application/shops/{shop_id}/listings/active"
    while True:
        url = f"{base}?limit={limit}&offset={offset}"
        r = requests.get(url, headers=auth_headers(access_token), timeout=30)
        if not r.ok:
            raise SystemExit(f"Error en listings: {r.status_code} {r.text}")
        payload = r.json()
        results = payload.get("results") or payload.get("listings") or payload.get("data") or []
        if not isinstance(results, list):
            results = []
        all_items.extend(results)
        total = payload.get("count", 0)
        offset += limit
        if len(all_items) >= total or not results:
            break
        time.sleep(0.2)
    return all_items

def money_to_str(m):
    """Convierte objeto money {amount, divisor, currency_code} a ('12.34','EUR')."""
    if not isinstance(m, dict): return "", ""
    amount = m.get("amount")
    divisor = m.get("divisor") or 100
    curr = m.get("currency_code") or ""
    try:
        val = float(amount) / float(divisor)
        s = f"{val:.2f}".rstrip('0').rstrip('.')
    except Exception:
        s = ""
    return s, curr

def normalize_row(li: dict, shop_name: str, shop_url: str):
    listing_id = li.get("listing_id") or li.get("listingId") or li.get("id")
    title = li.get("title","")
    state = li.get("state","")
    url = f"https://www.etsy.com/listing/{listing_id}" if listing_id else ""

    # precio puede venir como objeto money o como número+currency_code
    price_val, curr = "", ""
    p = li.get("price")
    if isinstance(p, dict):
        price_val, curr = money_to_str(p)
    elif isinstance(p, (int,float,str)):
        price_val = str(p); curr = li.get("currency_code","")
    else:
        price_val, curr = money_to_str(li.get("original_price", {}))

    tags = ", ".join(li.get("tags", []) or [])
    desc = (li.get("description") or "").strip()
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return [str(listing_id or ""), title, price_val, curr, url, state, tags, desc, shop_name or "", shop_url or "", ts]

def main():
    write_headers()
    token = oauth_refresh()
    shop_id, shop_name = resolve_shop_id(token)
    shop_url = ETSY_PROFILE_URL or (f"https://www.etsy.com/shop/{shop_name}" if shop_name else "")

    items = fetch_active_listings(token, shop_id)
    print(f"Total listings: {len(items)}")
    rows = [normalize_row(li, shop_name, shop_url) for li in items]
    write_rows(rows)
    print("Finalizado.")

if __name__ == "__main__":
    main()
