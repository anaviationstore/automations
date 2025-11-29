# sync_etsy_to_sheets.py
import os, re, json, time, requests
from datetime import datetime, timezone
from typing import Tuple, List, Dict, Any

import gspread
from google.oauth2.service_account import Credentials

# ---------- Helpers genéricos ----------
def as_text(x) -> str:
    if x is None:
        return ""
    return str(x)

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

# ---------- Config ----------
SHEET_ID       = os.getenv("SHEET_ID")
SHEET_TAB      = os.getenv("SHEET_TAB_ETSY", "etsy_items")
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")

ETSY_PROFILE_URL   = (os.getenv("ETSY_PROFILE_URL") or os.getenv("ETSY_SHOP_URL") or "").strip()
ETSY_SHOP_ID       = os.getenv("ETSY_SHOP_ID", "").strip()
ETSY_SHOP_NAME     = os.getenv("ETSY_SHOP_NAME", "").strip()      # opcional si ya tienes la URL de la shop
ETSY_CLIENT_ID     = os.getenv("ETSY_CLIENT_ID", "").strip()      # tu keystring
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
    ws = sh.add_worksheet(title=SHEET_TAB, rows=2000, cols=20)

HEADERS = [
    "id","title","price","currency","url","state",
    "tags","description","shop_name","shop_url","timestamp_utc"
]

def write_headers_and_clear_data_block():
    """
    - No borra toda la hoja.
    - Mantiene la fila 1 (cabeceras).
    - Limpia solo A2..(end_col,last_row) donde escribimos nuestros datos.
    """
    end_col = _col_letter(len(HEADERS))
    last_row = max(2, ws.row_count)

    # Reescribe cabeceras (argumentos nombrados para evitar DeprecationWarning)
    ws.update(values=[HEADERS], range_name="A1")

    rng = f"A2:{end_col}{last_row}"
    try:
        # batch_clear es método del Spreadsheet
        ws.spreadsheet.batch_clear([rng])
    except Exception:
        # Fallback: sobreescribe con vacío
        empty_rows = last_row - 1
        if empty_rows > 0:
            ws.update(
                values=[[""] * len(HEADERS) for _ in range(empty_rows)],
                range_name=rng,
                value_input_option="RAW",
            )

def write_rows(rows: List[List[str]]):
    if not rows:
        return
    need = len(rows) - (ws.row_count - 1)
    if need > 0:
        ws.add_rows(need)
    end_col = _col_letter(len(HEADERS))
    ws.update(values=rows, range_name=f"A2:{end_col}{len(rows)+1}")

# ---------- Etsy API helpers ----------
API_BASE = "https://api.etsy.com/v3"

def oauth_refresh() -> str:
    """
    Intercambia el refresh token por un access token.
    Etsy devuelve access_token con formato "<user_id>.<token>".
    """
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
    return as_text(data.get("access_token"))

def auth_headers(access_token: str) -> Dict[str, str]:
    return {
        "x-api-key": ETSY_CLIENT_ID,
        "Authorization": f"Bearer {access_token}",
    }

def shop_name_from_url(url: str) -> str:
    if not url: 
        return ""
    m = re.search(r"/shop/([^/?#]+)", url)
    return m.group(1) if m else ""

def resolve_shop_id(access_token: str) -> Tuple[str, str]:
    """Devuelve (shop_id, shop_name). Acepta SHOP_ID o SHOP_NAME o PROFILE_URL."""
    if ETSY_SHOP_ID:
        # Si dan el ID, intentamos recuperar nombre
        try:
            r = requests.get(
                f"{API_BASE}/application/shops/{ETSY_SHOP_ID}",
                headers=auth_headers(access_token), timeout=30
            )
            name = ""
            if r.ok:
                sj = r.json() or {}
                name = as_text(sj.get("shop_name") or sj.get("shop_name_full") or "")
            return ETSY_SHOP_ID, name
        except Exception:
            return ETSY_SHOP_ID, ""

    name = ETSY_SHOP_NAME or shop_name_from_url(ETSY_PROFILE_URL)
    if not name:
        raise SystemExit("Necesito ETSY_SHOP_ID o ETSY_SHOP_NAME o ETSY_PROFILE_URL para resolver la tienda.")

    # Búsqueda por nombre
    url = f"{API_BASE}/application/shops?shop_name={name}"
    r = requests.get(url, headers=auth_headers(access_token), timeout=30)
    if not r.ok:
        raise SystemExit(f"No pude resolver shop_id para '{name}': {r.status_code} {r.text}")

    data = r.json() or {}
    shop = None
    for key in ("results","shops","data"):
        if isinstance(data.get(key), list) and data[key]:
            shop = data[key][0]; break
    if not shop and "shop_id" in data:
        shop = data

    if not shop or "shop_id" not in shop:
        raise SystemExit(f"Respuesta inesperada al resolver '{name}': {data}")

    return as_text(shop["shop_id"]), as_text(shop.get("shop_name", name))

def fetch_active_listings(access_token: str, shop_id: str) -> List[Dict[str, Any]]:
    """
    Pagina por todas las publicaciones activas.
    Maneja 'count' + 'results' / 'listings' / 'data'.
    """
    all_items: List[Dict[str, Any]] = []
    limit, offset = 100, 0
    base = f"{API_BASE}/application/shops/{shop_id}/listings/active"
    while True:
        url = f"{base}?limit={limit}&offset={offset}"
        r = requests.get(url, headers=auth_headers(access_token), timeout=30)
        if not r.ok:
            raise SystemExit(f"Error en listings: {r.status_code} {r.text}")

        payload = r.json() or {}
        results = payload.get("results") or payload.get("listings") or payload.get("data") or []
        if not isinstance(results, list):
            results = []

        all_items.extend(results)

        total = payload.get("count")
        # Si 'count' no viene, paramos cuando la página trae menos que limit
        if total is None and len(results) < limit:
            break

        offset += limit
        if total is not None and len(all_items) >= int(total):
            break

        time.sleep(0.2)
    return all_items

def money_to_str(m: Any) -> Tuple[str, str]:
    """
    Convierte objeto money {amount, divisor, currency_code} a ('12.34','EUR').
    Acepta variaciones ocasionales (string/None).
    """
    if not isinstance(m, dict):
        return "", ""
    amount = m.get("amount")
    divisor = m.get("divisor") or 100
    curr = as_text(m.get("currency_code") or "")
    try:
        val = float(amount) / float(divisor)
        s = f"{val:.2f}".rstrip('0').rstrip('.')
    except Exception:
        s = ""
    return s, curr

def normalize_row(li: Dict[str, Any], shop_name: str, shop_url: str) -> List[str]:
    listing_id = li.get("listing_id") or li.get("listingId") or li.get("id")
    title = as_text(li.get("title", ""))
    state = as_text(li.get("state", ""))
    url = f"https://www.etsy.com/listing/{listing_id}" if listing_id else ""

    # precio puede venir como objeto money o como número/str + currency_code
    price_val, curr = "", ""
    p = li.get("price")
    if isinstance(p, dict):
        price_val, curr = money_to_str(p)
    elif isinstance(p, (int, float, str)):
        price_val = as_text(p)
        curr = as_text(li.get("currency_code", ""))
    else:
        price_val, curr = money_to_str(li.get("original_price", {}))

    tags_list = li.get("tags") or []
    if isinstance(tags_list, list):
        tags = ", ".join(as_text(t) for t in tags_list)
    else:
        tags = as_text(tags_list)

    desc = as_text(li.get("description", "")).strip()
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    return [
        as_text(listing_id or ""),
        title,
        price_val,
        curr,
        url,
        state,
        tags,
        desc,
        as_text(shop_name or ""),
        as_text(shop_url or ""),
        ts,
    ]

# ---------- Main ----------
def main():
    # Limpia solo nuestro bloque (A..end_col) desde la fila 2 y deja todo lo demás intacto
    write_headers_and_clear_data_block()

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
