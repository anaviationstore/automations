# sync_vinted_to_sheets.py
import os, re, json, time, random
import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright

# ---------- Config ----------
VINTED_DOMAIN = os.getenv("VINTED_DOMAIN", "es").strip()
ENV_USER_ID   = os.getenv("VINTED_USER_ID", "").strip()
ENV_PROFILE   = os.getenv("VINTED_PROFILE_URL", "").strip()
SHEET_ID      = os.getenv("SHEET_ID")
SHEET_TAB     = os.getenv("SHEET_TAB", "vinted_items")
GOOGLE_SA_JSON= os.getenv("GOOGLE_SA_JSON")

if not (SHEET_ID and GOOGLE_SA_JSON):
    raise SystemExit("Faltan variables: SHEET_ID o GOOGLE_SA_JSON")

# Parámetros anti-rate-limit
DETAIL_DELAY_MS = (1600, 2600)   # pausa aleatoria entre fichas (1.6–2.6 s)
BASE_BACKOFF_MS = 15000          # backoff inicial si nos limitan
MAX_RETRIES     = 5              # reintentos por ficha

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

# ---------- Playwright helpers ----------
ITEM_ID_RE = re.compile(r'(?:^|/)(?:items)/(\d+)(?:-|$)')

def collect_item_ids_with_browser(page, profile_url: str, domain: str) -> list[str]:
    url = profile_url
    if not url.startswith("http"):
        url = f"https://www.vinted.{domain}/member/{url}"
    sep = "&" if "?" in url else "?"
    url = f"{url}{sep}status=active&order=newest_first"

    print("[pw] goto", url)
    page.goto(url, wait_until="networkidle", timeout=60_000)

    try:
        page.locator("role=button[name=/Activos/i]").first.click(timeout=2_000)
    except Exception:
        pass

    seen_ids: set[str] = set()
    stable_rounds = 0
    for i in range(120):  # hasta ~120 scrolls
        hrefs = page.eval_on_selector_all("a[href]", "els => els.map(e => e.getAttribute('href'))")
        added = 0
        for h in hrefs:
            if not h:
                continue
            m = ITEM_ID_RE.search(h)
            if m:
                iid = m.group(1)
                if iid not in seen_ids:
                    seen_ids.add(iid)
                    added += 1
        print(f"[pw] scroll {i+1}: total_ids={len(seen_ids)} (+{added})")

        if added == 0:
            stable_rounds += 1
        else:
            stable_rounds = 0
        if stable_rounds >= 4:
            break

        page.evaluate("""
            const el = document.scrollingElement || document.documentElement || document.body;
            el.scrollTo(0, el.scrollHeight);
        """)
        # espera “humana”
        page.wait_for_timeout(1200 + random.randint(-150, 300))
        try:
            page.wait_for_load_state("networkidle", timeout=5000)
        except Exception:
            pass

    return list(seen_ids)

def _get_meta(page, prop):
    try:
        return page.eval_on_selector(f'meta[property="{prop}"]', "el => el ? el.content : null")
    except Exception:
        return None

def _parse_attributes_map(page):
    try:
        return page.evaluate("""
() => {
  const out = {};
  document.querySelectorAll('dt').forEach(dt => {
    const key = (dt.textContent || '').trim().toLowerCase();
    const dd = dt.nextElementSibling;
    if (!key || !dd) return;
    let val = (dd.textContent || '').trim();
    const a = dd.querySelector('a'); if (a) val = (a.textContent || '').trim();
    const span = dd.querySelector('span'); if (span && (!val || val.length < span.textContent.length)) val = (span.textContent || '').trim();
    out[key] = val;
  });
  return out;
}
""")
    except Exception:
        return {}

def _pick_attr(attr_map, variants):
    variants = [v.lower() for v in variants]
    for k, v in attr_map.items():
        if k.lower() in variants:
            return v
    for k, v in attr_map.items():
        lk = k.lower()
        if any(lk.startswith(pfx) or pfx in lk for pfx in variants):
            return v
    return ""

def _is_rate_limited(page, text_snippet=None):
    t = ""
    try:
        t = (page.title() or "").lower()
    except Exception:
        pass
    body = ""
    try:
        body = (text_snippet or page.content()).lower()
    except Exception:
        pass
    patterns = ("rate limited", "too many requests", "429", "demasiadas solicitudes", "muchas peticiones")
    return any(p in t or p in body for p in patterns)

def fetch_item_detail(page, req, item_id: str, domain: str) -> dict:
    """Usa la MISMA página/sesión para todas las fichas, con reintentos/backoff si hay rate-limit."""
    backoff = BASE_BACKOFF_MS
    for attempt in range(1, MAX_RETRIES + 1):
        # 1) API JSON con las cookies del contexto
        for url in (
            f"https://www.vinted.{domain}/api/v2/items/{item_id}",
            f"https://www.vinted.{domain}/web/api/v2/items/{item_id}",
        ):
            r = req.get(url, timeout=30_000)
            body = ""
            try:
                body = r.text()
            except Exception:
                body = ""
            if r.status == 429 or ("rate limited" in body.lower()):
                # backoff y reintentar
                sleep_ms = backoff + random.randint(0, 4000)
                print(f"[detail:{item_id}] API rate-limited (status {r.status}). Backoff {sleep_ms}ms (attempt {attempt}/{MAX_RETRIES})")
                page.wait_for_timeout(sleep_ms)
                backoff *= 1.7
                break  # reintenta el bucle externo
            if r.ok:
                try:
                    data = r.json()
                    obj = data.get("item") or data.get("data") or data
                    if isinstance(obj, dict):
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
                except Exception:
                    pass
        else:
            # 2) HTML: JSON-LD + metatags + atributos DOM
            item_url = f"https://www.vinted.{domain}/items/{item_id}"
            page.goto(item_url, wait_until="domcontentloaded", timeout=60_000)

            if _is_rate_limited(page):
                sleep_ms = backoff + random.randint(0, 4000)
                print(f"[detail:{item_id}] HTML rate-limited. Backoff {sleep_ms}ms (attempt {attempt}/{MAX_RETRIES})")
                page.wait_for_timeout(sleep_ms)
                backoff *= 1.7
                continue  # reintenta

            title = price_val = currency = brand = size = status = ""

            # JSON-LD
            try:
                el = page.query_selector('script[type="application/ld+json"]')
                data = el.text_content() if el else None
                if data:
                    ld = json.loads(data)
                    if isinstance(ld, dict):
                        title = ld.get("name","") or title
                        offers = ld.get("offers") or {}
                        if isinstance(offers, dict):
                            price_val = offers.get("price","") or price_val
                            currency = offers.get("priceCurrency","") or currency
                        if isinstance(ld.get("brand"), dict):
                            brand = ld["brand"].get("name","") or brand
            except Exception:
                pass

            # Metatags
            if not title:
                title = _get_meta(page, "og:title") or page.title()
            if not price_val:
                price_val = _get_meta(page, "product:price:amount") or _get_meta(page, "og:price:amount") or ""
            if not currency:
                currency = _get_meta(page, "product:price:currency") or _get_meta(page, "og:price:currency") or ""

            # Atributos del DOM
            attr_map = _parse_attributes_map(page)
            if not brand:
                brand = _pick_attr(attr_map, ["marca","brand","marque","marke","marca de","merk","marca:", "brand:"])
            if not size:
                size = _pick_attr(attr_map, ["talla","size","taille","größe","maat","taglia","rozmiar"])
            if not status:
                status = _pick_attr(attr_map, ["estado","condition","état","zustand","condición"])

            return {
                "id": item_id,
                "title": (title or "").strip(),
                "price": (price_val or "").strip(),
                "currency": (currency or "").strip(),
                "url": item_url,
                "brand": (brand or "").strip(),
                "size": (size or "").strip(),
                "status": (status or "").strip(),
            }

    # Si agotamos reintentos, devolvemos fila mínima para no romper flujo
    print(f"[detail:{item_id}] gave up after {MAX_RETRIES} attempts (rate limited).")
    return {
        "id": item_id,
        "title": "",
        "price": "", "currency": "",
        "url": f"https://www.vinted.{domain}/items/{item_id}",
        "brand": "", "size": "", "status": "",
    }

# ---------- Main ----------
def main():
    write_headers()

    profile_url = ENV_PROFILE
    if not profile_url:
        if ENV_USER_ID.isdigit():
            profile_url = f"https://www.vinted.{VINTED_DOMAIN}/member/{ENV_USER_ID}"
        else:
            raise SystemExit("Necesito VINTED_PROFILE_URL o VINTED_USER_ID numérico.")

    print("CONFIG:", "DOMAIN=", VINTED_DOMAIN, "PROFILE_URL=", profile_url, "SHEET_ID=", SHEET_ID)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"),
            locale="es-ES",
            viewport={"width": 1280, "height": 2000},
        )
        page = context.new_page()

        # 1) IDs desde el perfil (misma sesión)
        ids = collect_item_ids_with_browser(page, profile_url, VINTED_DOMAIN)
        print(f"[pw] total item ids found: {len(ids)}")
        if not ids:
            print("No hay IDs visibles (¿perfil con artículos ocultos/vacaciones?).")
            browser.close()
            return

        # 2) Detalles con la misma sesión/page y request context
        context.storage_state(path="playwright_state.json")
        req = context.request

        items = []
        for i, iid in enumerate(ids, 1):
            item = fetch_item_detail(page, req, iid, VINTED_DOMAIN)
            items.append(item)
            if i % 5 == 0:
                print(f"[detail] fetched {i}/{len(ids)}")
            # ritmo humano entre fichas
            page.wait_for_timeout(random.randint(*DETAIL_DELAY_MS))

        browser.close()

    print(f"Total artículos extraídos: {len(items)}")
    if items:
        write_rows(items)

if __name__ == "__main__":
    main()
