import os
import re
import json
import time
import argparse
import inspect
from datetime import datetime, timedelta

CONFIG_FILE = "rb_slot_config.json"
BOOKINGS_STUB_FILE = "bookings_stub.json"
CLIENT_OVERRIDES_FILE = "client_overrides.json"

CACHE_DIR = "cache"
CACHE_FILE = os.path.join(CACHE_DIR, "sheets_cache.json")

MAX_SHEETS_RETRIES = 4
BACKOFF_SECONDS = [5, 10, 20, 40]


def safe_str(v) -> str:
    return "" if v is None else str(v).strip()

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", safe_str(s)).strip().lower()

def load_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path: str, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def _load_runtime_config():
    cfg_path = os.path.join(os.getcwd(), CONFIG_FILE)
    if not os.path.exists(cfg_path):
        raise RuntimeError(f"Missing {CONFIG_FILE} in: {os.getcwd()}")
    data = load_json(cfg_path, {})
    if not isinstance(data, dict):
        raise RuntimeError(f"{CONFIG_FILE} must be a JSON object.")
    return data

def load_sheets_config():
    cfg = _load_runtime_config().get("sheets", {})
    if not isinstance(cfg, dict):
        cfg = {}

    spreadsheet_id = safe_str(cfg.get("spreadsheet_id"))
    service_account_path = safe_str(cfg.get("service_account_path")) or "service_account.json"
    automation_tab_name = safe_str(cfg.get("automation_tab_name")) or "RBSLOT_AUTOMATION"
    bookings_tab_name = safe_str(cfg.get("bookings_tab_name")) or "RBSLOT_BOOKINGS"

    if not spreadsheet_id:
        raise RuntimeError("Missing sheets.spreadsheet_id in rb_slot_config.json")

    return {
        "spreadsheet_id": spreadsheet_id,
        "service_account_path": service_account_path,
        "automation_tab_name": automation_tab_name,
        "bookings_tab_name": bookings_tab_name,
    }


# -------------------------
# UK date parsing
# -------------------------
def parse_sheet_datetime(value: str):
    s = safe_str(value)
    if not s:
        return None

    # ISO direct
    try:
        return datetime.fromisoformat(s)
    except Exception:
        pass

    # normalise dashes to slashes
    s2 = s.replace("-", "/")

    fmts = [
        # UK first
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%y %H:%M:%S",
        "%d/%m/%y %H:%M",

        # US-ish fallback (Sheets sometimes)
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",

        # ISO-ish with spaces
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
    ]

    for fmt in fmts:
        try:
            return datetime.strptime(s2, fmt)
        except Exception:
            continue

    return None


# -------------------------
# Sheets read once with caching + backoff
# -------------------------
def _ensure_gspread():
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        return gspread, Credentials
    except Exception:
        raise RuntimeError("Missing Google Sheets libraries. Run: pip install gspread google-auth")

def _is_quota_429(err: Exception) -> bool:
    msg = str(err).lower()
    return ("429" in msg) and ("quota" in msg or "rate" in msg)

def _fetch_rows_once():
    gspread, Credentials = _ensure_gspread()
    cfg = load_sheets_config()

    service_account_path = cfg["service_account_path"]
    if not os.path.isabs(service_account_path):
        service_account_path = os.path.join(os.getcwd(), service_account_path)

    if not os.path.exists(service_account_path):
        raise RuntimeError(f"service_account.json not found at: {service_account_path}")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(service_account_path, scopes=scopes)
    gc = gspread.authorize(creds)

    sh = gc.open_by_key(cfg["spreadsheet_id"])
    ws_bookings = sh.worksheet(cfg["bookings_tab_name"])
    ws_overrides = sh.worksheet(cfg["automation_tab_name"])

    bookings_rows = ws_bookings.get_all_values()
    overrides_rows = ws_overrides.get_all_values()
    return bookings_rows, overrides_rows

def read_sheets_with_cache(cache_minutes: int):
    os.makedirs(CACHE_DIR, exist_ok=True)
    ttl_seconds = max(0, int(cache_minutes)) * 60

    cached = load_json(CACHE_FILE, {})
    if isinstance(cached, dict) and cached.get("fetched_at"):
        try:
            fetched_at = float(cached["fetched_at"])
            age = time.time() - fetched_at
            if ttl_seconds > 0 and age <= ttl_seconds:
                print(f"Cache hit [OK] (age {int(age)}s) - using cached Sheets data.")
                return cached.get("bookings_rows", []), cached.get("overrides_rows", [])
        except Exception:
            pass

    last_err = None
    for attempt in range(1, MAX_SHEETS_RETRIES + 1):
        try:
            print(f"Reading Google Sheets... (attempt {attempt}/{MAX_SHEETS_RETRIES})")
            bookings_rows, overrides_rows = _fetch_rows_once()

            payload = {
                "fetched_at": time.time(),
                "bookings_rows": bookings_rows,
                "overrides_rows": overrides_rows,
            }
            save_json(CACHE_FILE, payload)

            print(f"Loaded bookings rows: {max(0, len(bookings_rows) - 1)}")
            print(f"Loaded overrides rows: {max(0, len(overrides_rows) - 1)}")
            return bookings_rows, overrides_rows

        except Exception as e:
            last_err = e
            if _is_quota_429(e) and attempt < MAX_SHEETS_RETRIES:
                wait_s = BACKOFF_SECONDS[min(attempt - 1, len(BACKOFF_SECONDS) - 1)]
                print(f"Sheets quota hit (429). Backing off for {wait_s}s then retrying...")
                time.sleep(wait_s)
                continue
            raise

    raise last_err if last_err else RuntimeError("Failed to read Sheets.")


# -------------------------
# Overrides transform
# -------------------------
def _parse_bool_cell(v) -> bool:
    s = safe_str(v).lower()
    return s in ("true", "1", "yes", "y", "on")

def _parse_hard_bans(v) -> list:
    s = safe_str(v)
    if not s:
        return []
    parts = [p.strip() for p in s.split(",") if p.strip()]
    return [norm(p) for p in parts if p.strip()]

def build_overrides_dict(overrides_rows: list) -> dict:
    if not overrides_rows or len(overrides_rows) < 2:
        return {}

    headers = [safe_str(h) for h in overrides_rows[0]]
    idx = {h: i for i, h in enumerate(headers)}

    def get(row, key):
        i = idx.get(key, None)
        if i is None:
            return ""
        return row[i] if i < len(row) else ""

    out = {}
    for r in overrides_rows[1:]:
        if not any(safe_str(x) for x in r):
            continue
        name = safe_str(get(r, "client_name"))
        if not name:
            continue

        out[norm(name)] = {
            "client_name": name.strip(),
            "omit_conditioning": _parse_bool_cell(get(r, "omit_conditioning")),
            "force_core_finisher": _parse_bool_cell(get(r, "force_core_finisher")),
            "force_no_cardio": _parse_bool_cell(get(r, "force_no_cardio")),
            "no_planks": _parse_bool_cell(get(r, "no_planks")),
            "no_floor": _parse_bool_cell(get(r, "no_floor")),
            "supported_only": _parse_bool_cell(get(r, "supported_only")),
            "no_spinal_flexion": _parse_bool_cell(get(r, "no_spinal_flexion")),
            "ban_bike": _parse_bool_cell(get(r, "ban_bike")),
            "ban_burpees": _parse_bool_cell(get(r, "ban_burpees")),
            "extra_abs": _parse_bool_cell(get(r, "extra_abs")),
            "force_abs_challenge": _parse_bool_cell(get(r, "force_abs_challenge")),
            "hard_bans": _parse_hard_bans(get(r, "hard_bans")),
        }

    return out


# -------------------------
# Bookings -> sessions (debug)
# -------------------------
def build_sessions_from_bookings(bookings_rows: list, window_hours: int, include_past_hours: int):
    if not bookings_rows or len(bookings_rows) < 2:
        print("Bookings sheet has no data rows.")
        return []

    headers = [safe_str(h) for h in bookings_rows[0]]
    idx = {h: i for i, h in enumerate(headers)}

    print("Bookings headers:", headers)

    for req in ("start_iso", "clients", "focus"):
        if req not in idx:
            raise ValueError(f"Bookings tab missing header: {req}")

    now = datetime.now()
    start_window = now - timedelta(hours=int(include_past_hours))
    end_window = now + timedelta(hours=int(window_hours))

    print(f"NOW:          {now.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"WINDOW START: {start_window.strftime('%d/%m/%Y %H:%M:%S')} (include_past_hours={include_past_hours})")
    print(f"WINDOW END:   {end_window.strftime('%d/%m/%Y %H:%M:%S')} (window_hours={window_hours})")

    sessions = []
    for row_i, r in enumerate(bookings_rows[1:], start=2):
        if not any(safe_str(x) for x in r):
            continue

        start_raw = safe_str(r[idx["start_iso"]] if idx["start_iso"] < len(r) else "")
        clients_raw = safe_str(r[idx["clients"]] if idx["clients"] < len(r) else "")
        focus = safe_str(r[idx["focus"]] if idx["focus"] < len(r) else "")

        print(f"\nRow {row_i} RAW:")
        print(f"  start_iso: {start_raw!r}")
        print(f"  clients:   {clients_raw!r}")
        print(f"  focus:     {focus!r}")

        if not start_raw or not clients_raw or not focus:
            print("  SKIP: missing one of start_iso/clients/focus")
            continue

        dt = parse_sheet_datetime(start_raw)
        if dt is None:
            print("  SKIP: could not parse start_iso into datetime")
            continue

        print(f"  parsed_dt: {dt.strftime('%d/%m/%Y %H:%M:%S')}")

        if not (start_window <= dt <= end_window):
            print("  SKIP: outside window")
            continue

        clients = [c.strip() for c in clients_raw.split("|") if c.strip()]
        if not clients:
            print("  SKIP: no clients after splitting by |")
            continue

        sessions.append({
            "start": dt.isoformat(timespec="minutes"),
            "clients": clients,
            "focus": focus
        })
        print("  INCLUDED [OK]")

    sessions.sort(key=lambda s: s["start"])
    return sessions


def write_bookings_stub(sessions: list):
    payload = {"sessions": sessions}
    with open(os.path.join(os.getcwd(), BOOKINGS_STUB_FILE), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"\nWrote {BOOKINGS_STUB_FILE} [OK] ({len(sessions)} sessions)")

def write_client_overrides(overrides_dict: dict):
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "overrides_by_name_norm": overrides_dict
    }
    with open(os.path.join(os.getcwd(), CLIENT_OVERRIDES_FILE), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"Wrote {CLIENT_OVERRIDES_FILE} [OK] ({len(overrides_dict)} clients)")


def _snapshot_generated_files():
    gen_dir = os.path.join(os.getcwd(), "generated_slots")
    if not os.path.exists(gen_dir):
        return set()
    return set([p for p in os.listdir(gen_dir) if p.lower().endswith(".txt")])


def run_v2_stub(window_hours: int, overrides_dict: dict, clients_source: str):
    import slot_generator_v2

    slot_generator_v2.SHEETS_OVERRIDES = overrides_dict or {}

    print("\nRunning v2 in STUB mode...")

    fn = slot_generator_v2.run_auto_from_stub
    sig = inspect.signature(fn)
    params = list(sig.parameters.keys())

    if "clients_source" in params:
        fn(window_hours=int(window_hours), clients_source=clients_source)
    else:
        fn(window_hours=int(window_hours))


def main():
    parser = argparse.ArgumentParser(description="R&B Slot Generator v3 (Sheets once -> stub -> v2)")
    parser.add_argument("--window-hours", type=int, default=48, help="Window size in hours (default 48)")
    parser.add_argument("--cache-minutes", type=int, default=2, help="Cache minutes for Sheets reads (default 2)")
    parser.add_argument("--include-past-hours", type=int, default=0, help="Also include bookings this many hours in the past (default 0)")
    parser.add_argument("--clients-source", type=str, default="sheets", help="Passed into v2 stub runner if required (default sheets)")
    args = parser.parse_args()

    before = _snapshot_generated_files()

    bookings_rows, overrides_rows = read_sheets_with_cache(cache_minutes=int(args.cache_minutes))

    overrides_dict = build_overrides_dict(overrides_rows)
    sessions = build_sessions_from_bookings(
        bookings_rows,
        window_hours=int(args.window_hours),
        include_past_hours=int(args.include_past_hours),
    )

    print(f"\nSessions in window: {len(sessions)}")
    if not sessions:
        print("No sessions found in the window.")
        return

    write_bookings_stub(sessions)
    write_client_overrides(overrides_dict)

    run_v2_stub(
        window_hours=int(args.window_hours),
        overrides_dict=overrides_dict,
        clients_source=safe_str(args.clients_source) or "sheets",
    )

    after = _snapshot_generated_files()
    new_files = sorted(list(after - before))
    if new_files:
        for nf in new_files[-5:]:
            print(f"Generated: generated_slots\\{nf}")
    else:
        print("Generated: (no new files detected)")

    print("\nv3 complete [OK]")


if __name__ == "__main__":
    main()