import os
import json
import hmac
import hashlib
import subprocess
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

# ============================================================
# R&B FITNESS – GYMLEADHUB WEBHOOK BRIDGE (UPDATED)
# ------------------------------------------------------------
# Receives GymLeadHub booking webhooks and instantly generates
# the WhatsApp-ready programme via slot_generator_v2.py
#
# How it works:
# 1) GymLeadHub POSTs booking payload to /webhooks/gymleadhub
# 2) Verify secret (header)
# 3) Normalise payload -> session(s)
# 4) Write bookings_stub.json in the exact v2 stub format:
#    {
#      "sessions": [
#        {
#          "start": "YYYY-MM-DDTHH:MM:SS",
#          "clients": [
#            {"name":"Kate","focus":"Shoulders, Cardio & Abs"},
#            ...
#          ]
#        }
#      ]
#    }
# 5) Run slot_generator_v2.py --auto --source stub --window-hours X
# 6) Output appears in generated_slots/
#
# NOTES:
# - Because GymLeadHub docs are unknown, this supports a few
#   payload shapes and logs everything for us to refine.
# - If "focus" isn't provided per client, it uses
#   client_focus_map.json as fallback.
# ============================================================

APP_HOST = os.getenv("RB_WEBHOOK_HOST", "127.0.0.1")
APP_PORT = int(os.getenv("RB_WEBHOOK_PORT", "8010"))

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR = os.path.join(PROJECT_DIR, "logs")
GENERATED_DIR = os.path.join(PROJECT_DIR, "generated_slots")

BOOKINGS_STUB_PATH = os.path.join(PROJECT_DIR, "bookings_stub.json")
FOCUS_MAP_PATH = os.path.join(PROJECT_DIR, "client_focus_map.json")

SLOT_GENERATOR_V2 = os.path.join(PROJECT_DIR, "slot_generator_v2.py")

# Shared secret (set in Windows env or loaded some other way)
# GymLeadHub must send this in header: X-RB-Webhook-Secret
WEBHOOK_SECRET = os.getenv("GYMLEADHUB_WEBHOOK_SECRET", "")

# Optional: If GymLeadHub supports signing the body:
# X-RB-Signature = HMAC-SHA256 hex digest of raw body using WEBHOOK_SECRET
VERIFY_HMAC_SIGNATURE = os.getenv("GYMLEADHUB_VERIFY_HMAC", "false").lower() == "true"

os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(GENERATED_DIR, exist_ok=True)

# Ensure log files exist
for fn in ("gymleadhub_webhooks.log", "gymleadhub_bridge.log"):
    p = os.path.join(LOGS_DIR, fn)
    if not os.path.exists(p):
        with open(p, "w", encoding="utf-8") as f:
            f.write("")

app = FastAPI(title="R&B GymLeadHub Webhook Bridge")


def _now_str() -> str:
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")


def _append_log(filename: str, text: str) -> None:
    path = os.path.join(LOGS_DIR, filename)
    with open(path, "a", encoding="utf-8") as f:
        f.write(text.rstrip() + "\n")


def _tail(s: Optional[str], n: int = 2000) -> str:
    if not s:
        return ""
    s = str(s)
    return s[-n:]


def _load_focus_map() -> Dict[str, str]:
    if not os.path.exists(FOCUS_MAP_PATH):
        return {}
    try:
        with open(FOCUS_MAP_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return {str(k).strip().lower(): str(v).strip() for k, v in data.items()}
    except Exception:
        pass
    return {}


def _verify_secret_header(request: Request) -> None:
    if not WEBHOOK_SECRET:
        raise HTTPException(
            status_code=500,
            detail="Server not configured: GYMLEADHUB_WEBHOOK_SECRET missing",
        )

    got = request.headers.get("X-RB-Webhook-Secret", "")
    if got != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Invalid webhook secret")


def _verify_hmac_signature(raw_body: bytes, request: Request) -> None:
    if not VERIFY_HMAC_SIGNATURE:
        return

    sig = request.headers.get("X-RB-Signature", "")
    if not sig:
        raise HTTPException(status_code=401, detail="Missing signature")

    expected = hmac.new(WEBHOOK_SECRET.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig.strip().lower(), expected.lower()):
        raise HTTPException(status_code=401, detail="Invalid signature")


def _parse_datetime(value: Any) -> Optional[datetime]:
    """
    Tries to parse a datetime from common formats:
    - ISO: 2026-03-06T18:30:00Z / 2026-03-06T18:30:00+00:00 / without tz
    - Date/time strings: 06/03/26 + 18:30
    """
    if value is None:
        return None

    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value))
        except Exception:
            return None

    s = str(value).strip()
    if not s:
        return None

    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%y %H:%M",
        "%d/%m/%Y %H:%M",
    ):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass

    if s.endswith("Z"):
        s2 = s.replace("Z", "+0000").replace(":", "")
        for fmt in ("%Y-%m-%dT%H%M%S%z", "%Y-%m-%dT%H%M%S.%f%z"):
            try:
                return datetime.strptime(s2, fmt)
            except Exception:
                pass

    return None


def _normalise_sessions(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Supports:
    - Single event payload -> one session
    - Batch payload -> payload["events"] or payload["bookings"] is a list
    """
    if isinstance(payload.get("events"), list):
        return [e for e in payload["events"] if isinstance(e, dict)]
    if isinstance(payload.get("bookings"), list):
        return [b for b in payload["bookings"] if isinstance(b, dict)]
    return [payload]


def _extract_session_datetime(session_payload: Dict[str, Any]) -> Optional[datetime]:
    for key in ("start", "start_time", "startTime", "session_start", "booking_start", "datetime"):
        dt = _parse_datetime(session_payload.get(key))
        if dt:
            return dt

    d = session_payload.get("date")
    t = session_payload.get("time")
    if d and t:
        return _parse_datetime(f"{d} {t}")

    return None


def _normalise_clients(session_payload: Dict[str, Any], focus_map: Dict[str, str]) -> List[Dict[str, str]]:
    """
    We accept a few possible webhook shapes:
    - session_payload["clients"] = [{"name":"Kate","focus":"..."}, ...]
    - session_payload["attendees"] = [{"full_name":"Kate","workout_focus":"..."}, ...]
    - session_payload["client_names"] = ["Kate","Russel"] (focus from focus_map)
    - session_payload["client_name"] = "Kate" (single booking)
    """
    clients_out: List[Dict[str, str]] = []

    def add_client(name: Any, focus: Any) -> None:
        n = str(name or "").strip()
        if not n:
            return
        f = str(focus or "").strip()
        if not f:
            f = focus_map.get(n.lower(), "")
        clients_out.append({"name": n, "focus": f})

    if isinstance(session_payload.get("clients"), list):
        for c in session_payload["clients"]:
            if isinstance(c, dict):
                add_client(
                    c.get("name") or c.get("client_name") or c.get("full_name"),
                    c.get("focus") or c.get("workout_focus"),
                )
            else:
                add_client(c, "")

    elif isinstance(session_payload.get("attendees"), list):
        for c in session_payload["attendees"]:
            if isinstance(c, dict):
                add_client(
                    c.get("name") or c.get("client_name") or c.get("full_name"),
                    c.get("focus") or c.get("workout_focus"),
                )
            else:
                add_client(c, "")

    elif isinstance(session_payload.get("client_names"), list):
        for n in session_payload["client_names"]:
            add_client(n, "")

    elif session_payload.get("client_name") or session_payload.get("full_name"):
        add_client(
            session_payload.get("client_name") or session_payload.get("full_name"),
            session_payload.get("focus") or session_payload.get("workout_focus") or "",
        )

    return clients_out


def _write_bookings_stub_v2_format(stub_sessions: List[Dict[str, Any]]) -> None:
    """
    Writes the exact stub format slot_generator_v2 expects:
    {"sessions": [ {"start": "...", "clients": [...]}, ... ]}
    """
    out = {"sessions": stub_sessions}
    with open(BOOKINGS_STUB_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)


def _run_slot_generator(window_hours: int = 72) -> Dict[str, Any]:
    """
    Calls v2 in auto/stub mode.
    """
    if not os.path.exists(SLOT_GENERATOR_V2):
        raise RuntimeError("slot_generator_v2.py not found in project folder")

    cmd = ["python", SLOT_GENERATOR_V2, "--auto", "--source", "stub", "--window-hours", str(window_hours)]
    _append_log("gymleadhub_bridge.log", f"[{_now_str()}] Running: {' '.join(cmd)}")

    p = subprocess.run(cmd, cwd=PROJECT_DIR, capture_output=True, text=True)

    _append_log("gymleadhub_bridge.log", f"[{_now_str()}] Return code: {p.returncode}")
    _append_log("gymleadhub_bridge.log", "STDOUT (tail):\n" + (_tail(p.stdout, 4000) or "<empty>"))
    _append_log("gymleadhub_bridge.log", "STDERR (tail):\n" + (_tail(p.stderr, 4000) or "<empty>"))

    return {"returncode": p.returncode, "stdout": p.stdout or "", "stderr": p.stderr or ""}


@app.get("/health")
async def health():
    return {"status": "ok", "time": _now_str()}


@app.post("/webhooks/gymleadhub")
async def gymleadhub_webhook(request: Request):
    raw = await request.body()

    try:
        _verify_secret_header(request)
        _verify_hmac_signature(raw, request)

        try:
            payload = await request.json()
        except Exception:
            _append_log(
                "gymleadhub_webhooks.log",
                f"[{_now_str()}] INVALID JSON:\n{raw.decode('utf-8', errors='replace')}",
            )
            raise HTTPException(status_code=400, detail="Invalid JSON")

        _append_log("gymleadhub_webhooks.log", f"[{_now_str()}] PAYLOAD:\n{json.dumps(payload, indent=2)}")

        focus_map = _load_focus_map()
        sessions_payloads = _normalise_sessions(payload)

        stub_sessions: List[Dict[str, Any]] = []

        for sp in sessions_payloads:
            dt = _extract_session_datetime(sp)
            if not dt:
                continue

            clients = _normalise_clients(sp, focus_map)

            stub_sessions.append(
                {
                    "start": dt.strftime("%Y-%m-%dT%H:%M:%S"),
                    "clients": clients,
                }
            )

        if not stub_sessions:
            return JSONResponse(
                status_code=202,
                content={"status": "accepted_no_sessions_found", "message": "Webhook received but no session datetime found"},
            )

        # Write stub file in v2 expected format
        _write_bookings_stub_v2_format(stub_sessions)
        _append_log("gymleadhub_bridge.log", f"[{_now_str()}] Wrote bookings_stub.json with {len(stub_sessions)} session(s).")

        # Run generator
        res = _run_slot_generator(window_hours=72)

        if res["returncode"] != 0:
            return JSONResponse(
                status_code=500,
                content={
                    "status": "error",
                    "message": "Generator failed",
                    "returncode": res["returncode"],
                    "stdout_tail": _tail(res["stdout"], 2000),
                    "stderr_tail": _tail(res["stderr"], 2000),
                },
            )

        return {"status": "ok", "generated": True, "sessions_received": len(stub_sessions)}

    except HTTPException:
        raise
    except Exception as e:
        tb = traceback.format_exc()
        _append_log("gymleadhub_bridge.log", f"[{_now_str()}] EXCEPTION:\n{tb}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": "Bridge exception",
                "error": str(e),
                "traceback_tail": _tail(tb, 2000),
            },
        )