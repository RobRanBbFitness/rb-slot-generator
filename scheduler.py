import os
import smtplib
import subprocess
import time
import json
from datetime import datetime
from pathlib import Path
from email.message import EmailMessage

from dotenv import load_dotenv

# ==========================================
# R&B SLOT GENERATOR AUTO SCHEDULER (EMAIL)
# ==========================================
# Runs slot_generator_v3 every X minutes:
# - Reads Sheets (cached/backoff inside v3)
# - Generates NEW sessions only
# - Detects NEW generated files
# - Emails the programme text to coaches automatically
#
# Configure email in .env:
#
# SMTP_HOST=smtp.gmail.com
# SMTP_PORT=587
# SMTP_USER=randbfitnessptcentre@gmail.com
# SMTP_PASS=YOUR_APP_PASSWORD
# EMAIL_FROM=R&B Slot Generator <randbfitnessptcentre@gmail.com>
# COACH_EMAILS=april@email.com,rayner@email.com
# EMAIL_SUBJECT_PREFIX=R&B 5-to-1 Slot
# ==========================================

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
GENERATED_DIR = BASE_DIR / "generated_slots"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

SCHEDULER_LOG = LOG_DIR / "scheduler.log"
SCHEDULER_STATE = BASE_DIR / "scheduler_state.json"

CHECK_INTERVAL_SECONDS = 300  # 5 minutes
WINDOW_HOURS = 48
CACHE_MINUTES = 2

# If v3 hits quota, wait longer
EXTRA_WAIT_ON_429_SECONDS = 120


# =========================
# ENV HELPERS
# =========================
def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _parse_emails(csv: str):
    out = []
    for part in (csv or "").split(","):
        p = part.strip()
        if p:
            out.append(p)
    return out


def _email_enabled() -> bool:
    return bool(_env("SMTP_HOST")) and bool(_env("SMTP_USER")) and bool(_env("SMTP_PASS")) and bool(_env("COACH_EMAILS"))


# =========================
# LOGGING
# =========================
def _write_log(line: str):
    ts = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    msg = f"[{ts}] {line}"
    print(msg)
    try:
        with open(SCHEDULER_LOG, "a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except Exception:
        pass


def _save_state(data: dict):
    try:
        with open(SCHEDULER_STATE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _load_state() -> dict:
    if not SCHEDULER_STATE.exists():
        return {}
    try:
        return json.loads(SCHEDULER_STATE.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return {}


# =========================
# FILE SNAPSHOTS
# =========================
def _snapshot_files() -> set:
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    return set([p.name for p in GENERATED_DIR.glob("*.txt")])


def _read_file_text(filename: str) -> str:
    try:
        p = (GENERATED_DIR / filename).resolve()
        return p.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        return f"[Could not read {filename}: {e}]"


# =========================
# EMAIL
# =========================
def send_email_for_files(new_files):
    if not new_files:
        return

    if not _email_enabled():
        _write_log("EMAIL: Not configured (missing SMTP_* or COACH_EMAILS). Skipping email.")
        return

    smtp_host = _env("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(_env("SMTP_PORT", "587") or "587")
    smtp_user = _env("SMTP_USER")
    smtp_pass = _env("SMTP_PASS")

    email_from = _env("EMAIL_FROM", smtp_user)
    coach_emails = _parse_emails(_env("COACH_EMAILS"))
    subject_prefix = _env("EMAIL_SUBJECT_PREFIX", "R&B 5-to-1 Slot")
    subject = f"{subject_prefix} — {len(new_files)} new programme(s)"

    body_lines = []
    body_lines.append("R&B Fitness — New 5-to-1 programmes generated")
    body_lines.append(f"Generated at: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    body_lines.append("")
    body_lines.append("Files:")
    for nf in new_files:
        body_lines.append(f"- {nf}")
    body_lines.append("")
    body_lines.append("Programme text(s):")
    body_lines.append("")

    for nf in new_files:
        body_lines.append("=" * 60)
        body_lines.append(nf)
        body_lines.append("=" * 60)
        body_lines.append(_read_file_text(nf).strip())
        body_lines.append("")

    msg = EmailMessage()
    msg["From"] = email_from
    msg["To"] = ", ".join(coach_emails)
    msg["Subject"] = subject
    msg.set_content("\n".join(body_lines))

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=25) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)

        _write_log(f"EMAIL: Sent to {', '.join(coach_emails)} (files: {len(new_files)})")

    except Exception as e:
        _write_log(f"EMAIL: Failed — {e}")


# =========================
# RUNNER
# =========================
def run_v3_once() -> int:
    """
    Runs v3 and returns the exit code.
    Captures output to logs/scheduler.log.
    """
    cmd = [
        "python",
        str(BASE_DIR / "slot_generator_v3.py"),
        "--window-hours",
        str(WINDOW_HOURS),
        "--cache-minutes",
        str(CACHE_MINUTES),
    ]

    _write_log("Running slot_generator_v3...")

    proc = subprocess.run(
        cmd,
        cwd=str(BASE_DIR),
        capture_output=True,
        text=True,
        shell=False,
    )

    out = (proc.stdout or "").strip()
    err = (proc.stderr or "").strip()

    # Log stdout
    if out:
        _write_log("STDOUT:")
        seen_generated_lines = set()
        for line in out.splitlines():
            # avoid duplicate "Generated:" lines in log if repeated
            if line.strip().startswith("Generated:"):
                if line.strip() in seen_generated_lines:
                    continue
                seen_generated_lines.add(line.strip())
            _write_log("  " + line)

    # Log stderr
    if err:
        _write_log("STDERR:")
        for line in err.splitlines():
            _write_log("  " + line)

    return proc.returncode


def main():
    print("========================================")
    print("R&B SLOT GENERATOR AUTO SCHEDULER")
    print("========================================")
    print("Checks bookings every 5 minutes")
    print("Press CTRL+C to stop")
    print("Log file: logs/scheduler.log")
    print("Email to coaches: " + ("Enabled" if _email_enabled() else "Not configured"))
    print("")

    state = _load_state()
    state.setdefault("started_at", datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    _save_state(state)

    while True:
        start_ts = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        _write_log("========================================")
        _write_log(f"CHECK START {start_ts}")
        _write_log("========================================")

        before_files = _snapshot_files()
        code = run_v3_once()
        after_files = _snapshot_files()

        new_files = sorted(list(after_files - before_files))

        # Track last run
        state["last_run"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        state["last_exit_code"] = int(code)
        state["last_new_files_count"] = int(len(new_files))
        state["last_new_files"] = new_files
        _save_state(state)

        if code == 0:
            if new_files:
                _write_log(f"NEW FILES: {len(new_files)}")
                for nf in new_files:
                    _write_log(f"  - {nf}")
                send_email_for_files(new_files)
            else:
                _write_log("No new files detected. No email sent.")
            _write_log("v3 complete [OK]")
        else:
            _write_log(f"v3 complete [ERROR] exit_code={code}")

            # If quota was hit, wait extra
            try:
                last_lines = ""
                if SCHEDULER_LOG.exists():
                    last_lines = SCHEDULER_LOG.read_text(encoding="utf-8", errors="replace")[-3000:].lower()
                if "429" in last_lines and ("quota" in last_lines or "rate" in last_lines):
                    _write_log(f"Quota detected. Extra wait {EXTRA_WAIT_ON_429_SECONDS}s.")
                    time.sleep(EXTRA_WAIT_ON_429_SECONDS)
            except Exception:
                pass

        _write_log(f"Sleeping for {int(CHECK_INTERVAL_SECONDS/60)} minutes...")
        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()