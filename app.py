import os
import re
import smtplib
import subprocess
import time
import json
from pathlib import Path
from datetime import datetime
from email.message import EmailMessage
from typing import List, Tuple

from dotenv import load_dotenv

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse, JSONResponse, FileResponse


# Load .env variables (SMTP settings, coach emails, etc.)
load_dotenv()

APP_TITLE = "R&B Slot Generator"
BASE_DIR = Path(__file__).resolve().parent
GENERATED_DIR = BASE_DIR / "generated_slots"

# Persisted run output so it doesn't disappear after refresh
LAST_RUN_OUTPUT_FILE = BASE_DIR / "last_run_output.txt"
LAST_RUN_META_FILE = BASE_DIR / "last_run.json"

app = FastAPI(title=APP_TITLE)

SAFE_FILENAME_RE = re.compile(r"^[A-Za-z0-9_.\-]+\.txt$")

_LAST_RUN_TS = 0.0
COOLDOWN_SECONDS = 45


# =========================
# EMAIL SETTINGS (ENV VARS)
# =========================
# Put these in your .env (ignored by Git)
#
# SMTP_HOST=smtp.gmail.com
# SMTP_PORT=587
# SMTP_USER=randbfitnessptcentre@gmail.com
# SMTP_PASS=YOUR_APP_PASSWORD
#
# EMAIL_FROM=R&B Slot Generator <randbfitnessptcentre@gmail.com>
# COACH_EMAILS=coach1@email.com,coach2@email.com
#
# EMAIL_SUBJECT_PREFIX=R&B 5-to-1 Slot
#
def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _parse_emails(csv: str) -> List[str]:
    out = []
    for part in (csv or "").split(","):
        p = part.strip()
        if p:
            out.append(p)
    return out


def _email_enabled() -> bool:
    return bool(_env("SMTP_HOST")) and bool(_env("SMTP_USER")) and bool(_env("SMTP_PASS")) and bool(_env("COACH_EMAILS"))


def _send_email_to_coaches(subject: str, body: str) -> Tuple[bool, str]:
    """
    Returns (success, message)
    """
    if not _email_enabled():
        return False, "Email not configured (missing SMTP_* or COACH_EMAILS env vars)."

    smtp_host = _env("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(_env("SMTP_PORT", "587") or "587")
    smtp_user = _env("SMTP_USER")
    smtp_pass = _env("SMTP_PASS")

    email_from = _env("EMAIL_FROM", smtp_user)
    coach_emails = _parse_emails(_env("COACH_EMAILS"))
    if not coach_emails:
        return False, "COACH_EMAILS is empty."

    msg = EmailMessage()
    msg["From"] = email_from
    msg["To"] = ", ".join(coach_emails)
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=25) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        return True, f"Email sent to: {', '.join(coach_emails)}"
    except Exception as e:
        return False, f"Email failed: {e}"


# =========================
# FILE HELPERS
# =========================
def _ensure_generated_dir() -> None:
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)


def _list_generated_files(limit: int = 20):
    _ensure_generated_dir()
    files = sorted(GENERATED_DIR.glob("*.txt"), key=lambda p: p.stat().st_mtime, reverse=True)
    out = []
    for p in files[:limit]:
        out.append(
            {
                "name": p.name,
                "path": str(p),
                "modified": datetime.fromtimestamp(p.stat().st_mtime).strftime("%d/%m/%y %H:%M"),
                "size_kb": round(p.stat().st_size / 1024, 1),
            }
        )
    return out


def _safe_file_path(filename: str) -> Path:
    if not SAFE_FILENAME_RE.match(filename):
        raise HTTPException(status_code=400, detail="Invalid filename.")
    p = (GENERATED_DIR / filename).resolve()
    if GENERATED_DIR not in p.parents:
        raise HTTPException(status_code=400, detail="Invalid path.")
    if not p.exists():
        raise HTTPException(status_code=404, detail="File not found.")
    return p


def _snapshot_files() -> set:
    _ensure_generated_dir()
    return set([p.name for p in GENERATED_DIR.glob("*.txt")])


def _write_last_run(output_text: str, ok: bool, new_files: List[str]):
    try:
        LAST_RUN_OUTPUT_FILE.write_text(output_text or "", encoding="utf-8", errors="replace")
    except Exception:
        pass

    meta = {
        "time": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "ok": bool(ok),
        "new_files_count": int(len(new_files)),
        "new_files": list(new_files),
    }
    try:
        LAST_RUN_META_FILE.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def _read_last_run_output() -> str:
    if not LAST_RUN_OUTPUT_FILE.exists():
        return "Press “Generate Next 48 Hours” to run the system…"
    try:
        return LAST_RUN_OUTPUT_FILE.read_text(encoding="utf-8", errors="replace") or "Press “Generate Next 48 Hours” to run the system…"
    except Exception:
        return "Press “Generate Next 48 Hours” to run the system…"


def _read_last_run_meta():
    if not LAST_RUN_META_FILE.exists():
        return None
    try:
        return json.loads(LAST_RUN_META_FILE.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return None


def _escape_html(s: str) -> str:
    # Safe for embedding in HTML without breaking the page
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


# =========================
# RUN GENERATOR
# =========================
def _run_v3_capture_output(window_hours: int = 48) -> str:
    """
    Runs slot_generator_v3.py which:
      - reads Sheets ONCE (with backoff + cache)
      - writes bookings_stub.json + client_overrides.json locally
      - runs slot_generator_v2 in stub mode
      - skips already-generated sessions (runs_log.json)

    Also:
      - detects newly created output files and emails them to coaches (optional)
      - persists the last run output so it won't disappear
    """
    global _LAST_RUN_TS

    now = time.time()
    if now - _LAST_RUN_TS < COOLDOWN_SECONDS:
        remaining = int(COOLDOWN_SECONDS - (now - _LAST_RUN_TS))
        raise HTTPException(status_code=429, detail=f"Cooldown active. Please wait {remaining}s then try again.")

    script = BASE_DIR / "slot_generator_v3.py"
    if not script.exists():
        raise HTTPException(status_code=500, detail="slot_generator_v3.py not found in the RBSLOT folder.")

    before_files = _snapshot_files()

    cmd = [
        "python",
        str(script),
        "--window-hours",
        str(window_hours),
        "--cache-minutes",
        "2",
    ]

    proc = subprocess.run(
        cmd,
        cwd=str(BASE_DIR),
        capture_output=True,
        text=True,
        shell=False,
    )

    output = ""
    if proc.stdout:
        output += proc.stdout
    if proc.stderr:
        output += "\n\n--- STDERR ---\n" + proc.stderr

    if proc.returncode != 0:
        if "429" in output and ("quota" in output.lower() or "rate" in output.lower()):
            _write_last_run(output_text=output, ok=False, new_files=[])
            raise HTTPException(
                status_code=429,
                detail="Google Sheets quota hit (429). Wait 2–3 minutes and click once.\n\n" + output,
            )

        _write_last_run(output_text=output, ok=False, new_files=[])
        raise HTTPException(status_code=500, detail=output or "Generator failed with unknown error.")

    after_files = _snapshot_files()
    new_files = sorted(list(after_files - before_files))

    # Optional: email new files to coaches
    email_note = ""
    if new_files:
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
            try:
                p = (GENERATED_DIR / nf).resolve()
                txt = p.read_text(encoding="utf-8", errors="replace")
            except Exception as e:
                txt = f"[Could not read {nf}: {e}]"

            body_lines.append("=" * 60)
            body_lines.append(nf)
            body_lines.append("=" * 60)
            body_lines.append(txt.strip())
            body_lines.append("")

        ok, msg = _send_email_to_coaches(subject=subject, body="\n".join(body_lines))
        email_note = f"\n\nEMAIL: {msg}" if msg else ""
    else:
        email_note = "\n\nEMAIL: No new files, nothing sent."

    _LAST_RUN_TS = time.time()

    summary = "\n\n--- SUMMARY ---\n"
    summary += f"New files: {len(new_files)}\n"
    if new_files:
        summary += "Created:\n" + "\n".join([f"  - {f}" for f in new_files]) + "\n"
    summary += email_note + "\n"

    final_out = (output or "SUCCESS: Generator ran, but no output was captured.") + summary
    _write_last_run(output_text=final_out, ok=True, new_files=new_files)
    return final_out


# =========================
# ROUTES
# =========================
@app.get("/", include_in_schema=False)
def home_redirect():
    return RedirectResponse(url="/dashboard")


@app.get("/health")
def health():
    return {"status": "ok", "app": APP_TITLE}


@app.get("/api/last-run", response_class=JSONResponse)
def api_last_run():
    meta = _read_last_run_meta() or {}
    return {"meta": meta, "output": _read_last_run_output()}


@app.post("/generate-48h", response_class=PlainTextResponse)
def generate_48h():
    return _run_v3_capture_output(window_hours=48)


@app.post("/generate-window/{hours}", response_class=PlainTextResponse)
def generate_window(hours: int):
    if hours < 1 or hours > 168:
        raise HTTPException(status_code=400, detail="Hours must be between 1 and 168.")
    return _run_v3_capture_output(window_hours=hours)


@app.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
def dashboard():
    files = _list_generated_files(limit=30)

    rows = ""
    if files:
        for f in files:
            rows += f"""
            <tr>
              <td class="mono">{f["name"]}</td>
              <td>{f["modified"]}</td>
              <td>{f["size_kb"]}kb</td>
              <td class="actions">
                <a class="btn small" href="/view/{f["name"]}" target="_blank">View</a>
                <a class="btn small" href="/download/{f["name"]}">Download</a>
                <button class="btn small secondary" onclick="copyFile('{f["name"]}')">Copy</button>
              </td>
            </tr>
            """
    else:
        rows = """<tr><td colspan="4" style="opacity:.8;">No files yet. Press “Generate Next 48 Hours”.</td></tr>"""

    email_status = "Enabled" if _email_enabled() else "Not configured"
    last_meta = _read_last_run_meta() or {}
    last_run_time = last_meta.get("time", "—")
    last_ok = last_meta.get("ok", None)
    last_files_count = last_meta.get("new_files_count", 0)

    if last_ok is True:
        last_status = f"OK ({last_files_count} new)"
    elif last_ok is False:
        last_status = "ERROR"
    else:
        last_status = "—"

    last_output = _escape_html(_read_last_run_output())

    html = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>R&B Fitness — Slot Dashboard</title>
  <style>
    body {{
      font-family: Arial, sans-serif;
      background: #0f1115;
      color: #e7e7e7;
      margin: 0;
      padding: 22px;
    }}
    .wrap {{
      max-width: 1100px;
      margin: 0 auto;
    }}
    .card {{
      background: #151a22;
      border: 1px solid #242b38;
      border-radius: 14px;
      padding: 18px;
      margin-bottom: 16px;
    }}
    h1 {{
      margin: 0 0 8px 0;
      font-size: 26px;
      letter-spacing: .2px;
    }}
    .sub {{
      opacity: .85;
      margin-bottom: 14px;
    }}
    .row {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
    }}
    .btn {{
      background: #e10600;
      color: white;
      border: none;
      padding: 10px 14px;
      border-radius: 10px;
      cursor: pointer;
      font-weight: 700;
      text-decoration: none;
      display: inline-block;
    }}
    .btn.secondary {{
      background: #2a3344;
    }}
    .btn.small {{
      padding: 7px 10px;
      border-radius: 9px;
      font-weight: 700;
    }}
    .btn:disabled {{
      opacity: .55;
      cursor: not-allowed;
    }}
    .mono {{
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      font-size: 13px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 10px;
    }}
    th, td {{
      border-bottom: 1px solid #242b38;
      padding: 10px 8px;
      text-align: left;
      vertical-align: top;
    }}
    th {{
      opacity: .85;
      font-size: 13px;
    }}
    .actions {{
      white-space: nowrap;
    }}
    #log {{
      width: 100%;
      min-height: 260px;
      background: #0b0e13;
      border: 1px solid #242b38;
      border-radius: 12px;
      padding: 12px;
      overflow: auto;
      white-space: pre-wrap;
    }}
    .pill {{
      display: inline-block;
      padding: 4px 10px;
      border-radius: 999px;
      background: #222a37;
      border: 1px solid #2e384a;
      font-size: 12px;
      opacity: .9;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>R&B Fitness — Slot Generator Dashboard</h1>
      <div class="sub">
        One-click generation + latest files. Email to coaches: <b>{email_status}</b><br/>
        Last run: <b>{last_run_time}</b> | Result: <b>{last_status}</b>
      </div>

      <div class="row">
        <button class="btn" id="runBtn" onclick="runGenerator()">Generate Next 48 Hours</button>
        <a class="btn secondary" href="/docs" target="_blank">Open API Docs</a>
        <span class="pill" id="statusPill">Status: Ready</span>
        <span class="pill" id="copyPill" style="display:none;">Copied</span>
      </div>
    </div>

    <div class="card">
      <div class="row" style="justify-content: space-between;">
        <div><b>Latest Generated Slots</b></div>
        <button class="btn secondary" onclick="location.reload()">Refresh List</button>
      </div>

      <table>
        <thead>
          <tr>
            <th>File</th>
            <th>Modified</th>
            <th>Size</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {rows}
        </tbody>
      </table>
    </div>

    <div class="card">
      <b>Run Log (saved)</b>
      <div class="sub">This persists the last run output so you can read/copy it even after refresh.</div>
      <div id="log" class="mono">{last_output}</div>
    </div>
  </div>

  <script>
    async function runGenerator() {{
      const btn = document.getElementById("runBtn");
      const log = document.getElementById("log");
      const pill = document.getElementById("statusPill");

      btn.disabled = true;
      pill.textContent = "Status: Running...";
      log.textContent = "Running generator (48 hours)...\\n\\n";

      try {{
        const res = await fetch("/generate-48h", {{
          method: "POST",
          headers: {{
            "accept": "text/plain"
          }}
        }});

        const text = await res.text();

        if (!res.ok) {{
          pill.textContent = "Status: Error";
          log.textContent += text || ("Error: " + res.status);
        }} else {{
          pill.textContent = "Status: Complete";
          log.textContent += text;
          // reload so the file list updates, but the log is saved now so it won't disappear
          setTimeout(() => location.reload(), 800);
        }}
      }} catch (e) {{
        pill.textContent = "Status: Error";
        log.textContent += "\\n" + (e?.message || String(e));
      }} finally {{
        btn.disabled = false;
      }}
    }}

    async function copyFile(filename) {{
      const pill = document.getElementById("copyPill");
      pill.style.display = "none";

      try {{
        const res = await fetch("/view/" + filename);
        const text = await res.text();
        await navigator.clipboard.writeText(text);

        pill.style.display = "inline-block";
        setTimeout(() => {{
          pill.style.display = "none";
        }}, 1200);

      }} catch (e) {{
        alert("Copy failed: " + (e?.message || String(e)));
      }}
    }}
  </script>
</body>
</html>
"""
    return html


@app.get("/api/latest", response_class=JSONResponse)
def api_latest():
    return {"files": _list_generated_files(limit=50)}


@app.get("/view/{filename}", response_class=PlainTextResponse, include_in_schema=False)
def view_file(filename: str):
    _ensure_generated_dir()
    p = _safe_file_path(filename)
    return p.read_text(encoding="utf-8", errors="replace")


@app.get("/download/{filename}", include_in_schema=False)
def download_file(filename: str):
    _ensure_generated_dir()
    p = _safe_file_path(filename)
    return FileResponse(path=str(p), filename=p.name, media_type="text/plain")