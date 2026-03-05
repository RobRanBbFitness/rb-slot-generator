import re
import subprocess
import time
from pathlib import Path
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse, JSONResponse, FileResponse


APP_TITLE = "R&B Slot Generator"
BASE_DIR = Path(__file__).resolve().parent
GENERATED_DIR = BASE_DIR / "generated_slots"

app = FastAPI(title=APP_TITLE)

SAFE_FILENAME_RE = re.compile(r"^[A-Za-z0-9_.\-]+\.txt$")

_LAST_RUN_TS = 0.0
COOLDOWN_SECONDS = 45


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


def _run_v3_capture_output(window_hours: int = 48) -> str:
    """
    Runs slot_generator_v3.py which should:
      - read Sheets ONCE (with backoff)
      - write bookings_stub.json + client_overrides.json locally
      - run slot_generator_v2 in stub mode (no more Sheets reads)
    """
    global _LAST_RUN_TS

    now = time.time()
    if now - _LAST_RUN_TS < COOLDOWN_SECONDS:
        remaining = int(COOLDOWN_SECONDS - (now - _LAST_RUN_TS))
        raise HTTPException(status_code=429, detail=f"Cooldown active. Please wait {remaining}s then try again.")

    script = BASE_DIR / "slot_generator_v3.py"
    if not script.exists():
        raise HTTPException(status_code=500, detail="slot_generator_v3.py not found in the RBSLOT folder.")

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
        if "[429]" in output or "Quota exceeded" in output:
            raise HTTPException(
                status_code=429,
                detail="Google Sheets quota hit (429). Wait 2–3 minutes and click once.\n\n" + output,
            )
        raise HTTPException(status_code=500, detail=output or "Generator failed with unknown error.")

    _LAST_RUN_TS = time.time()
    return output or "SUCCESS: Generator ran, but no output was captured."


@app.get("/", include_in_schema=False)
def home_redirect():
    return RedirectResponse(url="/dashboard")


@app.get("/health")
def health():
    return {"status": "ok", "app": APP_TITLE}


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
              </td>
            </tr>
            """
    else:
        rows = """<tr><td colspan="4" style="opacity:.8;">No files yet. Press “Generate Next 48 Hours”.</td></tr>"""

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
      min-height: 240px;
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
      <div class="sub">One-click generation + latest files. Generates the next 48 hours via v3 (cached + backoff).</div>

      <div class="row">
        <button class="btn" id="runBtn" onclick="runGenerator()">Generate Next 48 Hours</button>
        <a class="btn secondary" href="/docs" target="_blank">Open API Docs</a>
        <span class="pill" id="statusPill">Status: Ready</span>
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
      <b>Run Log</b>
      <div class="sub">This shows the generator output.</div>
      <div id="log" class="mono">Press “Generate Next 48 Hours” to run the system…</div>
    </div>
  </div>

  <script>
    async function runGenerator() {{
      const btn = document.getElementById("runBtn");
      const log = document.getElementById("log");
      const pill = document.getElementById("statusPill");

      btn.disabled = true;
      pill.textContent = "Status: Running…";
      log.textContent = "Running generator (48 hours)…\\n\\n";

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
          pill.textContent = "Status: Complete ✅";
          log.textContent += text;
          setTimeout(() => location.reload(), 800);
        }}
      }} catch (e) {{
        pill.textContent = "Status: Error";
        log.textContent += "\\n" + (e?.message || String(e));
      }} finally {{
        btn.disabled = false;
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