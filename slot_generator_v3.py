# slot_generator_v3.py
import argparse
import os
import subprocess
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GENERATED_DIR = os.path.join(BASE_DIR, "generated_slots")

def _run(cmd: list[str]) -> tuple[int, str]:
    """Run a command and return (exit_code, combined_output)."""
    p = subprocess.run(cmd, cwd=BASE_DIR, capture_output=True, text=True, shell=False)
    out = (p.stdout or "") + ("\n" + p.stderr if p.stderr else "")
    return p.returncode, out.strip()

def generate_window(window_hours: int = 48, source: str = "sheets", clients_source: str = "sheets") -> dict:
    """
    Generate slots for the next X hours.
    Uses your existing slot_generator_v2.py auto mode.
    Returns dict with status + log text.
    """
    os.makedirs(GENERATED_DIR, exist_ok=True)

    cmd = [
        "python",
        "slot_generator_v2.py",
        "--auto",
        "--source", source,
        "--clients-source", clients_source,
        "--window-hours", str(window_hours),
    ]

    code, log = _run(cmd)
    return {
        "ok": code == 0,
        "exit_code": code,
        "window_hours": window_hours,
        "source": source,
        "clients_source": clients_source,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "log": log,
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--window-hours", type=int, default=48)
    ap.add_argument("--source", type=str, default="sheets", choices=["stub", "sheets"])
    ap.add_argument("--clients-source", type=str, default="sheets", choices=["json", "sheets"])
    ap.add_argument("--logfile", type=str, default="")
    args = ap.parse_args()

    result = generate_window(args.window_hours, args.source, args.clients_source)
    text = (
        f"R&B SLOT GENERATOR V3\n"
        f"Time: {result['timestamp']}\n"
        f"Window: {result['window_hours']} hours\n"
        f"Source: {result['source']} | Clients: {result['clients_source']}\n"
        f"Exit: {result['exit_code']}\n\n"
        f"{result['log']}\n"
    )

    print(text)

    if args.logfile:
        with open(args.logfile, "a", encoding="utf-8") as f:
            f.write(text + "\n" + ("=" * 60) + "\n")

    raise SystemExit(0 if result["ok"] else 1)

if __name__ == "__main__":
    main()