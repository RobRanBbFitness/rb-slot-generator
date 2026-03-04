import subprocess
import glob
import os
import re

# ============================================================
# R&B FITNESS – DAILY AI PIPELINE
# Runs generator → autofix → validator automatically
# IMPORTANT: Only processes real SLOT files:
#   generated_slots\YYYY-MM-DD_HHMM.txt
# Ignores QA reports and other txt files.
# ============================================================

SLOT_FILENAME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}_\d{4}\.txt$", re.IGNORECASE)


def run_generator():
    print("----")
    print("Generating sessions from Google Sheets...")
    print("----")

    subprocess.run([
        "python",
        "slot_generator_v2.py",
        "--auto",
        "--source", "sheets",
        "--clients-source", "sheets",
        "--window-hours", "9999"
    ])


def find_generated_slot_files():
    files = glob.glob(os.path.join("generated_slots", "*.txt"))
    slot_files = []
    for f in files:
        base = os.path.basename(f)
        if SLOT_FILENAME_RE.match(base):
            slot_files.append(f)
    slot_files.sort()
    return slot_files


def run_autofix(file_path):
    subprocess.run(["python", "rb_autofix_slot.py", file_path])


def run_validator(file_path):
    result = subprocess.run(
        ["python", "chatgpt_validator.py", file_path],
        capture_output=True,
        text=True
    )
    return (result.stdout or "").strip()


def main():
    print("=================================")
    print("R&B FITNESS AI DAILY GENERATOR")
    print("=================================")

    run_generator()

    files = find_generated_slot_files()

    if not files:
        print("No generated SLOT files found in generated_slots (YYYY-MM-DD_HHMM.txt).")
        return

    print("")
    print("Processing generated slots...")
    print("")

    for file in files:
        print("----------------------------")
        print(f"Processing: {file}")
        print("----------------------------")

        print("Running auto-fix...")
        run_autofix(file)

        print("Running validator...")
        result = run_validator(file)

        print("Validator result:", result)

        if "PASS" in result:
            print("Program approved.")
        else:
            print("Program failed validation.")
            print("Review recommended.")

    print("")
    print("=================================")
    print("DAILY PIPELINE COMPLETE")
    print("=================================")


if __name__ == "__main__":
    main()