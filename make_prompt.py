from pathlib import Path

BASE = Path(__file__).parent
STYLE_FILE = BASE / "rb_style_pack.md"
OUTPUT_FILE = BASE / "_prompt_for_claude.txt"

def main():
    if not STYLE_FILE.exists():
        print("ERROR: rb_style_pack.md not found.")
        input("Press Enter to exit...")
        return

    style_text = STYLE_FILE.read_text(encoding="utf-8").strip()

    print("Type your full request below.")
    print("When finished, type END on a new line and press Enter.")
    print("")

    user_lines = []
    while True:
        line = input()
        if line.strip().upper() == "END":
            break
        user_lines.append(line)

    user_request = "\n".join(user_lines).strip()

    if not user_request:
        print("No request entered.")
        input("Press Enter to exit...")
        return

    final_prompt = (
        style_text
        + "\n\n"
        + "==============================\n"
        + "USER REQUEST:\n"
        + user_request
        + "\n"
    )

    OUTPUT_FILE.write_text(final_prompt, encoding="utf-8")

    print("\nDONE ✅")
    print(f"Saved here: {OUTPUT_FILE}")
    print("\nNow open that file and paste it into Claude.")
    input("\nPress Enter to close...")

if __name__ == "__main__":
    main()