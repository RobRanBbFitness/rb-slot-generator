import re
from pathlib import Path

FILE = Path("slot_generator_v2.py")

text = FILE.read_text(encoding="utf-8")

# Find main() function block start
m = re.search(r"^def\s+main\s*\(\s*\)\s*:\s*$", text, flags=re.MULTILINE)
if not m:
    raise SystemExit("ERROR: Could not find def main(): in slot_generator_v2.py")

start = m.end()

# Grab from main() start to end of file (we will operate only inside main)
main_text = text[start:]

# Find the first line of main body indentation (usually 4 spaces)
# We'll insert global CLIENTS_SOURCE right after def main(): line if it’s not already there.
lines = main_text.splitlines(True)  # keep line endings

# Determine indentation for main body: first non-empty, non-comment line
indent = "    "
for ln in lines:
    if ln.strip() and not ln.lstrip().startswith("#"):
        indent = re.match(r"^\s*", ln).group(0) or "    "
        break

global_line = f"{indent}global CLIENTS_SOURCE\n"

# If global exists later in main, remove it (we want it only once at the top)
lines_no_global = [ln for ln in lines if ln.strip() != "global CLIENTS_SOURCE"]

# Now check if the first few lines already contain it
head_chunk = "".join(lines_no_global[:20])
if "global CLIENTS_SOURCE" not in head_chunk:
    # Insert global line right at the top of main body
    # After any blank/comment lines at the start of main
    insert_at = 0
    for i, ln in enumerate(lines_no_global):
        if ln.strip() == "" or ln.lstrip().startswith("#"):
            continue
        insert_at = i
        break
    lines_no_global.insert(insert_at, global_line)

fixed_main_text = "".join(lines_no_global)

# Rebuild full file
fixed = text[:start] + fixed_main_text

FILE.write_text(fixed, encoding="utf-8")

print("✅ Patched: moved/inserted 'global CLIENTS_SOURCE' to the top of main() and removed duplicates.")
print("Now run your command again.")