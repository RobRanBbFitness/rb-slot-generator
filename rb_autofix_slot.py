import re
import sys

# ============================================================
# R&B FITNESS – SLOT AUTO-FIXER
# - Fix unilateral rep formatting: "10 to 12" -> "10L/10R to 12L/12R"
# - Fix common unilateral keywords (single arm/leg etc.)
# - Clean duplicated time tokens in plank/side plank/hollow hold lines
# ============================================================

# Keywords that indicate the exercise should be unilateral L/R
UNILATERAL_KEYWORDS = [
    # obvious phrases
    "Single Arm",
    "Single-Arm",
    "Single Leg",
    "Single-Leg",
    "Single Arm Cable",
    "Single Arm DB",
    "Single Arm Dumbbell",
    "Single Arm Kettlebell",
    "Single Arm Row",
    "Single Arm Cable Row",
    "Single Arm Chest Press",
    "Single Arm Cable Chest Press",
    "Single Arm Shoulder Press",
    "Single Arm Lat Pull Down",
    "Single Arm Lat Pulldown",
    "Single Arm Curl",
    "1 Arm",
    "1-Arm",
    "One Arm",
    "One-Arm",

    # common unilateral moves you flagged
    "Concentration Curl",
    "Incline Concentration Curl",
    "Cable Concentration Curl",

    # rotational unilateral patterns
    "Woodchop",
    "Wood Chop",
    "Landmine Rotation",
    "Landmine Rotations",
    "Landmine Twist",
    "Cable Woodchop",
    "Cable Wood Chop",

    # other typical L/R movements
    "Split Squat",
    "Incline Split Squat",
    "Step Ups",
    "Step-Ups",
    "Step Up",
    "Step-Up",
    "Control Step Down",
    "Control Step-Down",
    "Lunge",
    "Reverse Lunge",
    "Walking Lunge",
]

# Core holds that can include time; we also clean duplicated time tokens on these lines
TIME_HOLD_KEYWORDS = ["Plank", "Side Plank", "Hollow Hold"]


def _has_lr_reps(line: str) -> bool:
    return bool(re.search(r"\b\d+\s*L/\d+\s*R\b", line, flags=re.IGNORECASE)) or ("10L/10R" in line)


def _looks_like_unilateral(line: str) -> bool:
    low = line.lower()
    return any(k.lower() in low for k in UNILATERAL_KEYWORDS)


def _convert_reps_to_lr(line: str) -> str:
    """
    Convert common bilateral rep strings into LR reps.
    Only applied when line looks unilateral and doesn't already contain LR.
    """
    if _has_lr_reps(line):
        return line

    # Convert common ranges → LR range
    # Keep it simple and consistent with your validator:
    # unilateral strength exercises must use 10L/10R to 12L/12R
    line = re.sub(r"\b10\s*to\s*12\b", "10L/10R to 12L/12R", line, flags=re.IGNORECASE)
    line = re.sub(r"\b8\s*to\s*12\b", "10L/10R to 12L/12R", line, flags=re.IGNORECASE)
    line = re.sub(r"\b12\s*to\s*15\b", "10L/10R to 12L/12R", line, flags=re.IGNORECASE)

    return line


def _cleanup_duplicate_time_tokens(line: str) -> str:
    """
    If a line contains Plank/Side Plank/Hollow Hold, remove repeated time tokens.
    Example: "Side Plank 1min 1min" -> "Side Plank 1min"
    """
    low = line.lower()
    if not any(k.lower() in low for k in TIME_HOLD_KEYWORDS):
        return line

    # Normalise spaces a bit first
    line = re.sub(r"\s+", " ", line).strip()

    # Remove consecutive duplicated time tokens like "1min 1min" or "60sec 60sec"
    # Matches: "<time> <same time> [<same time>...]"
    # time token formats: 10sec, 60sec, 1min, 2mins, 30 seconds, etc.
    time_token = r"(\b\d+\s*(?:sec|secs|second|seconds|min|mins|minute|minutes)\b)"
    line = re.sub(rf"{time_token}(?:\s+\1)+", r"\1", line, flags=re.IGNORECASE)

    return line


def fix_line(line: str) -> str:
    # Clean duplicate time on hold lines
    line = _cleanup_duplicate_time_tokens(line)

    # Fix unilateral L/R reps when needed
    if _looks_like_unilateral(line):
        line = _convert_reps_to_lr(line)

    return line


def main():
    if len(sys.argv) < 2:
        print("USAGE: python rb_autofix_slot.py <path-to-slot-file>")
        return

    path = sys.argv[1]

    with open(path, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()

    fixed_lines = [fix_line(line) for line in lines]

    output_text = "\n".join(fixed_lines).rstrip() + "\n"

    with open(path, "w", encoding="utf-8") as f:
        f.write(output_text)

    print(f"Auto-fix complete: {path}")


if __name__ == "__main__":
    main()