import sys
from openai import OpenAI


def main():

    if len(sys.argv) < 2:
        print("USAGE: python chatgpt_validator.py <path-to-generated-slot-file>")
        return

    path = sys.argv[1]

    with open(path, "r", encoding="utf-8") as f:
        text = f.read().strip()

    client = OpenAI()

    prompt = f"""
You are a strict validator for R&B Fitness WhatsApp slot output.

Return EXACTLY either:
PASS
or:
FAIL: <short reason>

Hard rules:
- No blank empty lines.
- Uses "—" separators.
- No word "STOP".
- No "+X3" "+X4" "+X5" anywhere.

Rep rules:
- Bilateral strength exercises must use "10 to 12".
- Unilateral strength exercises must use "10L/10R to 12L/12R".

Core hold time rule:
- Plank / Side Plank / Hollow Hold are allowed to be timed.
- ANY sensible time is valid (including 1min).
- DO NOT enforce any time window such as 20–45sec.
- Only FAIL a timed hold if it has NO time unit at all (e.g., "Side Plank 1" with no sec/min).

Unilateral mandatory L/R rule:
- If an exercise name implies unilateral (e.g., "Single Arm", "Single Leg", "Concentration Curl", "Split Squat", "Step Up", "Woodchop", "Landmine Rotation")
  it MUST be written with L/R reps: 10L/10R to 12L/12R
- If a unilateral move appears WITHOUT L/R, FAIL it.

Core requirement (IMPORTANT):
- Each CLIENT block must include at least one core/abs exercise line.
- Do NOT require core in the Rehab (All) block.
- If ANY client block has zero core lines, FAIL it.

TEXT:
{text}
""".strip()

    resp = client.responses.create(
        model="gpt-5",
        input=prompt
    )

    print((resp.output_text or "").strip())


if __name__ == "__main__":
    main()