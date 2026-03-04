# rehab_day_map.py
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional, Tuple, List


DATE_FMT = "%d/%m/%y"


def normalise_date_key(date_str: str) -> str:
    """
    Normalise input date into dd/mm/yy (e.g., 27/02/26).
    Raises ValueError if invalid.
    """
    date_str = date_str.strip()
    dt = datetime.strptime(date_str, DATE_FMT)
    return dt.strftime(DATE_FMT)


def _norm(s: str) -> str:
    return " ".join(s.strip().lower().split())


def load_rehab_day_map(path: str) -> Dict[str, Dict[str, str]]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        return {}
    # Ensure shape: { "dd/mm/yy": {"upper": "...", "lower": "..."} }
    cleaned: Dict[str, Dict[str, str]] = {}
    for k, v in data.items():
        if not isinstance(k, str) or not isinstance(v, dict):
            continue
        upper = v.get("upper")
        lower = v.get("lower")
        if isinstance(upper, str) and isinstance(lower, str):
            try:
                kk = normalise_date_key(k)
            except ValueError:
                continue
            cleaned[kk] = {"upper": upper, "lower": lower}
    return cleaned


def save_rehab_day_map(path: str, data: Dict[str, Dict[str, str]]) -> None:
    # atomic write
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


@dataclass(frozen=True)
class RehabSelection:
    upper: str
    lower: str


def pick_or_prompt_rehab_for_date(
    *,
    date_key: str,
    rehab_day_map_path: str,
    allowed_upper_rehab: List[str],
    allowed_lower_rehab: List[str],
    enforce_day_to_day_rotation: bool = True,
) -> RehabSelection:
    """
    Phase 2A day-level rehab locking:
    - If date exists in rehab_day_map.json: use it, do NOT prompt.
    - If new date: prompt once, validate against allowed banks, save.
    - Optional: enforce not repeating the most recent saved day (rotate day-to-day).
    """
    date_key = normalise_date_key(date_key)
    data = load_rehab_day_map(rehab_day_map_path)

    if date_key in data:
        saved = data[date_key]
        return RehabSelection(upper=saved["upper"], lower=saved["lower"])

    # Build canonical lookup for strict matching
    upper_lookup = {_norm(x): x for x in allowed_upper_rehab}
    lower_lookup = {_norm(x): x for x in allowed_lower_rehab}

    def prompt_one(prompt: str, lookup: Dict[str, str]) -> str:
        raw = input(prompt).strip()
        k = _norm(raw)
        if k not in lookup:
            # Strict: reject anything not in bank
            raise ValueError("ERROR: REHAB NOT IN APPROVED BANK (EXACT MATCH REQUIRED)")
        return lookup[k]  # return canonical exact string

    # Optional rotation check (compare against latest known date)
    last_upper: Optional[str] = None
    last_lower: Optional[str] = None
    if enforce_day_to_day_rotation and data:
        latest_date = max(data.keys(), key=lambda d: datetime.strptime(d, DATE_FMT))
        last_upper = data[latest_date]["upper"]
        last_lower = data[latest_date]["lower"]

    while True:
        upper = prompt_one("Upper rehab (required): ", upper_lookup)
        lower = prompt_one("Lower rehab (required): ", lower_lookup)

        if enforce_day_to_day_rotation and last_upper and last_lower:
            if _norm(upper) == _norm(last_upper) and _norm(lower) == _norm(last_lower):
                print("ERROR: REHAB MUST ROTATE DAY-TO-DAY. Pick different upper/lower than last saved day.")
                continue

        data[date_key] = {"upper": upper, "lower": lower}
        save_rehab_day_map(rehab_day_map_path, data)
        return RehabSelection(upper=upper, lower=lower)