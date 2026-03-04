import re
import textwrap
from pathlib import Path
import shutil

SOURCE_FILE = "slot_generator_v2.py"
OUTPUT_FILE = "slot_generator_v2_PHASE3A_QA.py"
BACKUP_FILE = "slot_generator_v2.py.BACKUP_PHASE3A"

def indent(text: str, spaces: int = 4) -> str:
    pad = " " * spaces
    return "\n".join((pad + ln if ln.strip() else ln) for ln in text.splitlines())

def apply_patches(src: str) -> str:
    # 1) Add CLIENT_OVERRIDES_FILE constant
    if "CLIENT_OVERRIDES_FILE" not in src:
        src = re.sub(
            r'(CONFIG_FILE\s*=\s*"rb_slot_config\.json"\s*\n)',
            r'\1CLIENT_OVERRIDES_FILE = "client_overrides.json"\n',
            src,
            count=1
        )

    # 2) Insert deterministic client override loader right after load_json()
    m = re.search(
        r"def load_json\(path: str, default\):[\s\S]+?except Exception:\n\s+return default\n",
        src
    )
    if not m:
        raise RuntimeError("Could not find load_json() block to insert overrides loader.")

    if "CLIENT OVERRIDES (Phase 3A deterministic)" not in src:
        insert = textwrap.dedent("""\
        # =========================
        # CLIENT OVERRIDES (Phase 3A deterministic)
        # =========================
        def load_client_overrides() -> dict:
            data = load_json(CLIENT_OVERRIDES_FILE, {})
            if not isinstance(data, dict):
                return {}
            out = {}
            for k, v in data.items():
                if not str(k).strip() or not isinstance(v, dict):
                    continue
                out[norm(str(k))] = v
            return out

        def _norm_override_schema(raw: dict) -> dict:
            base = {
                "injuries_override": "",
                "goal_override": "",
                "omit_conditioning": False,
                "force_core_finisher": False,
                "force_no_cardio": False,
                "no_planks": False,
                "no_floor": False,
                "supported_only": False,
                "no_spinal_flexion": False,
                "ban_bike": False,
                "ban_burpees": False,
                "extra_abs": False,
                "force_abs_challenge": False,
                "hard_bans": [],
            }
            if not isinstance(raw, dict):
                return base
            for k in list(base.keys()):
                if k in raw:
                    base[k] = raw[k]
            hb = raw.get("hard_bans", [])
            if isinstance(hb, list):
                base["hard_bans"] = [norm(str(x)) for x in hb if str(x).strip()]
            elif isinstance(hb, str) and hb.strip():
                base["hard_bans"] = [norm(hb)]
            for bk in ["omit_conditioning","force_core_finisher","force_no_cardio","no_planks","no_floor",
                       "supported_only","no_spinal_flexion","ban_bike","ban_burpees","extra_abs","force_abs_challenge"]:
                base[bk] = bool(base.get(bk))
            base["injuries_override"] = safe_str(base.get("injuries_override"))
            base["goal_override"] = safe_str(base.get("goal_override"))
            return base

        _CLIENT_OVERRIDES_RAW = load_client_overrides()
        _CLIENT_OVERRIDES = {k: _norm_override_schema(v) for k, v in _CLIENT_OVERRIDES_RAW.items()}

        def _get_client_override(client_name: str) -> dict:
            n = norm(client_name)
            first = n.split(" ")[0] if n else ""
            if n in _CLIENT_OVERRIDES:
                return _CLIENT_OVERRIDES[n]
            if first in _CLIENT_OVERRIDES:
                return _CLIENT_OVERRIDES[first]
            return _norm_override_schema({})
        """).strip("\n")

        src = src[:m.end()] + "\n\n" + insert + "\n\n" + src[m.end():]

    # 3) Make merged injuries deterministic (use injuries_override if present)
    pat = r"def _merged_injuries_text\(client_name: str, injuries_text: str\):[\s\S]+?return injuries_text or \"\"\n"
    mm = re.search(pat, src)
    if mm:
        new_body = textwrap.dedent("""\
        def _merged_injuries_text(client_name: str, injuries_text: str) -> str:
            n = norm(client_name)
            override_inj = safe_str(_get_client_override(client_name).get("injuries_override", ""))
            legacy_override = CLIENT_INJURY_OVERRIDES_EXACT.get(n, "")
            chosen_override = override_inj or legacy_override

            if chosen_override and injuries_text and injuries_text != "None listed":
                return f"{injuries_text} | OVERRIDE: {chosen_override}"
            if chosen_override:
                return chosen_override
            return injuries_text or ""
        """) + "\n"
        src = src[:mm.start()] + new_body + src[mm.end():]

    # 4) Merge overrides into get_special_flags() (correct indentation)
    pat = r"def get_special_flags\(client_name: str, injuries_text: str\):[\s\S]+?\n\s*return flags\n"
    mm = re.search(pat, src)
    if not mm:
        raise RuntimeError("Could not find get_special_flags() to patch.")
    block = src[mm.start():mm.end()]

    if "Deterministic client overrides" not in block:
        insertion = textwrap.dedent("""\
        # ===== Deterministic client overrides (Phase 3A) =====
        override_flags = _get_client_override(client_name)

        if override_flags.get("force_no_cardio") or override_flags.get("force_core_finisher"):
            flags["force_no_cardio"] = True
            flags["force_core_finisher"] = True
        if override_flags.get("omit_conditioning"):
            flags["omit_conditioning"] = True
        if override_flags.get("no_planks"):
            flags["no_planks"] = True

        flags["ban_bike"] = bool(override_flags.get("ban_bike"))
        flags["ban_burpees"] = bool(override_flags.get("ban_burpees"))
        flags["no_floor"] = bool(override_flags.get("no_floor"))
        flags["supported_only"] = bool(override_flags.get("supported_only"))
        flags["no_spinal_flexion"] = bool(override_flags.get("no_spinal_flexion"))
        flags["extra_abs"] = bool(override_flags.get("extra_abs"))
        flags["force_abs_challenge"] = bool(override_flags.get("force_abs_challenge"))
        flags["hard_bans"] = list(override_flags.get("hard_bans") or [])
        """).rstrip()
        insertion = indent(insertion, 4)

        mret = re.search(r"\n\s*return flags\n", block)
        insert_pos = mm.start() + mret.start()
        src = src[:insert_pos] + "\n" + insertion + src[insert_pos:]

    # 5) Ensure conditioning selection respects override bike/burpee bans
    src = re.sub(
        r"ban_bike = real_name_norm in BIKE_BANNED_CLIENTS_EXACT\s*\n\s*ban_burpees = real_name_norm in BURPEES_BANNED_CLIENTS_EXACT",
        'ban_bike = (real_name_norm in BIKE_BANNED_CLIENTS_EXACT) or bool(flags.get("ban_bike"))\n        ban_burpees = (real_name_norm in BURPEES_BANNED_CLIENTS_EXACT) or bool(flags.get("ban_burpees"))',
        src,
        count=1
    )

    # 6) Add hard-ban validation inside validate_client_block()
    vstart = src.find("def validate_client_block")
    if vstart != -1:
        vend = src.find("def call_claude", vstart)
        vblock = src[vstart:vend]
        if "Client hard-ban keyword detected" not in vblock:
            mm = re.search(r"blob = norm\(\"\\n\"\.join\(lines\)\)\n", vblock)
            if mm:
                inject = textwrap.dedent("""\
                # ===== Phase 3A deterministic bans from overrides =====
                hard_bans = set([norm(x) for x in (flags.get("hard_bans") or []) if str(x).strip()])
                if hard_bans:
                    for hb in hard_bans:
                        if hb and hb in blob:
                            return False, f"Client hard-ban keyword detected: '{hb}'"

                if flags.get("ban_bike") and ("air bike" in blob or "spin bike" in blob or "bike" in blob):
                    return False, "Bike-based conditioning is not allowed for this client."

                if flags.get("ban_burpees") and ("burpee" in blob or "burpees" in blob):
                    return False, "Burpees are not allowed for this client."
                """).rstrip()
                inject = indent(inject, 4) + "\n"
                abs_insert = vstart + mm.end()
                src = src[:abs_insert] + inject + src[abs_insert:]

    # 7) Add QA runner (one-client slot per client) before main()
    if "def qa_all_clients(" not in src:
        qa = textwrap.dedent("""\
        # ============================================================
        # QA MODE (Phase 3A): Stress-test ALL clients
        # Generates 1-client slots and validates outputs. Saves report.
        # ============================================================
        def qa_all_clients(qa_date_ddmmyy: str, qa_focus: str, qa_limit: int = 0):
            _ensure_inputs_exist_or_print_and_exit()
            all_exercises = load_exercise_bank()
            clients_dict = load_clients(os.path.join(os.getcwd(), EXCEL_FILENAME))
            log = load_log()

            qa_time = "12:20pm"
            names = [v["name"] for _, v in clients_dict.items() if v and v.get("name")]
            names = sorted(set(names), key=lambda x: x.lower())

            if qa_limit and qa_limit > 0:
                names = names[:qa_limit]

            os.makedirs(AUTO_OUTPUT_DIR, exist_ok=True)
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_path = os.path.join(AUTO_OUTPUT_DIR, f"QA_REPORT_{stamp}.txt")

            passed = 0
            failed = 0
            report_lines = []

            for nm in names:
                try:
                    generate_slot_output(
                        slot_date_ddmmyy=qa_date_ddmmyy,
                        slot_time_display=qa_time,
                        raw_client_inputs=[nm],
                        slot_focus=qa_focus,
                        all_exercises=all_exercises,
                        clients_dict=clients_dict,
                        log=log
                    )
                    report_lines.append(f"{nm} — PASS")
                    passed += 1
                except Exception as e:
                    report_lines.append(f"{nm} — FAIL — {str(e)}")
                    failed += 1

            with open(report_path, "w", encoding="utf-8") as f:
                f.write("\\n".join(report_lines).strip() + "\\n")

            print(f"QA COMPLETE — Passed: {passed} | Failed: {failed}")
            print(f"Report saved: {report_path}")
        """).strip("\n") + "\n\n"
        src = src.replace("def main():", qa + "def main():", 1)

    # 8) Add QA args to argparse + handler
    main_start = src.find("def main():")
    main_end = src.find("if __name__ == \"__main__\":", main_start)
    mb = src[main_start:main_end]

    if "--qa-all" not in mb:
        mb = re.sub(
            r'(parser\.add_argument\("--window-hours".*?\n)',
            r'\1    parser.add_argument("--qa-all", action="store_true", help="QA: generate + validate 1-client slots for ALL clients and save a PASS/FAIL report.")\n'
            r'    parser.add_argument("--qa-date", type=str, default=datetime.now().strftime("%d/%m/%y"), help="QA date in dd/mm/yy (default today).")\n'
            r'    parser.add_argument("--qa-focus", type=str, default="back and biceps", help="QA focus used for all clients (default: back and biceps).")\n'
            r'    parser.add_argument("--qa-limit", type=int, default=0, help="QA: limit number of clients (0 = all).")\n',
            mb,
            count=1,
            flags=re.DOTALL
        )
        mb = re.sub(
            r'(\n\s*args = parser\.parse_args\(\)\n)',
            r'\1\n    if args.qa_all:\n        qa_all_clients(args.qa_date, args.qa_focus, args.qa_limit)\n        return\n',
            mb,
            count=1
        )
        src = src[:main_start] + mb + src[main_end:]

    return src

def main():
    src_path = Path(SOURCE_FILE)
    if not src_path.exists():
        raise FileNotFoundError(f"Could not find {SOURCE_FILE} in this folder.")

    # Backup
    shutil.copy2(SOURCE_FILE, BACKUP_FILE)
    print(f"Backup created: {BACKUP_FILE}")

    src = src_path.read_text(encoding="utf-8")
    patched = apply_patches(src)

    Path(OUTPUT_FILE).write_text(patched, encoding="utf-8")
    print(f"Patched file written: {OUTPUT_FILE}")
    print("Done ✅")

if __name__ == "__main__":
    main()