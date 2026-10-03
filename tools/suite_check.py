"""tools/suite_check.py --job J --run DIR [--suite parity/examples --suite parity/probes]
                        [--report PATH, default DIR/J.suite.md]

Independent grading of a suite-mode replay (the 16 acceptance examples and the
probes). For every case under the given suites whose `expected.json` names the
job, reads `DIR/<suite>/<case>/outputs.json` (+ `trace.jsonl` when expected.json
states watermarks) and compares it with that case's `expected.json` using the
same rules as tools/parity.py: records keyed by `output_id` with
tools/compare_rules.json tolerances, counters leaf by leaf, the watermark after
each batch expected.json states, and any repeated `output_id` fails the case.
Prints one PASS/FAIL line per case and fails unless the number of cases checked
equals the number of cases that name the job.

The job binary's own verdict (result.json) is not read: a job must not grade
its own examples. Both engines' suite modes write the same files
(stream-rs/jobs/README.md).
"""

import argparse
import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "tools"))
import parity  # noqa: E402  (values_equal, check_record_parity, dup_ids, flatten, RULES)

DEFAULT_SUITES = ("parity/examples", "parity/probes")
CASE_DIR = re.compile(r"\d\d-.*")


def case_dirs(suite):
    return sorted(p for p in Path(suite).iterdir() if p.is_dir() and CASE_DIR.fullmatch(p.name))


def check_case(case_id, expected, out_dir, tolerances):
    """Returns the list of problems for one case (empty = PASS)."""
    outputs_path = out_dir / "outputs.json"
    if not outputs_path.is_file():
        return [f"no {outputs_path} (the job did not run this case)"]
    outputs = json.loads(outputs_path.read_text(encoding="utf-8"))
    problems = []
    if outputs.get("job") != expected["job"]:
        problems.append(f"outputs.json job {outputs.get('job')!r} != expected {expected['job']!r}")

    records = outputs.get("records")
    if not isinstance(records, list):
        return problems + ["outputs.json has no records list"]
    for rid, n in sorted(parity.dup_ids(records).items()):
        problems.append(f"output_id {rid} emitted {n} times")
    _, record_problems = parity.check_record_parity(expected["records"], records, tolerances)
    problems += [p.replace("golden", "expected") for p in record_problems]

    exp_counters, got_counters = parity.flatten(expected["counters"]), parity.flatten(outputs.get("counters") or {})
    for key in sorted(set(exp_counters) | set(got_counters)):
        if exp_counters.get(key) != got_counters.get(key):
            problems.append(f"counter {key}: expected {exp_counters.get(key)} got {got_counters.get(key)}")

    if expected.get("watermarks"):
        trace_path = out_dir / "trace.jsonl"
        trace = parity.read_records(trace_path) if trace_path.is_file() else None
        if trace is None:
            problems.append(f"expected.json states watermarks but there is no {trace_path}")
        else:
            after = {int(line["batch"]): line.get("watermark_after") for line in trace}
            for batch, wm in expected["watermarks"].items():
                got = after.get(int(batch))
                if got != wm:
                    problems.append(f"watermark after batch {batch}: expected {wm} got "
                                    f"{'none' if got is None else got}")
    return problems


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--job", required=True)
    p.add_argument("--run", required=True, help="suite-mode output dir (<run>/<suite>/<case>/outputs.json)")
    p.add_argument("--suite", action="append", default=None,
                   help=f"suite dir(s) holding <nn>-<slug>/expected.json; default {' '.join(DEFAULT_SUITES)}")
    p.add_argument("--report", default=None)
    args = p.parse_args()
    job, run_dir = args.job, Path(args.run)
    suites = [REPO / s if not Path(s).is_absolute() else Path(s) for s in (args.suite or DEFAULT_SUITES)]
    report = Path(args.report) if args.report else run_dir / f"{job}.suite.md"
    tolerances = parity.RULES.get(job, {}).get("abs_tolerance", {})

    expected_cases = []
    for suite in suites:
        for case in case_dirs(suite):
            expected = json.loads((case / "expected.json").read_text(encoding="utf-8"))
            if expected["job"] == job:
                expected_cases.append((f"{suite.name}/{case.name}", expected))

    lines = [f"# Suite report: {job}", "",
             f"Run directory: `{run_dir}` vs `expected.json` under "
             + ", ".join(f"`{s.relative_to(REPO) if s.is_relative_to(REPO) else s}`" for s in suites)
             + f" (tolerances: {tolerances or 'none'}).", ""]
    failed = 0
    for case_id, expected in expected_cases:
        problems = check_case(case_id, expected, run_dir / case_id, tolerances)
        status = "FAIL" if problems else "PASS"
        failed += bool(problems)
        detail = (" -- " + "; ".join(problems[:5])
                  + (f"; ... {len(problems) - 5} more" if len(problems) > 5 else "")) if problems else ""
        line = f"{status} {case_id}{detail}"
        print(line)
        lines.append(f"- {line}")

    checked = len(expected_cases)
    want = sum(1 for suite in suites for case in case_dirs(suite)
               if json.loads((case / "expected.json").read_text(encoding="utf-8"))["job"] == job)
    summary = f"{checked - failed}/{checked} cases passed for {job} ({want} cases name it)"
    ok = failed == 0 and checked == want and checked > 0
    if checked == 0:
        summary += "; FAIL: no cases name this job"
    print(("PASS " if ok else "FAIL ") + summary)
    lines += ["", ("PASS " if ok else "FAIL ") + summary, "",
              "Graded by tools/suite_check.py from outputs.json and trace.jsonl; the job's own "
              "result.json verdict is informational only."]
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
