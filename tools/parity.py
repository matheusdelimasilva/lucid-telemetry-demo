"""tools/parity.py --job J --run DIR [--report PATH, default DIR/J.parity.md]

Compares one replay run directory against the frozen baseline: the manifest
(tree, fixtures, proto, replay env), the drain check, the harness's own
self-checks, and record/counter parity against parity/golden/.
"""

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "tools"))
import manifest as manifest_mod  # noqa: E402

RULES = json.loads((REPO / "tools" / "compare_rules.json").read_text(encoding="utf-8"))
GOLDEN = REPO / "parity" / "golden"


def read_records(path):
    return [json.loads(line) for line in
            Path(path).read_text(encoding="utf-8").splitlines() if line.strip()]


def flatten(obj, prefix=""):
    """Nested dict -> {dot.path: leaf}, for leaf-by-leaf manifest comparison."""
    out = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            out.update(flatten(v, f"{prefix}{k}."))
    else:
        out[prefix[:-1]] = obj
    return out


def dup_ids(records):
    counts = Counter(r["output_id"] for r in records)
    return {k: v for k, v in counts.items() if v > 1}


def values_equal(field, expected, actual, tolerances):
    if type(expected) is not type(actual):
        return False
    if field in tolerances and isinstance(expected, (int, float)):
        return abs(expected - actual) <= tolerances[field]
    return expected == actual


def check_record_parity(golden, actual, tolerances):
    """Returns (counts_text, first-5 differences) for check 5."""
    problems = []
    g_by_id = {r["output_id"]: r for r in golden}
    a_by_id = {r["output_id"]: r for r in actual}
    for rid in sorted(set(g_by_id) - set(a_by_id)):
        problems.append(f"missing record {rid}")
    for rid in sorted(set(a_by_id) - set(g_by_id)):
        problems.append(f"extra record {rid}")
    for rid in sorted(set(g_by_id) & set(a_by_id)):
        g, a = g_by_id[rid], a_by_id[rid]
        if set(g) != set(a):
            problems.append(
                f"record {rid} key set differs: golden {sorted(set(g) - set(a)) or '-'} "
                f"run {sorted(set(a) - set(g)) or '-'}")
            continue
        for f in sorted(g):
            if not values_equal(f, g[f], a[f], tolerances):
                problems.append(f"record {rid} field {f}: expected {g[f]!r} got {a[f]!r}")
    counts = (f"{len(golden)} golden records, {len(actual)} run records, "
              f"{len(set(g_by_id) & set(a_by_id))} common output_ids, {len(problems)} differences")
    return counts, problems


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--job", required=True)
    p.add_argument("--run", required=True)
    p.add_argument("--report", default=None)
    args = p.parse_args()
    job, run_dir = args.job, Path(args.run)
    report = Path(args.report) if args.report else run_dir / f"{job}.parity.md"
    tolerances = RULES.get(job, {}).get("abs_tolerance", {})

    lines = [f"# Parity report: {job}",
             "",
             f"Run directory: `{run_dir}` vs `parity/golden/` + `baseline/manifest.json`.",
             ""]

    def emit(status, name, counts, diffs=()):
        suffix = "; " + "; ".join(str(d) for d in list(diffs)[:5]) if diffs else ""
        lines.append(f"- {status} — {name}: {counts}{suffix}")

    def not_run(names):
        for n in names:
            emit("NOT RUN", n, "blocked by an earlier REFUSED/FAIL")

    NAMES = ["frozen baseline", "drain", "harness self-checks",
             "output integrity", "record parity", "counters"]
    ok = True

    # 1. Frozen baseline.
    expected = json.loads((REPO / "baseline" / "manifest.json").read_text(encoding="utf-8"))
    got = manifest_mod.compute(run_dir)
    diffs = []
    for key in sorted(set(flatten(expected)) | set(flatten(got))):
        e, g = flatten(expected).get(key), flatten(got).get(key)
        if e != g:
            diffs.append(f"{key}: expected {e} vs got {g}")
    run_meta = json.loads((run_dir / f"{job}.run.json").read_text(encoding="utf-8"))
    if run_meta["fixture_sha256"] != expected["fixtures"][job]["sha256"]:
        diffs.append(f"run.json fixture_sha256: expected "
                     f"{expected['fixtures'][job]['sha256']} vs got {run_meta['fixture_sha256']}")
    if diffs:
        emit("REFUSED", NAMES[0], f"{len(diffs)} manifest mismatches", diffs)
        not_run(NAMES[1:])
        finish(lines, report, 1)
        return
    emit("PASS", NAMES[0], "manifest identical, fixture_sha256 matches")

    # 2. Drain.
    drain = json.loads((run_dir / f"{job}.drain.json").read_text(encoding="utf-8"))
    if drain["failures"]:
        emit("REFUSED", NAMES[1], f"{len(drain['failures'])} drain failures", drain["failures"])
        not_run(NAMES[2:])
        finish(lines, report, 1)
        return
    emit("PASS", NAMES[1], "drain failures empty, passed = "
      f"{drain.get('passed')}")

    # 3. Harness self-checks (a failure here counts but does not stop the run).
    result = json.loads((run_dir / f"{job}.result.json").read_text(encoding="utf-8"))
    if result["status"] == "PASS":
        emit("PASS", NAMES[2], "harness result PASS")
    else:
        ok = False
        emit("FAIL", NAMES[2], f"harness result {result['status']}",
             result.get("failures", []))

    # 4. Output integrity: no repeated output_id in run or golden.
    records = read_records(run_dir / f"{job}.records.jsonl")
    golden_records = read_records(GOLDEN / f"{job}.records.jsonl")
    dups = {**{f"run:{k}": v for k, v in dup_ids(records).items()},
            **{f"golden:{k}": v for k, v in dup_ids(golden_records).items()}}
    if dups:
        emit("FAIL", NAMES[3], f"{len(dups)} repeated output_ids",
             [f"{k} x{v}" for k, v in sorted(dups.items())])
        not_run(NAMES[4:])
        finish(lines, report, 1)
        return
    emit("PASS", NAMES[3], f"{len(records)} run records, {len(golden_records)} golden records, "
                           "all output_ids unique")

    # 5. Record parity.
    counts, problems = check_record_parity(golden_records, records, tolerances)
    if problems:
        ok = False
        emit("FAIL", NAMES[4], counts, problems)
    else:
        emit("PASS", NAMES[4], counts)

    # 6. Counters.
    golden_counters = json.loads((GOLDEN / f"{job}.counters.json").read_text(encoding="utf-8"))
    run_counters = json.loads((run_dir / f"{job}.counters.json").read_text(encoding="utf-8"))
    cdiffs = []
    for key in sorted(set(flatten(golden_counters)) | set(flatten(run_counters))):
        e, g = flatten(golden_counters).get(key), flatten(run_counters).get(key)
        if e != g:
            cdiffs.append(f"{key}: golden {e} vs run {g}")
    if cdiffs:
        ok = False
        emit("FAIL", NAMES[5], f"{len(cdiffs)} counter mismatches", cdiffs)
    else:
        emit("PASS", NAMES[5], f"{len(flatten(golden_counters))} counters identical")

    lines += ["", "## Counters side by side", "",
              "| counter | golden (Scala) | this run |", "| --- | --- | --- |"]
    for key in sorted(set(flatten(golden_counters)) | set(flatten(run_counters))):
        lines.append(f"| {key} | {flatten(golden_counters).get(key, '—')} | "
                     f"{flatten(run_counters).get(key, '—')} |")

    lines += ["", "## What this proves and what it doesn't", "",
              "This proves that this run, on the frozen fixtures and the pinned replay "
              "config, produced the same set of final records (keyed by output_id) and "
              "the same counters as the frozen Scala baseline.", "",
              "It does not prove: the correctness of the baseline itself (the acceptance "
              "examples cover that), behaviour on other inputs or real traffic, "
              "exactly-once or crash recovery, ordering across vehicles, or privacy "
              "(privacy-check covers the declared outputs)."]
    finish(lines, report, 0 if ok else 1)


def finish(lines, report, code):
    text = "\n".join(lines) + "\n"
    Path(report).write_text(text, encoding="utf-8")
    print(text)
    sys.exit(code)


if __name__ == "__main__":
    main()
