"""tools/privacy_check.py --job J --run DIR [--report PATH, default DIR/J.privacy.md]

Privacy regression checks for the declared outputs (SPEC.md): policy
consistency, declared topics, allowlisted fields, and the coordinate
assertions from privacy/field-map.yaml.
"""

import argparse
import json
import sys
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "tools"))
import contract  # noqa: E402

PRIVACY = REPO / "privacy"
MANIFEST = REPO / "baseline" / "manifest.json"


def read_records(path):
    return [json.loads(line) for line in
            Path(path).read_text(encoding="utf-8").splitlines() if line.strip()]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--job", required=True)
    p.add_argument("--run", required=True)
    p.add_argument("--report", default=None)
    args = p.parse_args()
    job, run_dir = args.job, Path(args.run)
    report = Path(args.report) if args.report else run_dir / f"{job}.privacy.md"

    fields = yaml.safe_load((PRIVACY / "fields.yaml").read_text(encoding="utf-8"))
    allowlist = yaml.safe_load((PRIVACY / "output-allowlist.yaml").read_text(encoding="utf-8"))
    field_map = yaml.safe_load((PRIVACY / "field-map.yaml").read_text(encoding="utf-8"))
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))

    lines = [f"# Privacy check report: {job}",
             "",
             f"Run directory: `{run_dir}` vs `privacy/` policy files.", ""]

    def emit(status, name, counts, diffs=()):
        suffix = "; " + "; ".join(str(d) for d in list(diffs)[:5]) if diffs else ""
        lines.append(f"- {status} — {name}: {counts}{suffix}")

    def not_run(names):
        for n in names:
            emit("NOT RUN", n, "blocked by an earlier FAIL")

    NAMES = ["policy consistency", "topics", "fields", "coordinate assertions"]
    ok = True

    # 1. Policy consistency.
    job_topics = allowlist["jobs"][job]
    problems = []
    review_status = fields["review"]["status"]
    assertions = field_map[job]["assertions"]
    for topic, allowed in job_topics.items():
        classified = fields["topics"].get(topic, {})
        for f in allowed:
            if f not in classified:
                problems.append(f"{topic}.{f}: allowlisted but not classified in fields.yaml")
        for f, cls in classified.items():
            if cls == "location" and not any(a["output"] == f for a in assertions):
                problems.append(f"{topic}.{f}: classified location but has no field-map assertion")
    if problems:
        ok = False
        emit("FAIL", NAMES[0], f"{len(problems)} problems", problems)
    else:
        emit("PASS", NAMES[0],
             f"{sum(len(v) for v in job_topics.values())} allowlisted fields classified; "
             f"review status: {review_status} (pending review is a note, not a failure)")

    # 2. Topics.
    run_meta = json.loads((run_dir / f"{job}.run.json").read_text(encoding="utf-8"))
    topics = [t[:-len(".shadow")] if t.endswith(".shadow") else t
              for t in run_meta["output_topics"]]
    problems = [t for t in topics if t not in job_topics]
    if len(topics) != 1:
        problems.append(f"expected exactly one output topic, got {run_meta['output_topics']} "
                        "(records can't be attributed otherwise)")
    if problems:
        ok = False
        emit("FAIL", NAMES[1], f"{len(run_meta['output_topics'])} declared topics", problems)
        not_run(NAMES[2:])
        finish(lines, report, ok)
        return
    topic = topics[0]
    emit("PASS", NAMES[1], f"{run_meta['output_topics']} -> allowlisted topic {topic}")

    # 3. Fields.
    records = read_records(run_dir / f"{job}.records.jsonl")
    allowed = set(job_topics[topic])
    extra = {}
    for r in records:
        for f in r:
            if f not in allowed:
                extra[f] = extra.get(f, 0) + 1
    if extra:
        ok = False
        emit("FAIL", NAMES[2], f"{len(records)} records, {len(extra)} non-allowlisted fields",
             [f"{f} in {n} records" for f, n in sorted(extra.items())])
    else:
        emit("PASS", NAMES[2], f"{len(records)} records, all fields within the "
                               f"{len(allowed)}-field allowlist")

    # 4. Coordinate assertions.
    if not assertions:
        emit("PASS", NAMES[3], "no location fields to assert")
    else:
        fixture_path = REPO / manifest["fixtures"][job]["path"]
        events, _flush = contract.read_fixture(fixture_path)
        classes, _wm = contract.classify(job, events)
        failures = []
        checked = 0
        negative = 0
        halves = 0
        for a in assertions:
            sources = {}
            for row, cls in zip(events, classes):
                msg = row["message"]
                if cls == "accepted" and msg.get("event") == a["source_event"]:
                    sources[contract.sha256_hex(msg["event_id"])] = msg
            for r in records:
                src = sources.get(r["output_id"])
                if src is None:
                    failures.append(
                        f"{r['output_id']}: no accepted {a['source_event']} source found")
                    continue
                checked += 1
                raw = src[a["source_field"]]
                dec = repr(float(raw)).split(".")[1] if "." in repr(float(raw)) else ""
                if float(raw) < 0:
                    negative += 1
                if len(dec) == 4 and dec[3] == "5":
                    halves += 1
                expected = contract.round3(raw)
                got = r.get(a["output"])
                if not isinstance(got, (int, float)) or isinstance(got, bool) or got != expected:
                    failures.append(f"{r['output_id']} {a['output']}: got {got!r}, "
                                    f"expected {expected!r} (round3 of {raw!r})")
        if failures:
            ok = False
            emit("FAIL", NAMES[3],
                 f"{len(records)} records, {checked} coordinates checked, {len(failures)} failures; "
                 f"{negative} source coordinates negative, {halves} exact halves at the 3rd decimal", failures)
        else:
            emit("PASS", NAMES[3],
                 f"{len(records)} records, {checked} coordinates checked, all {len(assertions)} assertions hold; "
                 f"{negative} source coordinates negative, {halves} exact halves at the 3rd decimal")

    lines += ["", "## What this proves and what it doesn't", "",
              "This proves the run wrote only allowlisted fields to the declared output "
              "topics, and every start coordinate equals the contract rounding of the "
              "opening plug_in's coordinate.", "",
              "It does not prove anonymity (the VIN is still present and 3 decimals is "
              "roughly 100 m), anything about logs or side channels, or anything about "
              "fields the policy doesn't declare."]
    finish(lines, report, ok)


def finish(lines, report, ok):
    text = "\n".join(lines) + "\n"
    Path(report).write_text(text, encoding="utf-8")
    print(text)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
