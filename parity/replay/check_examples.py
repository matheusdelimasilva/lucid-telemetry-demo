"""Validate parity/examples/*/input.jsonl + expected.json against FORMAT.md.

Structural and encoding checks only; no business semantics. Exits non-zero
with a clear message on the first failure, prints one OK line per example.
"""

import json
import re
import sys
from pathlib import Path

from google.protobuf import json_format
from google.protobuf.descriptor import FieldDescriptor

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "generator"))

import battery_health_v1_pb2  # noqa: E402
import charging_sessions_v1_pb2  # noqa: E402
import vehicle_battery_v1_pb2  # noqa: E402
import vehicle_charging_v1_pb2  # noqa: E402

EXAMPLES_DIR = REPO_ROOT / "parity" / "examples"
PROBES_DIR = REPO_ROOT / "parity" / "probes"

INPUT_TYPES = {
    "charging-sessions": vehicle_charging_v1_pb2.ChargingEvent,
    "battery-health": vehicle_battery_v1_pb2.BatteryReading,
}
OUTPUT_TYPES = {
    "charging-sessions": charging_sessions_v1_pb2.ChargingSession,
    "battery-health": battery_health_v1_pb2.BatteryWindow,
}
COUNTER_KEYS = {
    "charging-sessions": {
        "rejected",
        "late",
        "duplicate_events",
        "conflicting_duplicates",
        "orphan",
        "sessions_by_close_reason",
    },
    "battery-health": {
        "rejected",
        "late",
        "duplicate_events",
        "conflicting_duplicates",
    },
}
CLOSE_REASONS = {"unplug", "inactivity_timeout", "replaced_by_plug_in"}
VIN_RE = re.compile(r"^TST[A-Z0-9]{14}$")
OUTPUT_ID_RE = re.compile(r"^[0-9a-f]{64}$")


class CheckError(Exception):
    pass


def check_message(obj, msg_type, where):
    desc = msg_type.DESCRIPTOR
    for key, value in obj.items():
        if value is None:
            raise CheckError(f"{where}: key '{key}' maps to null")
        field = desc.fields_by_name.get(key)
        if field is None:
            raise CheckError(
                f"{where}: '{key}' is not a proto field name of {desc.full_name} "
                "(unknown key or lowerCamelCase)"
            )
        if field.type == FieldDescriptor.TYPE_ENUM and not isinstance(value, str):
            raise CheckError(f"{where}: enum field '{key}' must be a string")
        if field.type == FieldDescriptor.TYPE_INT64 and (
            not isinstance(value, int) or isinstance(value, bool)
        ):
            raise CheckError(f"{where}: int64 field '{key}' must be a JSON int")
    try:
        json_format.ParseDict(obj, msg_type(), ignore_unknown_fields=False)
    except Exception as e:
        raise CheckError(f"{where}: ParseDict failed: {e}") from e


def non_neg_ints(obj, where):
    for key, value in obj.items():
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise CheckError(f"{where}: counter '{key}' must be a non-negative int")


def check_example(example_dir):
    name = example_dir.name
    input_path = example_dir / "input.jsonl"
    expected_path = example_dir / "expected.json"
    for p in (input_path, expected_path):
        if not p.is_file():
            raise CheckError(f"{name}: missing {p.name}")

    expected = json.loads(expected_path.read_text())
    if not isinstance(expected, dict):
        raise CheckError(f"{name}: expected.json is not an object")
    if not set(expected) <= {"job", "records", "counters", "watermarks"}:
        raise CheckError(f"{name}: unexpected keys in expected.json: {sorted(expected)}")
    if not {"job", "records", "counters"} <= set(expected):
        raise CheckError(f"{name}: expected.json needs job, records, counters")
    job = expected["job"]
    if job not in INPUT_TYPES:
        raise CheckError(f"{name}: job must be one of {sorted(INPUT_TYPES)}, got {job!r}")

    input_type = INPUT_TYPES[job]
    output_type = OUTPUT_TYPES[job]

    raw = input_path.read_text()
    lines = raw.splitlines()
    if not lines:
        raise CheckError(f"{name}: input.jsonl is empty")
    if any(line.strip() == "" for line in lines):
        raise CheckError(f"{name}: blank line in input.jsonl")

    events = []
    for i, line in enumerate(lines[:-1]):
        obj = json.loads(line)
        if not isinstance(obj, dict):
            raise CheckError(f"{name}: line {i + 1} is not a JSON object")
        if set(obj) != {"arrival_seq", "batch", "message"}:
            raise CheckError(
                f"{name}: line {i + 1} must be an event line with keys "
                "{arrival_seq, batch, message}"
            )
        events.append(obj)

    flush = json.loads(lines[-1])
    if not isinstance(flush, dict) or set(flush) != {"advance_watermark_to", "batch"}:
        raise CheckError(f"{name}: last line must be the flush line")
    if not events:
        raise CheckError(f"{name}: no event lines")

    ts_seen = set()
    last_batch = 0
    input_vins = set()
    input_batches = set()
    for i, ev in enumerate(events):
        if ev["arrival_seq"] != i + 1:
            raise CheckError(f"{name}: arrival_seq {ev['arrival_seq']} != {i + 1}")
        batch = ev["batch"]
        if not isinstance(batch, int) or batch < 1:
            raise CheckError(f"{name}: line {i + 1} batch must be an int >= 1")
        if batch < last_batch or batch > last_batch + 1:
            raise CheckError(
                f"{name}: batch must be non-decreasing and grow by at most 1 "
                f"(line {i + 1}: {last_batch} -> {batch})"
            )
        last_batch = batch
        input_batches.add(batch)

        message = ev["message"]
        if not isinstance(message, dict):
            raise CheckError(f"{name}: line {i + 1} message is not an object")
        check_message(message, input_type, f"{name} line {i + 1} message")
        vin = message.get("vin", "")
        if not VIN_RE.match(vin):
            raise CheckError(f"{name}: line {i + 1} vin {vin!r} is not TST + 14 [A-Z0-9]")
        input_vins.add(vin)
        ts = message.get("ts")
        if isinstance(ts, int) and not isinstance(ts, bool):
            ts_seen.add(ts)

    if events[0]["batch"] != 1:
        raise CheckError(f"{name}: first event batch must be 1")
    if flush["batch"] != last_batch + 1:
        raise CheckError(
            f"{name}: flush batch {flush['batch']} != last event batch + 1 ({last_batch + 1})"
        )
    if not ts_seen:
        raise CheckError(f"{name}: no int64 ts found in any event")
    # T = largest *valid* ts + 1 h; validity is the job's business, so only check
    # that T - 1 h is some event's ts.
    if flush["advance_watermark_to"] - 3_600_000 not in ts_seen:
        raise CheckError(
            f"{name}: advance_watermark_to {flush['advance_watermark_to']} - 3_600_000 "
            "is not the ts of any event line"
        )

    records = expected["records"]
    if not isinstance(records, list):
        raise CheckError(f"{name}: records must be a list")
    seen_ids = set()
    for j, rec in enumerate(records):
        if not isinstance(rec, dict):
            raise CheckError(f"{name}: record {j} is not an object")
        check_message(rec, output_type, f"{name} record {j}")
        desc = output_type.DESCRIPTOR
        missing = [f.name for f in desc.fields if f.name not in rec]
        if missing:
            raise CheckError(f"{name}: record {j} missing fields {missing}")
        oid = rec.get("output_id", "")
        if not OUTPUT_ID_RE.match(oid):
            raise CheckError(f"{name}: record {j} output_id must be 64 lowercase hex")
        if oid in seen_ids:
            raise CheckError(f"{name}: duplicate output_id {oid}")
        seen_ids.add(oid)
        if rec.get("vin") not in input_vins:
            raise CheckError(
                f"{name}: record {j} vin {rec.get('vin')!r} not in input vins"
            )

    counters = expected["counters"]
    if not isinstance(counters, dict) or set(counters) != COUNTER_KEYS[job]:
        raise CheckError(
            f"{name}: counters keys must be {sorted(COUNTER_KEYS[job])}, "
            f"got {sorted(counters) if isinstance(counters, dict) else counters!r}"
        )
    for key in COUNTER_KEYS[job]:
        value = counters[key]
        if key == "sessions_by_close_reason":
            if not isinstance(value, dict) or set(value) != CLOSE_REASONS:
                raise CheckError(
                    f"{name}: sessions_by_close_reason keys must be {sorted(CLOSE_REASONS)}"
                )
            non_neg_ints(value, f"{name} counters.sessions_by_close_reason")
        else:
            non_neg_ints({key: value}, f"{name} counters")

    if "watermarks" in expected:
        watermarks = expected["watermarks"]
        if not isinstance(watermarks, dict):
            raise CheckError(f"{name}: watermarks must be an object")
        for key, value in watermarks.items():
            if not key.isdecimal() or int(key) not in input_batches:
                raise CheckError(
                    f"{name}: watermarks key {key!r} is not a batch number in input.jsonl"
                )
            if not isinstance(value, int) or isinstance(value, bool):
                raise CheckError(f"{name}: watermarks[{key!r}] must be an int")

    print(f"OK {name}")


def main() -> None:
    dirs = sorted(
        d for d in EXAMPLES_DIR.iterdir() if d.is_dir() and re.match(r"^\d\d-", d.name)
    )
    numbers = sorted(d.name[:2] for d in dirs)
    if numbers != [f"{i:02d}" for i in range(1, 17)]:
        raise SystemExit(
            f"expected exactly example folders 01..16, got {[d.name for d in dirs]}"
        )
    if len(dirs) != 16:
        raise SystemExit(f"expected 16 example folders, got {len(dirs)}")
    for d in dirs:
        check_example(d)
    if PROBES_DIR.is_dir():
        for d in sorted(p for p in PROBES_DIR.iterdir() if p.is_dir() and re.match(r"^\d\d-", p.name)):
            check_example(d)


if __name__ == "__main__":
    try:
        main()
    except CheckError as e:
        raise SystemExit(f"FAIL {e}") from e
