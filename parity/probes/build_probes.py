#!/usr/bin/env python3
"""Writes the stage-3 late-count probes from hand-entered values.

Same rules as parity/examples/build_examples.py, whose helpers this imports:
every input and expected value below is a hand-entered literal worked out from
contracts/<job>.md. The script only converts clock times to epoch ms and hashes
output_ids; expected values never come from running an implementation.

Conventions: day 2026-09-21 UTC; VIN TSTP + probe number zero-padded to 13
digits; event_id 00000000-0000-4000-8000-00000099NNii (NN probe, ii event index).
"""
from __future__ import annotations

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "examples"))

from build_examples import (  # noqa: E402
    BH, CS, HOUR, REJECTED, SOC, SOH, bat_counters, ch, ch_counters, plug_in, reading,
    session, t, window,
)


def vin(n: int) -> str:
    return "TSTP" + str(n).zfill(13)


def eid(n: int, i: int) -> str:
    return f"00000000-0000-4000-8000-00000099{n:02d}{i:02d}"


def write_probe(n, slug, job, batches, flush_clock, records, counters, watermarks):
    lines, seq, max_ts = [], 0, None
    for b, msgs in enumerate(batches, start=1):
        for m in msgs:
            seq += 1
            lines.append({"arrival_seq": seq, "batch": b, "message": m})
            if m["event_id"] + "@" + str(m["ts"]) in REJECTED:
                continue
            max_ts = m["ts"] if max_ts is None else max(max_ts, m["ts"])
    flush = t(flush_clock)
    assert flush == max_ts + HOUR, (n, flush, max_ts)
    lines.append({"advance_watermark_to": flush, "batch": len(batches) + 1})
    expected = {
        "job": job,
        "records": records,
        "counters": counters,
        "watermarks": {str(k): t(v) for k, v in watermarks.items()},
    }
    d = os.path.join(HERE, f"{n:02d}-{slug}")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "input.jsonl"), "w") as f:
        for line in lines:
            f.write(json.dumps(line, separators=(",", ":")) + "\n")
    with open(os.path.join(d, "expected.json"), "w") as f:
        json.dump(expected, f, indent=2)
        f.write("\n")


def main():
    # 1: several late copies of one ID.
    #    B1 plug_in 10:00, progress +100 10:30 -> WM 10:20.
    #    B2 three identical copies of progress Y +50 10:15, plus a copy of Y +70 at 10:20
    #       (exactly at the watermark) -> all four late (ts <= 10:20); WM stays 10:20.
    #    B3 copy of Y +50 at 10:25 -> not late; late copies are not remembered, so a first arrival.
    #    -> 150 Wh, 10:00-10:30, inactivity_timeout at flush; late 4; no duplicates
    n, v = 1, vin(1)
    late_copy = ch(eid(n, 3), v, t("10:15"), "PROGRESS", 50)
    write_probe(n, "several-late-copies-one-id", CS, [
        [plug_in(eid(n, 1), v, t("10:00")), ch(eid(n, 2), v, t("10:30"), "PROGRESS", 100)],
        [late_copy, dict(late_copy), dict(late_copy), ch(eid(n, 3), v, t("10:20"), "PROGRESS", 70)],
        [ch(eid(n, 3), v, t("10:25"), "PROGRESS", 50)],
    ], "11:30",
        [session(eid(n, 1), v, t("10:00"), t("10:30"), 30, 150, "INACTIVITY_TIMEOUT")],
        ch_counters(late=4, timeout=1),
        watermarks={1: "10:20", 2: "10:20", 3: "10:20"})

    # 2: late rows across several batches.
    #    B1 plug_in 10:00, progress +100 10:20                 -> WM 10:10
    #    B2 progress +10 10:05 (late, <= 10:10), progress +100 10:40 -> WM 10:30
    #    B3 progress +10 10:30 (late, exactly at 10:30), progress +100 10:50 -> WM 10:40
    #    B4 stop +5 10:35 (late, <= 10:40), unplug 11:00      -> WM 10:50
    #    -> accepted 10:00, 10:20, 10:40, 10:50, 11:00 (gaps <= 30 min): 300 Wh, 60 min, unplug; late 3
    n, v = 2, vin(2)
    write_probe(n, "late-rows-across-batches", CS, [
        [plug_in(eid(n, 1), v, t("10:00")), ch(eid(n, 2), v, t("10:20"), "PROGRESS", 100)],
        [ch(eid(n, 3), v, t("10:05"), "PROGRESS", 10), ch(eid(n, 4), v, t("10:40"), "PROGRESS", 100)],
        [ch(eid(n, 5), v, t("10:30"), "PROGRESS", 10), ch(eid(n, 6), v, t("10:50"), "PROGRESS", 100)],
        [ch(eid(n, 7), v, t("10:35"), "STOP", 5), ch(eid(n, 8), v, t("11:00"), "UNPLUG")],
    ], "12:00",
        [session(eid(n, 1), v, t("10:00"), t("11:00"), 60, 300, "UNPLUG")],
        ch_counters(late=3, unplug=1),
        watermarks={1: "10:10", 2: "10:30", 3: "10:40", 4: "10:50"})

    # 3: no late rows, watermark moving every batch.
    #    B1 plug_in 10:00, progress +100 10:10                 -> WM 10:00
    #    B2 start 10:00:00.001 (1 ms above the watermark, not late), progress +200 10:20 -> WM 10:10
    #    B3 unplug 10:30                                       -> WM 10:20
    #    -> 300 Wh, 30 min, unplug; late 0
    n, v = 3, vin(3)
    write_probe(n, "no-late-rows", CS, [
        [plug_in(eid(n, 1), v, t("10:00")), ch(eid(n, 2), v, t("10:10"), "PROGRESS", 100)],
        [ch(eid(n, 3), v, t("10:00:00.001"), "START"), ch(eid(n, 4), v, t("10:20"), "PROGRESS", 200)],
        [ch(eid(n, 5), v, t("10:30"), "UNPLUG")],
    ], "11:30",
        [session(eid(n, 1), v, t("10:00"), t("10:30"), 30, 300, "UNPLUG")],
        ch_counters(unplug=1),
        watermarks={1: "10:00", 2: "10:10", 3: "10:20"})

    # 4: battery late rows (2-minute delay).
    #    B1 readings 10:00 (30.0 C), 10:06 (30.0 C)           -> WM 10:04
    #    B2 readings 10:03 (60.0 C, late), 10:04 (60.0 C, late, exactly at WM), 10:07 (31.0 C) -> WM 10:05
    #    -> window 10:00: 1 reading, max 30.0, no alert (the late 60.0s never count);
    #       window 10:05: 10:06 + 10:07, max 31.0, no alert; late 2
    n, v = 4, vin(4)
    write_probe(n, "battery-late-row", BH, [
        [reading(eid(n, 1), v, t("10:00"), 30.0), reading(eid(n, 2), v, t("10:06"), 30.0)],
        [reading(eid(n, 3), v, t("10:03"), 60.0), reading(eid(n, 4), v, t("10:04"), 60.0),
         reading(eid(n, 5), v, t("10:07"), 31.0)],
    ], "11:07", [
        window(v, t("10:00"), SOC, SOH, 30.0, False, 1),
        window(v, t("10:05"), SOC, SOH, 31.0, False, 2),
    ], bat_counters(late=2),
        watermarks={1: "10:04", 2: "10:05"})

    # 5: on-time conflicting copy of an ID whose VIN has no open session or buffered events.
    #    B1 VIN A plug_in 10:00, unplug X 10:10                -> WM 10:00
    #    B2 VIN B plug_in 10:30                                -> WM 10:20; A's unplug is processed,
    #       A's session closes (unplug, 10 min, 0 Wh); A now holds only its seen IDs.
    #    B3 VIN A copy of X with ts 10:25 (> 10:20, not late) -> X is still seen (IDs are kept per
    #       VIN for the whole run), payload differs -> conflicting_duplicates 1, not an orphan.
    #    -> B's session closes at flush: 0 min, 0 Wh, inactivity_timeout
    n, va, vb = 5, vin(5), "TSTP000000000005B"
    write_probe(n, "conflicting-copy-after-session-closed", CS, [
        [plug_in(eid(n, 1), va, t("10:00")), ch(eid(n, 2), va, t("10:10"), "UNPLUG")],
        [plug_in(eid(n, 3), vb, t("10:30"))],
        [ch(eid(n, 2), va, t("10:25"), "UNPLUG")],
    ], "11:30", [
        session(eid(n, 1), va, t("10:00"), t("10:10"), 10, 0, "UNPLUG"),
        session(eid(n, 3), vb, t("10:30"), t("10:30"), 0, 0, "INACTIVITY_TIMEOUT"),
    ], ch_counters(conflict=1, unplug=1, timeout=1),
        watermarks={1: "10:00", 2: "10:20", 3: "10:20"})


if __name__ == "__main__":
    main()
