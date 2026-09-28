#!/usr/bin/env python3
"""Writes the 16 acceptance examples from hand-entered values.

Every input value and every expected value below was entered by hand from the
spec's table (SPEC.md, "Acceptance examples"). This script does only two things
with them: convert clock times to epoch milliseconds and compute output_id
SHA-256 hashes. Assertions cross-check hand-entered values against each other
(flush target, durations); they never produce a value.

Conventions (stage 2 choices, listed in the MR):
  day            2026-09-21 UTC (example 9 crosses into 2026-09-22)
  VIN            TST + example number zero-padded to 14 digits
  event_id       00000000-0000-4000-8000-00000000NNii  (NN example, ii event index)
  plug_in coords lat 37.4, lon -122.1, charger_type "dc_fast" unless the spec gives them
  battery values soc 80.0, soh 95.0, voltage 400.0 unless the spec gives them
"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
DAY = datetime(2026, 9, 21, tzinfo=timezone.utc)
MIN = 60_000
HOUR = 60 * MIN

LAT, LON, CHARGER = 37.4, -122.1, "dc_fast"
SOC, SOH, VOLT = 80.0, 95.0, 400.0


def t(clock: str, day_offset: int = 0) -> int:
    """'HH:MM', 'HH:MM:SS' or 'HH:MM:SS.mmm' on DAY (+ day_offset) -> epoch ms."""
    parts = clock.split(":")
    h, m = int(parts[0]), int(parts[1])
    s, ms = 0, 0
    if len(parts) == 3:
        sec = parts[2].split(".")
        s = int(sec[0])
        ms = int(sec[1].ljust(3, "0")) if len(sec) == 2 else 0
    d = DAY + timedelta(days=day_offset, hours=h, minutes=m, seconds=s, milliseconds=ms)
    return int(d.timestamp() * 1000)


def vin(n: int) -> str:
    return "TST" + str(n).zfill(14)


def eid(n: int, i: int) -> str:
    return f"00000000-0000-4000-8000-00000000{n:02d}{i:02d}"


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# self-checks against the spec's worked examples
assert t("14:15") == 1790000100000
assert sha("00000000-0000-4000-8000-000000000001") == (
    "11e594f481958c10e3015d0bf0447a22f068a8a647f475df15ce2c7ab4b8f3f1"
)
assert sha("battery-health|TST00000000000001|1790000100000") == (
    "924bbe50976ec7a530dd47f70c1c42f19bb46342e4e320a06a3e076bb70feda8"
)


def ch(event_id, v, ts, event, energy_wh=0, lat=None, lon=None, charger=None):
    m = {"event_id": event_id, "vin": v, "ts": ts, "event": event, "energy_wh": energy_wh}
    if lat is not None:
        m["lat"] = lat
    if lon is not None:
        m["lon"] = lon
    if charger is not None:
        m["charger_type"] = charger
    return m


def plug_in(event_id, v, ts, lat=LAT, lon=LON, charger=CHARGER):
    return ch(event_id, v, ts, "PLUG_IN", 0, lat, lon, charger)


def reading(event_id, v, ts, temp, soc=SOC, soh=SOH, volt=VOLT):
    return {
        "event_id": event_id,
        "vin": v,
        "ts": ts,
        "soc_pct": soc,
        "soh_pct": soh,
        "cell_temp_max_c": temp,
        "pack_voltage_v": volt,
    }


def session(plug_event_id, v, start, end, minutes, wh, reason, lat=LAT, lon=LON, charger=CHARGER):
    assert end - start == minutes * MIN, (v, start, end, minutes)
    return {
        "output_id": sha(plug_event_id),
        "vin": v,
        "start_ts": start,
        "end_ts": end,
        "duration_ms": minutes * MIN,
        "total_energy_wh": wh,
        "start_lat": lat,
        "start_lon": lon,
        "charger_type": charger,
        "close_reason": reason,
    }


def window(v, start, avg_soc, min_soh, max_temp, alert, count):
    return {
        "output_id": sha(f"battery-health|{v}|{start}"),
        "vin": v,
        "window_start": start,
        "avg_soc_pct": avg_soc,
        "min_soh_pct": min_soh,
        "max_cell_temp_c": max_temp,
        "alert": alert,
        "event_count": count,
    }


def ch_counters(rejected=0, late=0, dup=0, conflict=0, orphan=0, unplug=0, timeout=0, replaced=0):
    return {
        "rejected": rejected,
        "late": late,
        "duplicate_events": dup,
        "conflicting_duplicates": conflict,
        "orphan": orphan,
        "sessions_by_close_reason": {
            "unplug": unplug,
            "inactivity_timeout": timeout,
            "replaced_by_plug_in": replaced,
        },
    }


def bat_counters(rejected=0, late=0, dup=0, conflict=0):
    return {
        "rejected": rejected,
        "late": late,
        "duplicate_events": dup,
        "conflicting_duplicates": conflict,
    }


def write_example(n, slug, job, batches, flush_clock, records, counters, watermarks=None, flush_day_offset=0):
    """batches: list of lists of messages, in arrival order."""
    lines = []
    seq = 0
    max_ts = None
    for b, msgs in enumerate(batches, start=1):
        for m in msgs:
            seq += 1
            lines.append({"arrival_seq": seq, "batch": b, "message": m})
            max_ts = m["ts"] if max_ts is None else max(max_ts, m["ts"])
    flush = t(flush_clock, flush_day_offset)
    assert flush == max_ts + HOUR, (n, flush, max_ts)
    lines.append({"advance_watermark_to": flush, "batch": len(batches) + 1})

    expected = {"job": job, "records": records, "counters": counters}
    if watermarks:
        expected["watermarks"] = {str(k): t(v) for k, v in watermarks.items()}

    d = os.path.join(HERE, f"{n:02d}-{slug}")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "input.jsonl"), "w") as f:
        for line in lines:
            f.write(json.dumps(line, separators=(",", ":")) + "\n")
    with open(os.path.join(d, "expected.json"), "w") as f:
        json.dump(expected, f, indent=2)
        f.write("\n")


CS = "charging-sessions"
BH = "battery-health"


def main():
    # 1: plug_in 10:00; progress +2000 10:10; stop +500 10:20; unplug 10:25 -> 2500 Wh; 25 min; unplug
    n, v = 1, vin(1)
    base = [
        plug_in(eid(n, 1), v, t("10:00")),
        ch(eid(n, 2), v, t("10:10"), "PROGRESS", 2000),
        ch(eid(n, 3), v, t("10:20"), "STOP", 500),
        ch(eid(n, 4), v, t("10:25"), "UNPLUG"),
    ]
    write_example(n, "basic-session", CS, [base], "11:25",
                  [session(eid(n, 1), v, t("10:00"), t("10:25"), 25, 2500, "UNPLUG")],
                  ch_counters(unplug=1))

    # 2: as 1 plus an identical second copy of the 10:10 progress -> 2500 Wh; duplicate_events 1
    n, v = 2, vin(2)
    prog = ch(eid(n, 2), v, t("10:10"), "PROGRESS", 2000)
    write_example(n, "identical-duplicate", CS, [[
        plug_in(eid(n, 1), v, t("10:00")),
        prog,
        dict(prog),
        ch(eid(n, 3), v, t("10:20"), "STOP", 500),
        ch(eid(n, 4), v, t("10:25"), "UNPLUG"),
    ]], "11:25",
        [session(eid(n, 1), v, t("10:00"), t("10:25"), 25, 2500, "UNPLUG")],
        ch_counters(dup=1, unplug=1))

    # 3: as 1 plus a second copy of the 10:10 progress claiming +9000 -> 2500 Wh; conflicting_duplicates 1
    n, v = 3, vin(3)
    write_example(n, "conflicting-duplicate", CS, [[
        plug_in(eid(n, 1), v, t("10:00")),
        ch(eid(n, 2), v, t("10:10"), "PROGRESS", 2000),
        ch(eid(n, 2), v, t("10:10"), "PROGRESS", 9000),
        ch(eid(n, 3), v, t("10:20"), "STOP", 500),
        ch(eid(n, 4), v, t("10:25"), "UNPLUG"),
    ]], "11:25",
        [session(eid(n, 1), v, t("10:00"), t("10:25"), 25, 2500, "UNPLUG")],
        ch_counters(conflict=1, unplug=1))

    # 4: B1 plug_in 10:00, progress +100 10:05. B2 copy of the 10:05 progress with ts 10:30 and +900.
    #    B3 progress +100 10:15. -> WM 9:55 after B1, 10:20 after B2. 100 Wh; ends 10:05,
    #    inactivity_timeout; conflicting_duplicates 1; late 1
    n, v = 4, vin(4)
    write_example(n, "conflicting-copy-moves-watermark", CS, [
        [plug_in(eid(n, 1), v, t("10:00")), ch(eid(n, 2), v, t("10:05"), "PROGRESS", 100)],
        [ch(eid(n, 2), v, t("10:30"), "PROGRESS", 900)],
        [ch(eid(n, 3), v, t("10:15"), "PROGRESS", 100)],
    ], "11:30",
        [session(eid(n, 1), v, t("10:00"), t("10:05"), 5, 100, "INACTIVITY_TIMEOUT")],
        ch_counters(late=1, conflict=1, timeout=1),
        watermarks={1: "09:55", 2: "10:20"})

    # 5: plug_in 10:00; progress +100 10:30 -> one session 100 Wh; ends 10:30, inactivity_timeout
    n, v = 5, vin(5)
    write_example(n, "gap-exactly-30-min", CS, [[
        plug_in(eid(n, 1), v, t("10:00")),
        ch(eid(n, 2), v, t("10:30"), "PROGRESS", 100),
    ]], "11:30",
        [session(eid(n, 1), v, t("10:00"), t("10:30"), 30, 100, "INACTIVITY_TIMEOUT")],
        ch_counters(timeout=1))

    # 6: plug_in 10:00; progress +100 10:30:00.001 -> 0 Wh session ending 10:00, inactivity_timeout; orphan 1
    n, v = 6, vin(6)
    write_example(n, "gap-over-30-min-orphan", CS, [[
        plug_in(eid(n, 1), v, t("10:00")),
        ch(eid(n, 2), v, t("10:30:00.001"), "PROGRESS", 100),
    ]], "11:30:00.001",
        [session(eid(n, 1), v, t("10:00"), t("10:00"), 0, 0, "INACTIVITY_TIMEOUT")],
        ch_counters(orphan=1, timeout=1))

    # 7: plug_in 10:00; progress +100 10:10 -> emitted once WM passes 10:40: 100 Wh; ends 10:10, inactivity_timeout
    n, v = 7, vin(7)
    write_example(n, "timeout-after-flush", CS, [[
        plug_in(eid(n, 1), v, t("10:00")),
        ch(eid(n, 2), v, t("10:10"), "PROGRESS", 100),
    ]], "11:10",
        [session(eid(n, 1), v, t("10:00"), t("10:10"), 10, 100, "INACTIVITY_TIMEOUT")],
        ch_counters(timeout=1))

    # 8: B1 plug_in 10:00, progress +100 10:20. B2 progress +50 10:10 -> WM 10:10 after B1;
    #    exactly at the watermark is late. 100 Wh; late 1
    n, v = 8, vin(8)
    write_example(n, "late-exactly-at-watermark", CS, [
        [plug_in(eid(n, 1), v, t("10:00")), ch(eid(n, 2), v, t("10:20"), "PROGRESS", 100)],
        [ch(eid(n, 3), v, t("10:10"), "PROGRESS", 50)],
    ], "11:20",
        [session(eid(n, 1), v, t("10:00"), t("10:20"), 20, 100, "INACTIVITY_TIMEOUT")],
        ch_counters(late=1, timeout=1),
        watermarks={1: "10:10"})

    # 9: plug_in 23:50; unplug 00:10 next day -> one 20-minute session
    n, v = 9, vin(9)
    write_example(n, "crosses-midnight", CS, [[
        plug_in(eid(n, 1), v, t("23:50")),
        ch(eid(n, 2), v, t("00:10", 1), "UNPLUG"),
    ]], "01:10", 
        [session(eid(n, 1), v, t("23:50"), t("00:10", 1), 20, 0, "UNPLUG")],
        ch_counters(unplug=1), flush_day_offset=1)

    # 10: plug_in 10:00; progress +100 10:05; plug_in 10:10; unplug 10:20
    #     -> 100 Wh ending 10:05, replaced_by_plug_in; then 0 Wh, 10 min, unplug
    n, v = 10, vin(10)
    write_example(n, "plug-in-replaces-open-session", CS, [[
        plug_in(eid(n, 1), v, t("10:00")),
        ch(eid(n, 2), v, t("10:05"), "PROGRESS", 100),
        plug_in(eid(n, 3), v, t("10:10")),
        ch(eid(n, 4), v, t("10:20"), "UNPLUG"),
    ]], "11:20", [
        session(eid(n, 1), v, t("10:00"), t("10:05"), 5, 100, "REPLACED_BY_PLUG_IN"),
        session(eid(n, 3), v, t("10:10"), t("10:20"), 10, 0, "UNPLUG"),
    ], ch_counters(unplug=1, replaced=1))

    # 11: plug_in 10:00; progress ID X -5 Wh 10:05; progress ID X +200 10:10; unplug 10:15
    #     -> 200 Wh; rejected 1; no duplicates
    n, v = 11, vin(11)
    write_example(n, "invalid-first-then-valid-copy", CS, [[
        plug_in(eid(n, 1), v, t("10:00")),
        ch(eid(n, 2), v, t("10:05"), "PROGRESS", -5),
        ch(eid(n, 2), v, t("10:10"), "PROGRESS", 200),
        ch(eid(n, 3), v, t("10:15"), "UNPLUG"),
    ]], "11:15",
        [session(eid(n, 1), v, t("10:00"), t("10:15"), 15, 200, "UNPLUG")],
        ch_counters(rejected=1, unplug=1))

    # 12: plug_in 10:00; progress +100 10:05; stop +50 10:10; start 10:20; progress +100 10:30; unplug 10:35
    #     -> one session: 250 Wh; 35 min; unplug
    n, v = 12, vin(12)
    write_example(n, "stop-then-resume", CS, [[
        plug_in(eid(n, 1), v, t("10:00")),
        ch(eid(n, 2), v, t("10:05"), "PROGRESS", 100),
        ch(eid(n, 3), v, t("10:10"), "STOP", 50),
        ch(eid(n, 4), v, t("10:20"), "START"),
        ch(eid(n, 5), v, t("10:30"), "PROGRESS", 100),
        ch(eid(n, 6), v, t("10:35"), "UNPLUG"),
    ]], "11:35",
        [session(eid(n, 1), v, t("10:00"), t("10:35"), 35, 250, "UNPLUG")],
        ch_counters(unplug=1))

    # 13: B1 plug_in 10:00, progress +100 10:30. B2 progress ID Y +50 10:15. B3 progress ID Y +50 10:25
    #     -> B2's copy late (WM 10:20), not remembered; B3's copy is a first arrival.
    #        150 Wh; ends 10:30; late 1; no duplicates
    n, v = 13, vin(13)
    write_example(n, "late-first-arrival-not-remembered", CS, [
        [plug_in(eid(n, 1), v, t("10:00")), ch(eid(n, 2), v, t("10:30"), "PROGRESS", 100)],
        [ch(eid(n, 3), v, t("10:15"), "PROGRESS", 50)],
        [ch(eid(n, 3), v, t("10:25"), "PROGRESS", 50)],
    ], "11:30",
        [session(eid(n, 1), v, t("10:00"), t("10:30"), 30, 150, "INACTIVITY_TIMEOUT")],
        ch_counters(late=1, timeout=1),
        watermarks={1: "10:20"})

    # 14: two VINs: plug_in at (-33.8675, 151.2095) and at (0.0005, -0.0005)
    #     -> rounded to (-33.868, 151.210) and (0.001, -0.001)
    n = 14
    va, vb = vin(14), "TST0000000000014B"
    write_example(n, "coordinate-rounding", CS, [[
        plug_in(eid(n, 1), va, t("10:00"), lat=-33.8675, lon=151.2095),
        plug_in(eid(n, 2), vb, t("10:00"), lat=0.0005, lon=-0.0005),
    ]], "11:00", [
        session(eid(n, 1), va, t("10:00"), t("10:00"), 0, 0, "INACTIVITY_TIMEOUT", lat=-33.868, lon=151.210),
        session(eid(n, 2), vb, t("10:00"), t("10:00"), 0, 0, "INACTIVITY_TIMEOUT", lat=0.001, lon=-0.001),
    ], ch_counters(timeout=2))

    # 15: battery readings at 14:14:59.999 and 14:15:00.000 -> two windows, starting 14:10 and 14:15
    n, v = 15, vin(15)
    temp = 30.0
    write_example(n, "battery-window-boundary", BH, [[
        reading(eid(n, 1), v, t("14:14:59.999"), temp),
        reading(eid(n, 2), v, t("14:15:00.000"), temp),
    ]], "15:15", [
        window(v, t("14:10"), SOC, SOH, temp, False, 1),
        window(v, t("14:15"), SOC, SOH, temp, False, 1),
    ], bat_counters())

    # 16: battery: one window with max cell temp 55.0, another with 55.1 -> alert false, then true
    n, v = 16, vin(16)
    write_example(n, "battery-alert-strictly-above-55", BH, [[
        reading(eid(n, 1), v, t("10:00"), 55.0),
        reading(eid(n, 2), v, t("10:05"), 55.1),
    ]], "11:05", [
        window(v, t("10:00"), SOC, SOH, 55.0, False, 1),
        window(v, t("10:05"), SOC, SOH, 55.1, True, 1),
    ], bat_counters())


if __name__ == "__main__":
    main()
