"""Seeded synthetic replay fixtures for both jobs (parity/replay/FORMAT.md).

Writes parity/replay/charging-sessions.jsonl, parity/replay/battery-health.jsonl and
parity/replay/generator-run.json (seed, Python version, sizes, case summary), then
prints a summary table. Same seed and Python version => same bytes.

Every injected row carries an intended contract class (rejected / late / accepted /
duplicate / conflicting); the generator re-classifies the finished fixture with
tools/contract.py and fails if any row's class differs from its intent. That checks
the fixture contains the cases it claims; outputs still come only from the jobs.
"""

import argparse
import hashlib
import json
import os
import platform
import random
import sys
import uuid
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "tools"))
sys.path.insert(0, os.environ.get("PB2_DIR", str(REPO / "generator")))

import contract  # noqa: E402

SEED = 20260922
DAY0 = 1_790_035_200_000  # 2026-09-22 00:00 UTC
DAY = 86_400_000
MIN = 60_000
BATCH_MS = 30 * MIN
VEHICLES = 200
MAX_FIXTURE_BYTES = 5_000_000
CHARGER_TYPES = ("ac_l1", "ac_l2", "dc_fast")
WH_PER_MIN = {"ac_l1": 23, "ac_l2": 120, "dc_fast": 2_500}
# Arrival = ts + network delay. Normal delays are small; a few rows straggle, but always
# by less than the job's watermark delay minus the watermark-moving copies' lead, so no
# row is late unless it was made late on purpose.
JITTER = {"charging-sessions": (20_000, 5 * MIN), "battery-health": (5_000, 90_000)}
WM_MOVE_LEAD = {"charging-sessions": 3 * MIN, "battery-health": 10_000}


class Fixture:
    def __init__(self, job, rng):
        self.job = job
        self.rng = rng
        self.rows = []
        self.cases = Counter()
        self.ids = set()

    def new_id(self):
        while True:
            eid = str(uuid.UUID(int=self.rng.getrandbits(128), version=4))
            if eid not in self.ids:
                self.ids.add(eid)
                return eid

    def add(self, msg, arrival, intent="accepted", case=None, batch=None):
        row = {"arrival": arrival, "ord": len(self.rows), "msg": msg, "intent": intent,
               "case": case, "batch": batch}
        self.rows.append(row)
        if case:
            self.cases[case] += 1
        return row

    def jitter(self):
        small, big = JITTER[self.job]
        if self.rng.random() < 0.05:
            return self.rng.randrange(small, big)
        return self.rng.randrange(100, small)

    def base_rows(self, pred=lambda r: True):
        return [r for r in self.rows if r["case"] is None and r["intent"] == "accepted" and pred(r)]

    def assign_batches(self):
        self.rows.sort(key=lambda r: (r["arrival"], r["ord"]))
        slots = sorted({(r["arrival"] - DAY0) // BATCH_MS for r in self.rows})
        dense = {s: i + 1 for i, s in enumerate(slots)}
        for r in self.rows:
            r["batch"] = dense[(r["arrival"] - DAY0) // BATCH_MS]

    def ordered(self):
        return sorted(self.rows, key=lambda r: (r["batch"], r["arrival"], r["ord"]))

    def classify(self):
        rows = self.ordered()
        classes, wm = contract.classify(self.job, [{"batch": r["batch"], "message": r["msg"]} for r in rows])
        return rows, classes, wm


def coord(text):
    v = float(text)
    assert repr(v) == repr(float(Decimal(text))) and len(text.split(".")[-1]) <= 6
    return v


def vins():
    return ["TSTV%013d" % i for i in range(1, VEHICLES + 1)]


# ---------------------------------------------------------------- charging

SPECIAL_COORDS = [
    ("-33.8675", "151.2095"), ("0.0005", "-0.0005"), ("-0.0005", "0.0005"),
    ("89.9995", "-179.9995"), ("-89.9995", "179.9995"), ("-0.0004", "0.0004"),
    ("-12.3445", "-45.6785"), ("90", "-180"), ("-90", "180"), ("1.0005", "-2.0015"),
]


def plug_coords(rng, home, special):
    if special:
        lat, lon = special.pop(0)
        kind = "special"
    else:
        kind = rng.choice(("half", "half", "six", "short"))
        out = []
        for base, lim in ((home[0], 89), (home[1], 179)):
            v = max(-lim, min(lim, base + rng.uniform(-0.2, 0.2)))
            if kind == "half":
                k = int(Decimal(v).quantize(Decimal("0.001")) * 1000)
                d = Decimal(k * 10 + (5 if k >= 0 else -5)) / 10000
            elif kind == "six":
                d = Decimal(round(v * 1_000_000)) / 1_000_000
            else:
                d = Decimal(round(v * 100)) / 100
            out.append(format(d.normalize(), "f"))
        lat, lon = out
    return coord(lat), coord(lon), kind


def is_half(x):
    frac = repr(x).split(".")[-1] if "." in repr(x) else ""
    return len(frac) == 4 and frac[-1] == "5"


def cmsg(eid, vin, ts, event, energy=0, lat=None, lon=None, ct=None):
    m = {"event_id": eid, "vin": vin, "ts": ts, "event": event, "energy_wh": energy}
    if lat is not None:
        m["lat"] = lat
    if lon is not None:
        m["lon"] = lon
    if ct is not None:
        m["charger_type"] = ct
    return m


SCENARIOS = ["stop_then_resume"] * 30 + ["plug_in_during_open_session"] * 20 + \
    ["no_unplug_after_stop"] * 15 + ["no_unplug_mid_progress"] * 15 + \
    ["gap_exactly_30_min"] * 8 + ["gap_over_30_min_then_orphans"] * 8


def charging(rng):
    fx = Fixture("charging-sessions", rng)
    special = list(SPECIAL_COORDS)
    sessions = Counter()
    coords = Counter()
    midnight = set(rng.sample(vins(), 5))
    plan = []
    for vin in vins():
        n = rng.choice((1, 2, 2, 3))
        plan.append((vin, n))
    total = sum(n for _, n in plan)
    scen = SCENARIOS + ["normal"] * (total - len(SCENARIOS))
    rng.shuffle(scen)
    si = 0
    for vin, n in plan:
        home = (rng.uniform(-55, 65), rng.uniform(-170, 175))
        t = DAY0 + rng.randrange(10 * MIN, 8 * 60 * MIN)
        for j in range(n):
            last = j == n - 1
            kind = scen[si]
            si += 1
            if last and vin in midnight:
                kind = "crosses_midnight"
                t = max(t, DAY0 + DAY - rng.randrange(20 * MIN, 45 * MIN))
            if t > DAY0 + DAY - 50 * MIN and kind != "crosses_midnight":
                break
            ct = rng.choice(CHARGER_TYPES)
            lat, lon, ck = plug_coords(rng, home, special)
            coords[ck] += 1
            coords["negative"] += lat < 0 or lon < 0
            coords["exact_half_3rd_decimal"] += is_half(lat) or is_half(lon)
            sessions[kind] += 1
            ev = [(t, "PLUG_IN", 0, (lat, lon, ct))]
            now = t + rng.randrange(30_000, 3 * MIN)
            ev.append((now, "START", 0, None))
            span = rng.randrange(20, 60) if ct == "dc_fast" else rng.randrange(40, 180)
            end = t + span * MIN
            if kind == "crosses_midnight":
                end = max(end, DAY0 + DAY + 5 * MIN)
            special_gap_at = rng.randrange(1, 4)
            k = 0
            while now < end:
                step = rng.randrange(3 * MIN, 8 * MIN)
                k += 1
                if k == special_gap_at and kind == "gap_exactly_30_min":
                    step = 30 * MIN
                if k == special_gap_at and kind == "gap_over_30_min_then_orphans":
                    step = 30 * MIN + 1
                if k == special_gap_at and kind == "plug_in_during_open_session":
                    now += step
                    ct = rng.choice(CHARGER_TYPES)
                    lat, lon, ck = plug_coords(rng, home, special)
                    coords[ck] += 1
                    coords["negative"] += lat < 0 or lon < 0
                    coords["exact_half_3rd_decimal"] += is_half(lat) or is_half(lon)
                    ev.append((now, "PLUG_IN", 0, (lat, lon, ct)))
                    continue
                now += step
                ev.append((now, "PROGRESS", int(WH_PER_MIN[ct] * step / MIN * rng.uniform(0.8, 1.1)), None))
            if kind == "no_unplug_mid_progress":
                pass
            else:
                now += rng.randrange(MIN, 6 * MIN)
                ev.append((now, "STOP", rng.randrange(0, WH_PER_MIN[ct] * 2), None))
                if kind == "stop_then_resume":
                    now += rng.randrange(5 * MIN, 25 * MIN)
                    ev.append((now, "START", 0, None))
                    for _ in range(rng.randrange(1, 4)):
                        now += rng.randrange(3 * MIN, 8 * MIN)
                        ev.append((now, "PROGRESS", rng.randrange(1, WH_PER_MIN[ct] * 5), None))
                    now += rng.randrange(MIN, 4 * MIN)
                    ev.append((now, "STOP", rng.randrange(0, WH_PER_MIN[ct]), None))
                if kind != "no_unplug_after_stop":
                    now += rng.randrange(MIN, 15 * MIN)
                    ev.append((now, "UNPLUG", 0, None))
            for ts, event, energy, plug in ev:
                m = cmsg(fx.new_id(), vin, ts, event, energy, *(plug or (None, None, None)))
                fx.add(m, ts + fx.jitter())
            t = now + rng.randrange(45 * MIN, 5 * 60 * MIN)
    inject(fx, rng)
    return fx, {"sessions_by_scenario": dict(sorted(sessions.items())),
                "plug_in_coordinates": dict(sorted(coords.items()))}


def charging_invalid(fx, rng, vin, ts):
    """(reason, message) pairs, each failing exactly one contract rule."""
    p = dict(lat=37.4, lon=-122.1, ct="ac_l2")
    return [
        ("event_id_uppercase", cmsg(str(uuid.UUID(int=rng.getrandbits(128), version=4)).upper(), vin, ts, "PROGRESS", 10)),
        ("event_id_no_dashes", cmsg(uuid.UUID(int=rng.getrandbits(128), version=4).hex, vin, ts, "PROGRESS", 10)),
        ("vin_16_chars", cmsg(fx.new_id(), vin[:16], ts, "PROGRESS", 10)),
        ("vin_lowercase", cmsg(fx.new_id(), vin.lower(), ts, "PROGRESS", 10)),
        ("ts_at_2100_exclusive_bound", cmsg(fx.new_id(), vin, contract.TS_MAX, "PROGRESS", 10)),
        ("ts_before_2020", cmsg(fx.new_id(), vin, contract.TS_MIN - 1, "PROGRESS", 10)),
        ("ts_zero", cmsg(fx.new_id(), vin, 0, "UNPLUG")),
        ("event_unspecified", cmsg(fx.new_id(), vin, ts, "EVENT_UNSPECIFIED")),
        ("energy_negative", cmsg(fx.new_id(), vin, ts, "PROGRESS", -5)),
        ("energy_on_plug_in", cmsg(fx.new_id(), vin, ts, "PLUG_IN", 5, p["lat"], p["lon"], p["ct"])),
        ("energy_on_start", cmsg(fx.new_id(), vin, ts, "START", 5)),
        ("energy_on_unplug", cmsg(fx.new_id(), vin, ts, "UNPLUG", 5)),
        ("plug_in_missing_lat", cmsg(fx.new_id(), vin, ts, "PLUG_IN", 0, None, p["lon"], p["ct"])),
        ("plug_in_missing_lon", cmsg(fx.new_id(), vin, ts, "PLUG_IN", 0, p["lat"], None, p["ct"])),
        ("plug_in_missing_charger_type", cmsg(fx.new_id(), vin, ts, "PLUG_IN", 0, p["lat"], p["lon"], None)),
        ("plug_in_empty_charger_type", cmsg(fx.new_id(), vin, ts, "PLUG_IN", 0, p["lat"], p["lon"], "")),
        ("plug_in_lat_above_90", cmsg(fx.new_id(), vin, ts, "PLUG_IN", 0, 90.0001, p["lon"], p["ct"])),
        ("plug_in_lon_below_minus_180", cmsg(fx.new_id(), vin, ts, "PLUG_IN", 0, p["lat"], -180.0001, p["ct"])),
    ]


def charging_conflict(rng, m):
    c = dict(m)
    if m["event"] in ("PROGRESS", "STOP"):
        c["energy_wh"] = m["energy_wh"] + rng.randrange(100, 9_000)
    elif m["event"] == "PLUG_IN":
        c["lat"] = coord(format(Decimal(repr(m["lat"])) / 2, "f")[:10]) if m["lat"] else 1.0
        c["charger_type"] = "dc_fast" if m["charger_type"] != "dc_fast" else "ac_l2"
    else:
        c["ts"] = m["ts"] - 1_000
    return c


def invalid_twin_charging(m):
    c = dict(m)
    if m["event"] == "PLUG_IN":
        del c["lat"]
    elif m["event"] in ("PROGRESS", "STOP"):
        c["energy_wh"] = -abs(m["energy_wh"]) - 1
    else:
        c["event"] = "EVENT_UNSPECIFIED"
    return c


# ---------------------------------------------------------------- battery

def bmsg(eid, vin, ts, soc, soh, temp, volt):
    m = {"event_id": eid, "vin": vin, "ts": ts}
    for k, v in (("soc_pct", soc), ("soh_pct", soh), ("cell_temp_max_c", temp), ("pack_voltage_v", volt)):
        if v is not None:
            m[k] = v
    return m


def battery(rng):
    fx = Fixture("battery-health", rng)
    extra = Counter()
    for vin in vins():
        soh = round(rng.uniform(82, 99.5), 2)
        hot = rng.random() < 0.1
        t = DAY0 + rng.randrange(0, 4 * 60 * MIN)
        for _ in range(rng.choice((1, 2, 2, 3))):
            if t > DAY0 + DAY - 60 * MIN:
                break
            soc = rng.uniform(20, 95)
            temp = rng.uniform(22, 38)
            for i in range(rng.randrange(15, 50)):
                ts = t + i * MIN + rng.randrange(0, 1_000)
                soc = max(1.0, soc - rng.uniform(0.05, 0.4))
                temp = min(58.0 if hot else 45.0, temp + rng.uniform(-0.3, 0.9 if hot else 0.4))
                fx.add(bmsg(fx.new_id(), vin, ts, round(soc, 2), soh, round(temp, 1),
                            round(350 + soc * 0.7, 1)), ts + fx.jitter())
                extra["hot_trip_readings"] += hot
            t += rng.randrange(60 * MIN, 8 * 60 * MIN)
    # Window boundaries (example 15) and alert threshold (example 16), set on base readings.
    cool = fx.base_rows(lambda r: r["msg"]["cell_temp_max_c"] <= 45.0)
    # Only ever moved earlier, so ts <= arrival still holds.
    for r in rng.sample(cool, 400):
        ts = r["msg"]["ts"]
        b = ts - ts % 300_000
        if ts - b < 60_000 and extra["reading_at_window_start"] < 8:
            r["msg"]["ts"] = b
            extra["reading_at_window_start"] += 1
        elif 60_000 <= ts - b < 120_000 and extra["reading_at_window_end_minus_1ms"] < 8:
            r["msg"]["ts"] = b - 1
            extra["reading_at_window_end_minus_1ms"] += 1
    for i, r in enumerate(rng.sample(cool, 16)):
        r["msg"]["cell_temp_max_c"] = 55.0 if i % 2 == 0 else 55.1
        extra["temp_exactly_55.0" if i % 2 == 0 else "temp_55.1"] += 1
    for (field, value), r in zip([("soc_pct", 0.0), ("soc_pct", 100.0), ("soh_pct", 100.0),
                                  ("cell_temp_max_c", -60.0), ("cell_temp_max_c", 120.0),
                                  ("pack_voltage_v", 1000.0), ("pack_voltage_v", 0.001)],
                                 rng.sample(fx.base_rows(), 7)):
        r["msg"][field] = value
        extra["valid_boundary_value"] += 1
    inject(fx, rng)
    return fx, {"readings": dict(sorted(extra.items()))}


def battery_invalid(fx, rng, vin, ts):
    ok = dict(soc=50.0, soh=95.0, temp=30.0, volt=400.0)

    def b(**kw):
        v = {**ok, **kw}
        return bmsg(fx.new_id(), kw.get("vin", vin), kw.get("ts", ts), v["soc"], v["soh"], v["temp"], v["volt"])
    return [
        ("event_id_uppercase", bmsg(str(uuid.UUID(int=rng.getrandbits(128), version=4)).upper(), vin, ts, 50.0, 95.0, 30.0, 400.0)),
        ("vin_18_chars", b(vin=vin + "0")),
        ("ts_at_2100_exclusive_bound", b(ts=contract.TS_MAX)),
        ("ts_before_2020", b(ts=contract.TS_MIN - 1)),
        ("soc_above_100", b(soc=100.01)),
        ("soc_negative", b(soc=-0.5)),
        ("soc_missing", b(soc=None)),
        ("soh_missing", b(soh=None)),
        ("soh_above_100", b(soh=100.5)),
        ("temp_above_120", b(temp=120.5)),
        ("temp_below_minus_60", b(temp=-60.01)),
        ("voltage_zero", b(volt=0.0)),
        ("voltage_above_1000", b(volt=1000.01)),
        ("voltage_missing", b(volt=None)),
    ]


def battery_conflict(rng, m):
    c = dict(m)
    c["soc_pct"] = round(min(100.0, m["soc_pct"] + rng.uniform(1, 20)), 2)
    return c


def invalid_twin_battery(m):
    c = dict(m)
    del c["soh_pct"]
    return c


# ---------------------------------------------------------------- shared injections

def copy_row(fx, m, arrival, intent, case, batch=None):
    return fx.add(dict(m), arrival, intent, case, batch)


def inject(fx, rng):
    charging_job = fx.job == "charging-sessions"
    conflict = charging_conflict if charging_job else battery_conflict
    twin = invalid_twin_charging if charging_job else invalid_twin_battery
    used = set()

    def pick(n, pred=lambda r: True, before=DAY0 + DAY - 4 * 60 * MIN):
        pool = [r for r in fx.base_rows(pred) if r["ord"] not in used and r["msg"]["ts"] < before
                and r["msg"]["ts"] > DAY0 + 60 * MIN]
        chosen = rng.sample(pool, n)
        used.update(r["ord"] for r in chosen)
        return chosen

    not_plug = (lambda r: r["msg"].get("event") != "PLUG_IN") if charging_job else (lambda r: True)
    progress = (lambda r: r["msg"].get("event") == "PROGRESS") if charging_job else (lambda r: True)

    # Late rows: fresh events that only ever arrive 75-180 min after their ts.
    for r in pick(12, not_plug):
        r["arrival"] = r["msg"]["ts"] + rng.randrange(75 * MIN, 180 * MIN)
        r["intent"], r["case"] = "late", "late_fresh_event"
        fx.cases["late_fresh_event"] += 1
    # Late first arrival, then an on-time copy (example 13): the copy is a first arrival.
    for r in pick(3, progress):
        r["arrival"] = r["msg"]["ts"] + rng.randrange(75 * MIN, 100 * MIN)
        r["intent"], r["case"] = "late", "late_first_arrival"
        fx.cases["late_first_arrival"] += 1
        arr = r["arrival"] + rng.randrange(35 * MIN, 60 * MIN)
        c = dict(r["msg"], ts=arr - 30_000)
        fx.add(c, arr, "accepted", "on_time_copy_after_late_first_arrival")
    # Late copies of IDs already seen (identical and conflicting payloads).
    for i, r in enumerate(pick(15)):
        m = r["msg"] if i % 3 else conflict(rng, r["msg"])
        copy_row(fx, m, r["msg"]["ts"] + rng.randrange(90 * MIN, 200 * MIN), "late",
                 "late_copy_of_seen_id")
    # Several late copies of one ID (probe 01), each counted late.
    r = pick(1)[0]
    for k in range(3):
        copy_row(fx, r["msg"], r["msg"]["ts"] + (70 + 40 * k) * MIN + rng.randrange(0, MIN), "late",
                 "late_copy_several_of_one_id")
    # Identical duplicates, on time.
    for r in pick(40):
        copy_row(fx, r["msg"], r["arrival"] + rng.randrange(1_000, 30_000), "duplicate", "identical_duplicate")
    # Conflicting duplicates, on time (first arrival's payload is kept).
    plugs = pick(5, lambda r: r["msg"].get("event") == "PLUG_IN") if charging_job else []
    for r in plugs + pick(15, not_plug):
        copy_row(fx, conflict(rng, r["msg"]), r["arrival"] + rng.randrange(1_000, 60_000), "conflicting",
                 "conflicting_duplicate_plug_in" if r in plugs else "conflicting_duplicate")
    # Invalid first, then a valid copy of the same event_id (example 11).
    twins = (pick(2, lambda r: r["msg"].get("event") == "PLUG_IN") if charging_job else []) + pick(6, not_plug)
    for r in twins:
        fx.add(twin(r["msg"]), r["arrival"] - rng.randrange(1_000, 10_000), "rejected", "invalid_then_valid_copy")
    # Standalone invalid messages, one per rule.
    make_invalid = charging_invalid if charging_job else battery_invalid
    hosts = rng.sample(fx.base_rows(), len(make_invalid(fx, random.Random(0), "TSTV0000000000001", DAY0)))
    for i, host in enumerate(hosts):
        reason, m = make_invalid(fx, rng, host["msg"]["vin"], host["msg"]["ts"])[i]
        fx.add(m, host["arrival"] + 1, "rejected", "invalid:" + reason)

    fx.assign_batches()

    # Conflicting copies with a later ts that moves the shared watermark (example 4).
    rows, _, _ = fx.classify()
    nbatches = rows[-1]["batch"]
    for k in (nbatches // 4, nbatches // 2, 3 * nbatches // 4):
        in_k = [r for r in rows if r["batch"] == k]
        max_ts = max(r["msg"]["ts"] for r in rows if r["batch"] <= k and r["intent"] != "rejected")
        orig = rng.choice([r for r in in_k if r["case"] is None and r["intent"] == "accepted" and progress(r)])
        c = dict(conflict(rng, orig["msg"]), ts=max_ts + WM_MOVE_LEAD[fx.job])
        row = fx.add(c, in_k[-1]["arrival"], "conflicting", "conflicting_copy_moves_watermark", batch=k)
        row["moves_watermark_of_batch"] = k
        rows = fx.ordered()

    rows, _, wm = fx.classify()
    by_batch = defaultdict(list)
    for r in rows:
        by_batch[r["batch"]].append(r)

    # Conflicting copy that arrives after its VIN's session closed (probe 05).
    if charging_job:
        last_unplug = {}
        for r in rows:
            if r["case"] is None and r["msg"]["event"] == "UNPLUG" and r["intent"] == "accepted":
                last_unplug[r["msg"]["vin"]] = r
        last_ts = defaultdict(int)
        for r in rows:
            if r["intent"] == "accepted":
                last_ts[r["msg"]["vin"]] = max(last_ts[r["msg"]["vin"]], r["msg"]["ts"])
        done = [u for v, u in sorted(last_unplug.items()) if u["msg"]["ts"] == last_ts[v]
                and u["msg"]["ts"] < DAY0 + DAY - 5 * 60 * MIN]
        for u in rng.sample(done, 4):
            vin = u["msg"]["vin"]
            orig = rng.choice([r for r in rows if r["msg"]["vin"] == vin and r["case"] is None
                               and r["msg"]["event"] == "PROGRESS" and r["intent"] == "accepted"])
            k = min(b for b, w in wm.items() if w is not None and w >= u["msg"]["ts"] + 30 * MIN)
            host = rng.choice([r for r in by_batch[k] if r["intent"] == "accepted" and r["msg"]["ts"] > wm[k] + MIN])
            c = dict(conflict(rng, orig["msg"]), ts=host["msg"]["ts"] - 1_000)
            fx.add(c, host["arrival"], "conflicting", "conflicting_copy_after_session_closed", batch=k)

    # A fresh row exactly at the watermark (late, example 8) and one 1 ms after it (on time).
    vin_pool = sorted({r["msg"]["vin"] for r in rows if r["intent"] == "accepted"})
    for k in (nbatches // 3, 2 * nbatches // 3):
        host = by_batch[k][len(by_batch[k]) // 2]
        for delta, intent, case in ((0, "late", "late_exactly_at_watermark"),
                                    (1, "accepted", "on_time_1ms_after_watermark")):
            vin = rng.choice(vin_pool)
            if charging_job:
                m = cmsg(fx.new_id(), vin, wm[k] + delta, "PROGRESS", 50)
            else:
                m = bmsg(fx.new_id(), vin, wm[k] + delta, 60.0, 90.0, 30.0, 390.0)
            fx.add(m, host["arrival"], intent, case, batch=k)


def check_and_write(fx, out_dir, pb_type):
    from google.protobuf import json_format

    rows, classes, wm = fx.classify()
    errors = []
    for r, c in zip(rows, classes):
        if c != r["intent"]:
            errors.append(f"{fx.job}: {r['case'] or 'base'} row {r['msg']['event_id']} batch {r['batch']} "
                          f"intended {r['intent']} but is {c}")
    for r in rows:
        if r.get("moves_watermark_of_batch"):
            k = r["moves_watermark_of_batch"]
            if wm.get(k + 1) != r["msg"]["ts"] - contract.DELAY_MS[fx.job]:
                errors.append(f"{fx.job}: copy in batch {k} did not set the watermark (got {wm.get(k + 1)})")
    owner = {}
    for r in rows:
        eid, vin = r["msg"].get("event_id"), r["msg"].get("vin")
        if owner.setdefault(eid, vin) != vin:
            errors.append(f"{fx.job}: event_id {eid} appears under {owner[eid]} and {vin}")
        if vin == contract.RESERVED_VIN:
            errors.append(f"{fx.job}: reserved VIN used")
        json_format.ParseDict(r["msg"], pb_type(), ignore_unknown_fields=False)
    batches = [r["batch"] for r in rows]
    if batches[0] != 1 or any(b - a not in (0, 1) for a, b in zip(batches, batches[1:])):
        errors.append(f"{fx.job}: batches are not contiguous from 1")
    if errors:
        raise SystemExit("\n".join(errors))

    valid_ts = [r["msg"]["ts"] for r, c in zip(rows, classes) if c != "rejected"]
    flush = {"advance_watermark_to": max(valid_ts) + 3_600_000, "batch": batches[-1] + 1}
    lines = [json.dumps({"arrival_seq": i + 1, "batch": r["batch"], "message": r["msg"]},
                        separators=(",", ":"), ensure_ascii=False) for i, r in enumerate(rows)]
    lines.append(json.dumps(flush, separators=(",", ":")))
    data = ("\n".join(lines) + "\n").encode("utf-8")
    if len(data) >= MAX_FIXTURE_BYTES:
        raise SystemExit(f"{fx.job}: fixture is {len(data)} bytes, limit {MAX_FIXTURE_BYTES}")
    path = Path(out_dir) / f"{fx.job}.jsonl"
    path.write_bytes(data)
    return {
        "path": f"parity/replay/{fx.job}.jsonl",
        "sha256": hashlib.sha256(data).hexdigest(),
        "bytes": len(data),
        "event_lines": len(rows),
        "batches": batches[-1],
        "flush": flush,
        "vins": len({r["msg"]["vin"] for r in rows if contract.VIN_RE.match(r["msg"]["vin"])}),
        "rows_by_contract_class": dict(sorted(Counter(classes).items())),
        "injected_cases": dict(sorted(fx.cases.items())),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(REPO / "parity" / "replay"))
    args = ap.parse_args()
    import vehicle_battery_v1_pb2
    import vehicle_charging_v1_pb2
    import google.protobuf

    Path(args.out).mkdir(parents=True, exist_ok=True)
    meta = {
        "seed": SEED,
        "python_version": platform.python_version(),
        "protobuf_version": google.protobuf.__version__,
        "choices": {
            "vehicles": VEHICLES,
            "simulated_day_utc": "2026-09-22",
            "batch_interval_ms": BATCH_MS,
            "network_jitter_ms": {k: {"usual_max": a, "straggler_max": b} for k, (a, b) in JITTER.items()},
            "battery_reading_interval_ms": MIN,
        },
        "fixtures": {},
    }
    ch, ch_extra = charging(random.Random(SEED * 10 + 1))
    meta["fixtures"]["charging-sessions"] = {**check_and_write(ch, args.out, vehicle_charging_v1_pb2.ChargingEvent), **ch_extra}
    bt, bt_extra = battery(random.Random(SEED * 10 + 2))
    meta["fixtures"]["battery-health"] = {**check_and_write(bt, args.out, vehicle_battery_v1_pb2.BatteryReading), **bt_extra}
    owners = defaultdict(set)
    for r in ch.rows + bt.rows:
        owners[r["msg"]["event_id"]].add(r["msg"]["vin"])
    assert all(len(v) == 1 for v in owners.values()), "event_id under two VINs across fixtures"
    (Path(args.out) / "generator-run.json").write_text(json.dumps(meta, indent=2) + "\n")
    print_summary(meta)


def print_summary(meta):
    print(f"seed {meta['seed']}, Python {meta['python_version']}, protobuf {meta['protobuf_version']}")
    for job, f in meta["fixtures"].items():
        print(f"\n## {job}: {f['event_lines']} events, {f['vins']} VINs, {f['batches']} batches + flush, "
              f"{f['bytes']:,} bytes")
        print("| Case | Count |\n| --- | --- |")
        for group in ("injected_cases", "sessions_by_scenario", "plug_in_coordinates", "readings",
                      "rows_by_contract_class"):
            for k, v in f.get(group, {}).items():
                print(f"| {group}: {k} | {v} |")


if __name__ == "__main__":
    main()
