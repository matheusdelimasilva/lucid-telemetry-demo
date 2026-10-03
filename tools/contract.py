"""Contract helpers shared by the generator and the privacy check.

Only the parts of the behavior contract a fixture checker needs: validation,
the per-batch watermark (steps 3, 4 and 7) and first-arrival deduplication
(step 5). No session or window logic: outputs come from the jobs, never from here.
"""

import hashlib
import json
import math
import re
from decimal import ROUND_HALF_UP, Decimal

TS_MIN = 1_577_836_800_000
TS_MAX = 4_102_444_800_000  # exclusive
RESERVED_VIN = "TSTZZZZZZZZZZZZZZ"
EVENT_ID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
VIN_RE = re.compile(r"^[A-Z0-9]{17}$")
EVENTS = {"PLUG_IN", "START", "PROGRESS", "STOP", "UNPLUG"}

DELAY_MS = {"charging-sessions": 600_000, "battery-health": 120_000}
INPUT_FIELDS = {
    "charging-sessions": ("event_id", "vin", "ts", "event", "energy_wh", "lat", "lon", "charger_type"),
    "battery-health": ("event_id", "vin", "ts", "soc_pct", "soh_pct", "cell_temp_max_c", "pack_voltage_v"),
}
# Non-optional fields decode to their proto3 default when absent.
DEFAULTS = {"event_id": "", "vin": "", "ts": 0, "event": "EVENT_UNSPECIFIED", "energy_wh": 0}


def decoded(job, msg):
    """Decoded field tuple; unset optional fields are None. Conflicts compare these."""
    return tuple(msg.get(f, DEFAULTS.get(f)) for f in INPUT_FIELDS[job])


def _finite_in(v, lo, hi):
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v) and lo <= v <= hi


def valid(job, msg):
    m = dict(zip(INPUT_FIELDS[job], decoded(job, msg)))
    if not (EVENT_ID_RE.match(m["event_id"]) and VIN_RE.match(m["vin"]) and TS_MIN <= m["ts"] < TS_MAX):
        return False
    if job == "charging-sessions":
        ev, e = m["event"], m["energy_wh"]
        if ev not in EVENTS or e < 0 or (e != 0 and ev not in ("PROGRESS", "STOP")):
            return False
        if ev == "PLUG_IN":
            return (_finite_in(m["lat"], -90, 90) and _finite_in(m["lon"], -180, 180)
                    and isinstance(m["charger_type"], str) and m["charger_type"] != "")
        return True
    v = m["pack_voltage_v"]
    return (_finite_in(m["soc_pct"], 0, 100) and _finite_in(m["soh_pct"], 0, 100)
            and _finite_in(m["cell_temp_max_c"], -60, 120)
            and _finite_in(v, -math.inf, 1000) and v > 0)


def read_fixture(path):
    """Returns (event lines, flush line) as parsed JSON."""
    with open(path, encoding="utf-8") as f:
        lines = [json.loads(line) for line in f]
    return lines[:-1], lines[-1]


def classify(job, events):
    """Per-row class under contract steps 2-5 and 7, plus the watermark per batch.

    Returns (classes, watermark_in_effect) where classes[i] is one of
    "rejected", "late", "accepted", "duplicate", "conflicting" for events[i], and
    watermark_in_effect maps batch -> watermark the batch ran with (None = unset).
    """
    delay = DELAY_MS[job]
    classes = []
    wm_in_effect = {}
    wm = None
    max_ts = None
    seen = {}  # (vin, event_id) -> decoded tuple
    current = None
    for row in events:
        b = row["batch"]
        if b != current:
            if current is not None and max_ts is not None:
                wm = max(wm, max_ts - delay) if wm is not None else max_ts - delay
            current = b
            wm_in_effect[b] = wm
        msg = row["message"]
        if not valid(job, msg):
            classes.append("rejected")
            continue
        max_ts = msg["ts"] if max_ts is None else max(max_ts, msg["ts"])
        if wm is not None and msg["ts"] <= wm:
            classes.append("late")
            continue
        key = (msg["vin"], msg["event_id"])
        d = decoded(job, msg)
        if key not in seen:
            seen[key] = d
            classes.append("accepted")
        else:
            classes.append("duplicate" if seen[key] == d else "conflicting")
    return classes, wm_in_effect


def round3(x):
    """Contract rounding: shortest decimal form, 3 decimals, exact halves away from zero."""
    return float(Decimal(repr(float(x))).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP))


def sha256_hex(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
