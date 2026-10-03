# Replay fixture format

A replay fixture is one job's input in arrival order, split into fixed batches,
plus the standard flush. Both the Spark harness (`MemoryStream` +
`processAllAvailable()` per batch) and the Rust replay runner consume the same
file. Fixtures live in `parity/replay/<job>.jsonl`; acceptance examples use the
same format in `parity/examples/<nn>-<slug>/input.jsonl`.

Everything is UTF-8 JSON Lines: one JSON object per line, `\n` terminated, no
blank lines, no comments. Integers are JSON numbers (all values fit in 2^53).

## Event lines

```json
{"arrival_seq": 1, "batch": 1, "message": {"event_id": "00000000-0000-4000-8000-000000000101", "vin": "TST00000000000001", "ts": 1789984800000, "event": "PLUG_IN", "energy_wh": 0, "lat": 37.4, "lon": -122.1, "charger_type": "dc_fast"}}
```

| Key | Type | Rules |
| --- | --- | --- |
| `arrival_seq` | integer ≥ 1 | Arrival order. Strictly increasing down the file, starting at 1, no gaps. Plays the role of the Kafka offset (contract step 5). |
| `batch` | integer ≥ 1 | Replay batch the event is delivered in. Non-decreasing down the file, starting at 1, no gaps: batch *n+1* may start only after batch *n* has at least one line. |
| `message` | object | The Protobuf message as JSON (below). Exactly one input message type per fixture: `vehicle.charging.v1.ChargingEvent` for `charging-sessions`, `vehicle.battery.v1.BatteryReading` for `battery-health`. |

No other keys are allowed on an event line.

### `message` encoding

The message object is the Protobuf message in JSON, with these fixed choices
(they are a subset of the proto3 JSON mapping, chosen so the file is
unambiguous and easy to read):

- Keys are the `.proto` field names exactly (`snake_case`), never lowerCamelCase.
- `int64` fields (`ts`, `energy_wh`) are JSON numbers, not strings.
- `double` fields are JSON numbers written in their shortest decimal form
  (`-33.8675`, `0.0005`, `37.4`). Non-finite values (`NaN`, `Infinity`) are not
  representable and are out of scope for fixtures.
- Enum fields use the enum value **name** as a string (`"PLUG_IN"`,
  `"EVENT_UNSPECIFIED"`), never the integer.
- A proto3 `optional` field that is **unset** is **absent** from the object.
  `null` is not allowed. A present key means the field is set (its presence
  bit is on), even if its value is `0`.
- A non-`optional` field may be omitted, meaning its proto3 default (`""`, `0`,
  enum 0). Fixtures should still write every non-`optional` field explicitly.
- No unknown keys.

A message object is valid for the fixture when it converts to the Protobuf
message without error (`json_format.ParseDict` in Python with
`ignore_unknown_fields=False`, or the equivalent). **That is the only
requirement on a message.** Whether the decoded message passes the job's
*contract* validation (`contracts/<job>.md`) is the job's business: invalid
messages in a fixture are still decodable Protobuf that fails contract
validation (bad UUID text, `EVENT_UNSPECIFIED`, negative `energy_wh`, missing
`lat` on a `PLUG_IN`, out-of-range `ts`, ...). Undecodable bytes are out of
scope for this repo.

## Flush line

Every fixture ends with exactly one flush line, and it is the last line:

```json
{"advance_watermark_to": 1789992000000, "batch": 3}
```

| Key | Type | Rules |
| --- | --- | --- |
| `advance_watermark_to` | integer | The target watermark T in epoch milliseconds: the largest `ts` of any event line that passes the job's contract validation, plus 1 hour (3,600,000 ms). Rejected rows never count, late or duplicate copies do (they are valid). Stated explicitly so a reader doesn't have to derive it; `check_examples.py` only checks that T − 1 h is the `ts` of some event line. |
| `batch` | integer | The batch of the synthesized control event: last event batch + 1. |

Meaning, per the spec's replay section: the runner synthesizes one valid control
event on the reserved VIN `TSTZZZZZZZZZZZZZZ` with `ts = T + <job delay>`
(charging: T + 10 min, a `PLUG_IN` with valid `lat`, `lon`, `charger_type`;
battery: T + 2 min, a valid reading), delivers it as batch
`advance_watermark_to.batch`, then runs one more empty batch (`batch + 1`),
which in Spark is the no-data batch that runs when the watermark moves. The
result is a watermark of exactly T. The reserved VIN is excluded from outputs and
counters. The control event's `event_id` and reading values are the runner's
choice; they never appear in outputs.

A fixture with no valid event lines is invalid (T is undefined).

## Counters file

`parity/replay/<job>.counters.json`, `parity/golden/<job>.counters.json` and the
`counters` object in `parity/examples/*/expected.json` share one shape: a single
JSON object, every key present, every value a non-negative integer.

`charging-sessions`:

```json
{
  "rejected": 0,
  "late": 0,
  "duplicate_events": 0,
  "conflicting_duplicates": 0,
  "orphan": 0,
  "sessions_by_close_reason": {
    "unplug": 0,
    "inactivity_timeout": 0,
    "replaced_by_plug_in": 0
  }
}
```

`battery-health` (no session state, so no `orphan` and no close reasons; see the
contract's open questions):

```json
{
  "rejected": 0,
  "late": 0,
  "duplicate_events": 0,
  "conflicting_duplicates": 0
}
```

Counter meanings are the contract's: `late` counts every row dropped at step 4,
copies included; the reserved flush VIN is never counted.

## Acceptance example folders

`parity/examples/<nn>-<slug>/` holds `input.jsonl` (this format) and
`expected.json`:

```json
{
  "job": "charging-sessions",
  "records": [ { ...output message as JSON, same encoding rules as above... } ],
  "counters": { ...counters object for the job... },
  "watermarks": { "1": 1789984500000 }
}
```

- `job`: `charging-sessions` or `battery-health`.
- `records`: the final output records (`charging.sessions.v1.ChargingSession` or
  `battery.health.v1.BatteryWindow`) as JSON, in any order; compared as a set by
  `output_id`. Every field of the output message is present.
- `counters`: as above.
- `watermarks`: optional. Keys are batch numbers (as strings), values the
  watermark in epoch milliseconds *after* that batch. Only present where the
  spec's table states a watermark; absent otherwise.

Fields the spec's table leaves implicit (`start_ts`, `duration_ms`,
`start_lat`/`start_lon`, `charger_type`, ...) are derived by hand from the rules
and the chosen input values, never from running an implementation.
