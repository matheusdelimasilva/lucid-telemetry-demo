# Behavior contract: `charging-sessions`

Restated from `SPEC.md` ("Behavior contract and replay"). The spec is the source
of truth; this file adds no rules. Both the legacy Scala job and the Rust port
are built against it. These are rules for the synthetic system, not claims about
how any manufacturer handles charging.

## Open questions

Things the spec leaves open. Nothing below is decided here; the answers go into
the spec (and then this file) before the baseline is frozen.

1. **Rejected messages and the watermark.** Step 3 says every *valid* message's
   `ts` counts toward the running maximum. So a rejected message's `ts` never
   moves the watermark, even if it's the largest `ts` seen. Confirm this is the
   intent (example 11's rejected row has a smaller `ts`, so it doesn't discriminate).
2. **Gap rule vs. step 9.** The 30-minute gap is checked in two places: at
   processing time (step 8: "more than 30 minutes between consecutive accepted
   session events" closes the session before the new event is handled) and at
   the end of the batch (step 9: "last event plus 30 minutes strictly earlier
   than the watermark"). Example 6 needs the step-8 reading (the 10:30:00.001
   `progress` becomes an `orphan`). Confirm both apply, and that in step 8 the
   comparison is `new.ts − last.ts > 30 min` (strict), so exactly 30 minutes is
   the same session (example 5).
3. **`start` with no energy after `plug_in`.** `start` is listed only under
   "`start` or `progress` after `stop`". A `start` while a session is open and
   not stopped is presumably an accepted session event that carries no energy
   and refreshes the gap clock. Confirm.
4. **`stop` as the last event and `end_ts`.** For a session that times out after
   a `stop`, `end_ts` is the `stop`'s `ts` (the last accepted event). Confirm.
5. **`energy_wh` on `plug_in`, `start`, `unplug`.** Validation says nonzero is
   invalid there. It doesn't say what `energy_wh = 0` on those events means;
   we read it as "no energy report" (adds nothing). Confirm.
6. **`lat`/`lon`/`charger_type` set on non-`plug_in` events.** "Optional and
   ignored" — we read it as: their presence or values never affect validation,
   duplicates or output. But "conflicting duplicate" compares *decoded fields*;
   does a second copy that differs only in an ignored field count as
   `conflicting_duplicates`? We read yes (all decoded fields compare).
7. **Duplicate copies of an accepted `plug_in` that opens a new session.** If a
   second `plug_in` with a *new* `event_id` arrives while a session is open, it
   replaces (rule table). If it has the *same* `event_id` as the opening
   `plug_in`, it's a duplicate (ignored). Confirm no special case.
8. **Batch of the flush's empty batch.** The flush is a control event plus "one
   empty batch". Whether that empty batch appears in traces as its own batch
   number is a stage 3 detail; `parity/replay/FORMAT.md` makes it explicit.
9. **Output topic name for battery.** Not a charging question, but the same
    ambiguity list: the spec names `charging.sessions.v1` but never names the
    battery output topic. `battery.health.v1` is used as a placeholder.
10. **`close_reason` wire encoding.** The spec gives lowercase words
    (`unplug`, ...). The proto uses an enum (`UNPLUG`, ...). Counters keyed by
    the lowercase words. Confirm the enum is acceptable for the wire format.
11. **Watermark when no valid `ts` has been seen yet.** Before any valid message,
    "the largest valid `ts` seen so far minus 10 minutes" is undefined. We read
    the watermark as "unset / negative infinity" (nothing is late) until the
    first valid message. Confirm.

## Input: `vehicle.charging.v1`

Protobuf `vehicle.charging.v1.ChargingEvent` (`proto/vehicle_charging_v1.proto`),
keyed by VIN.

| Field | Meaning |
| --- | --- |
| `event_id` | Globally unique. Retransmissions reuse the same ID. |
| `vin` | Synthetic vehicle ID and the Kafka message key |
| `ts` | When the event happened: `int64` epoch milliseconds, UTC |
| `event` | Exactly one of `plug_in`, `start`, `progress`, `stop`, `unplug` (enum `PLUG_IN` ... `UNPLUG`) |
| `energy_wh` | `int64` watt-hours delivered since the previous energy report, not a running total. Nonzero only on `progress` and `stop`. |
| `lat`, `lon` | Required on `plug_in`; optional and ignored on other events |
| `charger_type` | Taken from `plug_in` and kept for the session |

Integer watt-hours avoid rounding differences when Scala and Rust add values;
display kWh as `energy_wh / 1000`.

## Validation

A message that fails any rule is counted `rejected` and doesn't reserve its
`event_id`. Validation never looks at duplication.

- `event_id`: canonical lowercase UUID text, 36 characters.
- `vin`: 17 characters from `[A-Z0-9]`.
- `ts`: from 1,577,836,800,000 (2020-01-01 00:00 UTC) up to, not including,
  4,102,444,800,000 (2100-01-01 00:00 UTC).
- `event`: one of the five values. The Protobuf default, `EVENT_UNSPECIFIED`, is invalid.
- `energy_wh`: zero or more, and nonzero only on `progress` and `stop`.
- `lat`, `lon`, `charger_type` are proto3 `optional`, so "missing" differs from
  zero. On `plug_in` all three are required: `lat` finite in [−90, 90], `lon`
  finite in [−180, 180], `charger_type` non-empty.

## Session rules

| Situation | Behavior |
| --- | --- |
| `plug_in`, no open session | Start a session |
| Any other event, no open session | Count as `orphan`; don't invent a session |
| `progress` | Add its energy |
| `stop` | Add any final energy; keep the session open until `unplug` or timeout |
| `start` or `progress` after `stop` | Resumed charging in the same session, subject to the gap rule |
| `unplug` | Close with `close_reason = unplug` |
| Another `plug_in` while one is open | Close the old one with `replaced_by_plug_in`, then start a new one |
| More than 30 minutes between consecutive accepted session events | Close with `inactivity_timeout`; the next event starts a session only if it's `plug_in` |
| Exactly 30 minutes | Same session |
| Session crosses midnight | Nothing special happens |

Duration is connected time: `plug_in` to the end. `end_ts` is the `unplug` time
or, for an incomplete session, the last accepted event. It never includes the
30-minute wait.

## Event time, lateness and duplicates

Arrival order decides which duplicate wins; event time decides how a session
unfolds. Keeping the first payload and advancing time are separate: a later copy
never replaces the original event's energy or session details, but its timestamp
still moves the watermark.

The lateness check comes before deduplication, to match Spark: rows at or behind
the watermark are filtered out before the state function sees them, so a late
row can't be remembered by ID.

Each batch runs these steps:

1. Take input in the fixed batches of the replay fixture. The watermark from the
   previous batch stays fixed for the whole batch.
2. Validate each message.
3. Every valid message's `ts` counts toward the running maximum, duplicates included.
4. Drop any valid message with `ts` at or before the previous batch's watermark
   and count it `late`. Late rows never reach deduplication, so their IDs aren't
   remembered.
5. Deduplicate the rest per VIN. Sort by `arrival_seq` first (the Kafka offset in
   the Kafka path, the fixture's sequence number in replay); first arrival wins.
6. Buffer accepted events.
7. At the end of the batch, set the watermark to the greater of its previous
   value and the largest valid `ts` seen so far minus **10 minutes**.
8. Process buffered events at or before the new watermark, sorted by
   (`ts`, `arrival_seq`).
9. Close sessions whose last event plus 30 minutes is strictly earlier than the
   watermark.

There's one shared watermark per job. A newer timestamp on one VIN can therefore
make another VIN's older events late. That's documented behavior, not a bug.

| Duplicate situation | Behavior |
| --- | --- |
| Same `event_id`, same decoded fields | Ignore; count `duplicate_events` |
| Same `event_id`, different decoded fields | Keep the first arrival's payload; count `conflicting_duplicates`. The later copy's `ts` still counts toward the watermark. |
| Invalid message arrives first | Rejected; the ID isn't reserved, so a later valid copy is processed |
| Any copy at or behind the watermark | Counted `late`, never as a duplicate |
| First valid arrival was late | Counted `late`, and its ID isn't remembered. A later copy that's on time is processed as a first arrival (example 13). |

Deduplication is scoped per VIN, the same key as session state. An `event_id`
never appears under two VINs; the generator asserts this, and cross-VIN conflicts
are out of scope. Seen IDs are kept for the whole bounded run. Conflicts compare
decoded fields, not raw Protobuf bytes.

## Output: `charging.sessions.v1`

Protobuf `charging.sessions.v1.ChargingSession` (`proto/charging_sessions_v1.proto`).
One final record per session, emitted once the rules allow it to close. No
intermediate updates.

| Field | Meaning |
| --- | --- |
| `output_id` | See the encoding below |
| `vin` | Vehicle ID |
| `start_ts`, `end_ts` | Session boundaries |
| `duration_ms` | `end_ts − start_ts` |
| `total_energy_wh` | Sum of accepted increments |
| `start_lat`, `start_lon` | `plug_in` coordinates rounded to 3 decimals, exact halves away from zero, negatives included |
| `charger_type` | From the opening `plug_in` |
| `close_reason` | `unplug`, `inactivity_timeout` or `replaced_by_plug_in` |

Original coordinates are never emitted or logged. VIN stays, so the output is
still identifiable; rounding doesn't make it anonymous. Rounding runs on the
shortest decimal form: Scala `BigDecimal(x).setScale(3, HALF_UP)`, Rust a
decimal crate, never float math. Records are compared by `output_id`; no order
across vehicles is required.

### `output_id`

Lowercase hex SHA-256, 64 characters, over the UTF-8 text of the opening
`plug_in`'s `event_id`.
`00000000-0000-4000-8000-000000000001` →
`11e594f481958c10e3015d0bf0447a22f068a8a647f475df15ce2c7ab4b8f3f1`.

## Counters (per run)

`rejected`, `late` (every row dropped at step 4, copies included),
`duplicate_events`, `conflicting_duplicates`, `orphan`, and sessions by
`close_reason` (`unplug`, `inactivity_timeout`, `replaced_by_plug_in`). File
format in `parity/replay/FORMAT.md`.

## Replay and flush

Fixtures list events in arrival order with `arrival_seq` and batch number
(`parity/replay/FORMAT.md`). Every fixture ends the same way: the target
watermark T is the largest *valid* fixture `ts` plus 1 hour (rejected rows never count); one control `plug_in` on
the reserved VIN `TSTZZZZZZZZZZZZZZ` with `ts = T + 10 min`, then one empty
batch. That puts the watermark at exactly T. The reserved VIN is left out of
outputs and counters.

Drain condition, checked rather than assumed: after the flush, the only state
left is the reserved VIN's buffered event. If the check fails, parity refuses to
compare.

## Acceptance examples

Examples 1–14 of the spec's table live in `parity/examples/`, one folder each.
Expected values come from the spec's table, never from running either
implementation.
