# Behavior contract: `charging-sessions`

Restated from `SPEC.md` ("Behavior contract and replay"). The spec is the source
of truth; this file adds no rules. Both the legacy Scala job and the Rust port
are built against it. These are rules for the synthetic system, not claims about
how any manufacturer handles charging.

## Resolved questions

Answered in the stage 2 review and recorded in `SPEC.md` ("Resolved questions").
No open questions remain for this job.

| # | Question | Answer |
| --- | --- | --- |
| 1 | Rejected `ts` and the watermark | Never moves it. Validation comes first. |
| 2 | Flush T | Largest valid `ts` + 1 hour; rejected rows never count. |
| 3 | Gap check (step 8 vs. step 9) | Both apply, strict `>` 30 min; exactly 30 minutes is the same session. |
| 4 | `start` while a session is open | Accepted, carries no energy, resets the 30-minute gap timer. |
| 5 | `end_ts` when a session times out after `stop` | The `stop`'s `ts`. |
| 6 | `energy_wh = 0` on `plug_in`/`start`/`unplug` | Adds nothing. |
| 7 | Duplicate copy differing only in `lat`/`lon`/`charger_type` on a non-`plug_in` event | Counts as `conflicting_duplicates` (all decoded fields compare). |
| 8 | `plug_in` with the same `event_id` as the opening `plug_in` | Plain duplicate; no special case. |
| 9 | Batch number of the flush's empty batch | As `parity/replay/FORMAT.md` says: control event in batch N, empty batch N+1. |
| 10 | Battery output topic | `battery.health.v1` (see the battery contract). |
| 11 | `close_reason` encoding | `CloseReason` enum on the wire; counters keyed by the lowercase words. |
| 12 | Watermark before any valid `ts` | Unset. It's only set at the end of a batch, so nothing in batch 1 is ever late. |

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

Drain condition, checked rather than assumed: after the flush, no VIN other than the reserved one has an open session (charging), an open window (battery), or buffered events. Seen IDs may remain; they're kept for the whole run.

- Scala: every state-function call emits a status marker `{vin, batch, busy}`, where `busy` means the VIN has an open session or window or buffered events. Every VIN's last marker must be `busy = false`, except the reserved VIN's, which must be `busy = true` (its control event is still buffered). Cross-check: the stateful operator's `numRowsTotal` in the last progress equals the number of distinct VINs that reached state (valid, non-late rows) plus one for the reserved VIN.
- Rust: inspect the state map directly with the same rule.
- If the check fails, parity refuses to compare.

## Acceptance examples

Examples 1–14 of the spec's table live in `parity/examples/`, one folder each.
Expected values come from the spec's table, never from running either
implementation.
