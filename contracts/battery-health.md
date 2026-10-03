# Behavior contract: `battery-health`

Restated from `SPEC.md` ("Behavior contract and replay", "`battery-health`").
The spec is the source of truth; this file adds no rules. The legacy Scala job
and the Rust reference job are both built against it.

## Resolved questions

Answered in the stage 2 review and recorded in `SPEC.md` ("Resolved questions").
No open questions remain for this job.

| # | Question | Answer |
| --- | --- | --- |
| 1 | Output topic | `battery.health.v1` (`proto/battery_health_v1.proto`). |
| 2 | Counters | Exactly `rejected`, `late`, `duplicate_events`, `conflicting_duplicates`. |
| 3 | Is `ts` `optional`? | No, plain `int64`; `optional` applies to the four readings only. |
| 4 | Empty windows | Never emitted. |
| 5 | `avg_soc_pct` arithmetic | Double sum ÷ count; the 1e-9 tolerance covers it. |
| 6 | Watermark before any valid `ts` | Unset, as for charging; nothing in batch 1 is ever late. |
| 7 | Window emission | `watermark ≥ window_end`. |
| 8 | Window alignment | Aligned to the Unix epoch (`window_start = ts − ts mod 300,000`). |

## Input: `vehicle.battery.v1`

Protobuf `vehicle.battery.v1.BatteryReading` (`proto/vehicle_battery_v1.proto`),
keyed by VIN.

| Field | Meaning |
| --- | --- |
| `event_id` | Globally unique. Retransmissions reuse the same ID. |
| `vin` | Synthetic vehicle ID and the Kafka message key |
| `ts` | When the reading was taken: `int64` epoch milliseconds, UTC |
| `soc_pct`, `soh_pct`, `cell_temp_max_c`, `pack_voltage_v` | proto3 `optional` doubles; all required |

Sensitive field: `vin`.

## Validation

A message that fails any rule is counted `rejected` and doesn't reserve its
`event_id`. Validation never looks at duplication.

- `event_id`: canonical lowercase UUID text, 36 characters.
- `vin`: 17 characters from `[A-Z0-9]`.
- `ts`: from 1,577,836,800,000 (2020-01-01 00:00 UTC) up to, not including,
  4,102,444,800,000 (2100-01-01 00:00 UTC).
- Every numeric reading is proto3 `optional` and required (missing ≠ zero):
  `soc_pct` and `soh_pct` in [0, 100]; `cell_temp_max_c` in [−60, 120];
  `pack_voltage_v` above 0 and at most 1,000; all finite.

## Windows

| Question | Decision |
| --- | --- |
| Window | Tumbling 5 minutes by event time: [start, start + 5 min) |
| Validation, lateness, duplicates | Same steps as charging, with a **2-minute** watermark delay; duplicates dropped per VIN |
| When a window is emitted | Once the watermark reaches the window end; emitted windows are final |
| Alert | `max_cell_temp_c` strictly above 55.0 |
| Output | `output_id`, `vin`, `window_start`, `avg_soc_pct`, `min_soh_pct`, `max_cell_temp_c`, `alert`, `event_count` |
| Comparison | Averages within 1e-9 absolute; every other field exact |

`window_start` = `ts − (ts mod 300,000)` in epoch milliseconds (the 5-minute
boundary at or before `ts`).

## Event time, lateness and duplicates

Identical to the charging contract's steps, with the delay changed:

1. Take input in the fixed batches of the replay fixture. The watermark from the
   previous batch stays fixed for the whole batch.
2. Validate each message.
3. Every valid message's `ts` counts toward the running maximum, duplicates included.
4. Drop any valid message with `ts` at or before the previous batch's watermark
   and count it `late`. Late rows never reach deduplication, so their IDs aren't
   remembered.
5. Deduplicate the rest per VIN. Sort by `arrival_seq` first; first arrival wins.
6. Buffer accepted readings.
7. At the end of the batch, set the watermark to the greater of its previous
   value and the largest valid `ts` seen so far minus **2 minutes**.
8. Process buffered readings at or before the new watermark, sorted by
   (`ts`, `arrival_seq`), into their windows.
9. Emit every window whose end the watermark has reached; emitted windows are final.

One shared watermark per job. The duplicate table is the charging contract's:

| Duplicate situation | Behavior |
| --- | --- |
| Same `event_id`, same decoded fields | Ignore; count `duplicate_events` |
| Same `event_id`, different decoded fields | Keep the first arrival's payload; count `conflicting_duplicates`. The later copy's `ts` still counts toward the watermark. |
| Invalid message arrives first | Rejected; the ID isn't reserved, so a later valid copy is processed |
| Any copy at or behind the watermark | Counted `late`, never as a duplicate |
| First valid arrival was late | Counted `late`, and its ID isn't remembered. A later on-time copy is processed as a first arrival. |

## Output

Protobuf `battery.health.v1.BatteryWindow` (`proto/battery_health_v1.proto`).
One final record per (VIN, window).

| Field | Meaning |
| --- | --- |
| `output_id` | See the encoding below |
| `vin` | Vehicle ID |
| `window_start` | Epoch milliseconds, UTC |
| `avg_soc_pct` | Mean of accepted `soc_pct` in the window |
| `min_soh_pct` | Minimum accepted `soh_pct` |
| `max_cell_temp_c` | Maximum accepted `cell_temp_max_c` |
| `alert` | `max_cell_temp_c > 55.0` (strict) |
| `event_count` | Accepted readings in the window |

### `output_id`

Lowercase hex SHA-256, 64 characters, over the UTF-8 text
`battery-health|<vin>|<window_start>`, with `window_start` as a base-10
epoch-millisecond integer, no padding. For VIN `TST00000000000001` and the window
at 2026-09-21 14:15:00 UTC the text is
`battery-health|TST00000000000001|1790000100000`, which gives
`924bbe50976ec7a530dd47f70c1c42f19bb46342e4e320a06a3e076bb70feda8`.

## Counters (per run)

`rejected`, `late`, `duplicate_events`, `conflicting_duplicates` (see open
question 2). File format in `parity/replay/FORMAT.md`.

## Replay and flush

As for charging (`parity/replay/FORMAT.md`), with the battery delay: T is the
largest *valid* fixture `ts` plus 1 hour (rejected rows never count); one valid control reading on the reserved VIN
`TSTZZZZZZZZZZZZZZ` with `ts = T + 2 min`, then one empty batch, putting the
watermark at exactly T. The reserved VIN is left out of outputs and counters.

Drain condition, checked rather than assumed: after the flush, no VIN other than the reserved one has an open session (charging), an open window (battery), or buffered events. Seen IDs may remain; they're kept for the whole run.

- Scala: every state-function call emits a status marker `{vin, batch, busy}`, where `busy` means the VIN has an open session or window or buffered events. Every VIN's last marker must be `busy = false`, except the reserved VIN's, which must be `busy = true` (its control event is still buffered). Cross-check: the stateful operator's `numRowsTotal` in the last progress equals the number of distinct VINs that reached state (valid, non-late rows) plus one for the reserved VIN.
- Rust: inspect the state map directly with the same rule.
- If the check fails, parity refuses to compare.

## Acceptance examples

Examples 15 and 16 of the spec's table live in `parity/examples/`. Expected
values come from the spec's table, never from running either implementation.
