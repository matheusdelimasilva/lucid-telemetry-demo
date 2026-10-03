# lucid-telemetry-demo: build spec
 
This is the source of truth for building this repo. Work one stage at a time (see "Stages" at the end) and stop at each stage's exit condition.
 
## Scenario
 
This repo supports a demo for Lucid Motors' engineering leadership. They shared this use case:
 
> **Vehicle telemetry pipeline modernization.** Re-platforming the ingestion path for vehicle-to-cloud telemetry, such as battery health, charging sessions, diagnostics, and over-the-air update status, from aging Scala/Spark Streaming jobs onto purpose-built Rust stream processors running on Kubernetes. The pipeline handles very high event volumes through Kafka and feeds service, warranty, and OTA systems. Ordering and exactly-once guarantees must hold, and the data includes location tied to a VIN. The Rust service skeleton and deployment pipeline are in place, but only one small job has been ported. The team wants to move the remaining jobs without disrupting downstream consumers or triggering a lengthy privacy review for each one.
 
## What this repo is
 
An illustrative environment built from the scenario above, not a copy of Lucid's system. All data is synthetic. It exists to port one Scala Structured Streaming job to Rust against a frozen Scala baseline and to produce reviewable evidence that the port is correct.
 
**In scope**
 
- Two jobs: `battery-health` (legacy Scala plus an already-ported Rust reference) and `charging-sessions` (legacy Scala only; its Rust port happens later, in a separate session).
- Protobuf on Kafka (Redpanda), a deterministic replay harness, parity checks, privacy regression checks.
- One input partition and one worker per job. This is a deliberate boundary.
**Out of scope**
 
- Exactly-once and crash recovery. This version claims parity on a deterministic replay, nothing more.
- Performance or scale numbers, and production-grade tuning.
- Real Lucid code, data, logos or VINs.
## Repo layout and stack
 
One repo, one `docker-compose.yml` (Redpanda only), one `Makefile`. The Scala jobs run once, in a pinned container, to produce the frozen baseline. Nothing after the baseline needs Spark.
 
In a real cutover the Rust jobs would write to `.shadow` topics that nothing reads. The checks compare logical topic names, so `.shadow` is normalized away before any comparison.
 
```
lucid-telemetry-demo/
  README.md
  Makefile                 # make up, make baseline, make parity JOB=..., make privacy-check JOB=...
  docker-compose.yml       # Redpanda only
  docker/spark.Dockerfile  # pinned JDK, Scala, Spark 3.5.x and sbt
  .gitlab-ci.yml           # includes the oracle guard
  contracts/               # behavior contract per job (human-reviewed)
  proto/                   # .proto per topic: the one source of truth for schemas
  generator/               # Python synthetic events + replay schedules, seeded
  legacy-spark/            # Scala + sbt, one folder per job
    battery-health/  charging-sessions/
  stream-rs/               # Rust workspace
    crates/common/         # config, Protobuf types, Kafka plumbing, Processor trait, replay runner
    jobs/battery-health/   # already ported: the reference job
  parity/
    replay/                # replay schedules (events + batch boundaries)
    examples/              # tiny hand-checked cases
    golden/                # frozen Scala outputs + counters
  baseline/manifest.json   # legacy commit, fixture checksum, proto version, replay config
  privacy/                 # fields.yaml, output allowlist, reviewed field map
  deploy/k8s/              # minimal manifests, not run here
  docs/PORTING.md          # the migration checklist; seed for the Devin playbook
```
 
| Piece | Choice | Why |
| --- | --- | --- |
| Kafka | Redpanda, one container, no Console | Same Kafka API, starts in seconds |
| Legacy jobs | Scala 2.12, Spark 3.5.x Structured Streaming, in a pinned Docker image | Reproducible baseline; see "Decisions already made" |
| New jobs | Rust stable, `rdkafka`, `tokio`, `prost`, a decimal crate | Kafka client, Protobuf types, exact rounding |
| Data format | Protobuf, `.proto` files in the repo, no schema registry | Likely Lucid's format |
| Generator and tools | Python 3.12 + `protobuf` | Fast to write |
| Observability | Counters in the parity report | No Prometheus in this version |
| CI | GitLab CI | Matches Lucid's GitLab |
| Deploy | Minimal Kubernetes manifests | Shows the target runtime; not run here |
 
**Decisions already made**
 
- Protobuf is the wire format. Add `make peek TOPIC=...` to decode a few messages as JSON, and keep a decoded JSONL copy of every fixture for humans.
- Legacy jobs use Spark Structured Streaming, not DStreams.
## Synthetic data
 
Two input topics, both keyed by VIN, both fake, with Protobuf messages defined in `proto/`. Every event carries an `event_id` (UUID) so duplicates can be detected. Location lives in the charging topic, which is what makes the privacy beat work.
 
| Topic | Message fields | Sensitive fields |
| --- | --- | --- |
| `vehicle.battery.v1` | `event_id`, `vin`, `ts`, `soc_pct`, `soh_pct`, `cell_temp_max_c`, `pack_voltage_v` | `vin` |
| `vehicle.charging.v1` | `event_id`, `vin`, `ts`, `event`, `energy_wh`, `charger_type`, `lat`, `lon` (see the message contract) | `vin`, `lat`, `lon` |
 
`ts` is an `int64` of epoch milliseconds in UTC in every message.
 
**Generator (`generator/`)**
 
- Python with classes generated from `proto/`, fixed seed.
- Writes replay schedules, not just events: every event carries its batch number, in arrival order (see the behavior contract).
- About 200 fake vehicles over a simulated 24 hours, plus the tiny hand-checked cases in `parity/examples/`.
- Fake VINs with an obvious test prefix (for example `TST` + 14 characters). Coordinates have at most 6 decimals.
**Edge cases the generator injects on purpose**
 
- Every acceptance example, embedded in the larger run on its own VINs.
- Late events placed in later batches, after the watermark has moved, including late copies of IDs already seen.
- Duplicates with the same payload, and conflicting duplicates, including one whose later `ts` moves the watermark.
- Invalid messages, including one that arrives before a valid copy of the same `event_id`.
- A `stop` followed by resumed charging, a `plug_in` during an open session, and a session with no `unplug`.
- Coordinates that test rounding: negatives and exact halves (like `-33.8675` and `151.2095`).
- No `event_id` appears under two VINs; the generator asserts it.
## Behavior contract and replay
 
Both implementations are built against this contract, kept in `contracts/`. The legacy code can hide rules; the preparation can't be ambiguous. These are rules for the synthetic system, not claims about how Lucid handles charging. The repo makes three promises: energy is counted by explicit rules, session boundaries are reproducible, and declared outputs keep the required location precision.
 
**Message contract (`vehicle.charging.v1`)**
 
| Field | Meaning |
| --- | --- |
| `event_id` | Globally unique. Retransmissions reuse the same ID. |
| `vin` | Synthetic vehicle ID and the Kafka message key |
| `ts` | When the event happened: `int64` epoch milliseconds, UTC |
| `event` | Exactly one of `plug_in`, `start`, `progress`, `stop`, `unplug` |
| `energy_wh` | `int64` watt-hours delivered since the previous energy report, not a running total. Nonzero only on `progress` and `stop`. |
| `lat`, `lon` | Required on `plug_in`; optional and ignored on other events |
| `charger_type` | Taken from `plug_in` and kept for the session |
 
**Validation.** A message that fails any rule is counted `rejected` and doesn't reserve its `event_id`. Validation never looks at duplication.
 
- `event_id`: canonical lowercase UUID text, 36 characters.
- `vin`: 17 characters from `[A-Z0-9]`.
- `ts`: from 1,577,836,800,000 (2020-01-01 00:00 UTC) up to, not including, 4,102,444,800,000 (2100-01-01 00:00 UTC).
- `event`: one of the five values. The Protobuf default, `EVENT_UNSPECIFIED`, is invalid.
- `energy_wh`: zero or more, and nonzero only on `progress` and `stop`.
- `lat`, `lon`, `charger_type` are proto3 `optional`, so "missing" differs from zero. On `plug_in` all three are required: `lat` finite in \[−90, 90\], `lon` finite in \[−180, 180\], `charger_type` non-empty.
- Battery: every numeric field is proto3 `optional` and required. `soc_pct` and `soh_pct` in \[0, 100\]; `cell_temp_max_c` in \[−60, 120\]; `pack_voltage_v` above 0 and at most 1,000; all finite.
Integer watt-hours avoid rounding differences when Scala and Rust add values; display kWh as `energy_wh / 1000`.
 
**Session rules**
 
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
 
Duration is connected time: `plug_in` to the end. `end_ts` is the `unplug` time or, for an incomplete session, the last accepted event. It never includes the 30-minute wait.
 
**Event time, lateness and duplicates**
 
Arrival order decides which duplicate wins; event time decides how a session unfolds. Keeping the first payload and advancing time are separate: a later copy never replaces the original event's energy or session details, but its timestamp still moves the watermark.
 
The lateness check comes before deduplication. That's a deliberate simplification to match Spark: its `GroupState` docs say that with event-time timeouts, rows at or behind the watermark are filtered out before the state function sees them. So a late row can't be remembered by ID. Each batch runs these steps:
 
1. Take input in the fixed batches of the replay fixture. The watermark from the previous batch stays fixed for the whole batch.
2. Validate each message.
3. Every valid message's `ts` counts toward the running maximum, duplicates included.
4. Drop any valid message with `ts` at or before the previous batch's watermark and count it `late`. Late rows never reach deduplication, so their IDs aren't remembered.
5. Deduplicate the rest per VIN. Sort by `arrival_seq` first (the Kafka offset in the Kafka path, the fixture's sequence number in replay); first arrival wins.
6. Buffer accepted events.
7. At the end of the batch, set the watermark to the greater of its previous value and the largest valid `ts` seen so far minus 10 minutes.
8. Process buffered events at or before the new watermark, sorted by (`ts`, `arrival_seq`).
9. Close sessions whose last event plus 30 minutes is strictly earlier than the watermark.
There's one shared watermark per job. A newer timestamp on one VIN can therefore make another VIN's older events late. That's documented behavior, not a bug.
 
| Duplicate situation | Behavior |
| --- | --- |
| Same `event_id`, same decoded fields | Ignore; count `duplicate_events` |
| Same `event_id`, different decoded fields | Keep the first arrival's payload; count `conflicting_duplicates`. The later copy's `ts` still counts toward the watermark. |
| Invalid message arrives first | Rejected; the ID isn't reserved, so a later valid copy is processed |
| Any copy at or behind the watermark | Counted `late`, never as a duplicate |
| First valid arrival was late | Counted `late`, and its ID isn't remembered. A later copy that's on time is processed as a first arrival (example 13). |
 
Deduplication is scoped per VIN, the same key as session state. An `event_id` never appears under two VINs; the generator asserts this, and cross-VIN conflicts are out of scope. Seen IDs are kept for the whole bounded run, which isn't a production retention policy; choosing one is pilot scope. Conflicts compare decoded fields, not raw Protobuf bytes.
 
**Output (`charging.sessions.v1`)**
 
One final record per session, emitted once the rules allow it to close. No intermediate updates.
 
| Field | Meaning |
| --- | --- |
| `output_id` | See the encodings below |
| `vin` | Vehicle ID |
| `start_ts`, `end_ts` | Session boundaries |
| `duration_ms` | `end_ts − start_ts` |
| `total_energy_wh` | Sum of accepted increments |
| `start_lat`, `start_lon` | `plug_in` coordinates rounded to 3 decimals, exact halves away from zero, negatives included |
| `charger_type` | From the opening `plug_in` |
| `close_reason` | `unplug`, `inactivity_timeout` or `replaced_by_plug_in` |
 
Original coordinates are never emitted or logged. VIN stays, as this repo's policy allows, so the output is still identifiable; rounding doesn't make it anonymous. Rounding runs on the shortest decimal form: Scala `BigDecimal(x).setScale(3, HALF_UP)`, Rust a decimal crate, never float math. Records are compared by `output_id`; no order across vehicles is required.
 
Counters per run: `rejected`, `late` (every row dropped at step 4, copies included), `duplicate_events`, `conflicting_duplicates`, `orphan`, and sessions by `close_reason`.
 
**Acceptance examples** (these become `parity/examples/`, checked by hand)
 
Each example runs on its own VIN. Events arrive in one batch, B1, in the order listed, unless batches are shown. Every example ends with the standard flush from the replay section. Times are UTC on one day.
 
| # | Input | Expected |
| --- | --- | --- |
| 1 | `plug_in` 10:00; `progress` +2,000 Wh 10:10; `stop` +500 Wh 10:20; `unplug` 10:25 | 2,500 Wh; 25 min; `unplug` |
| 2 | As 1, plus a second copy of the 10:10 `progress` with the same ID and fields | 2,500 Wh; `duplicate_events` 1 |
| 3 | As 1, plus a second copy of the 10:10 `progress` claiming +9,000 Wh | 2,500 Wh; `conflicting_duplicates` 1 |
| 4 | B1: `plug_in` 10:00, `progress` +100 Wh 10:05. B2: a copy of the 10:05 `progress` with `ts` 10:30 and +900 Wh. B3: `progress` +100 Wh 10:15 | Watermark 9:55 after B1, 10:20 after B2. 100 Wh; ends 10:05, `inactivity_timeout`; `conflicting_duplicates` 1; `late` 1 |
| 5 | `plug_in` 10:00; `progress` +100 Wh 10:30 | One session: 100 Wh; ends 10:30, `inactivity_timeout`. Exactly 30 minutes is allowed. |
| 6 | `plug_in` 10:00; `progress` +100 Wh 10:30:00.001 | 0 Wh session ending 10:00, `inactivity_timeout`; `orphan` 1 |
| 7 | `plug_in` 10:00; `progress` +100 Wh 10:10 | Emitted once the watermark passes 10:40: 100 Wh; ends 10:10, `inactivity_timeout` |
| 8 | B1: `plug_in` 10:00, `progress` +100 Wh 10:20. B2: `progress` +50 Wh 10:10 | Watermark 10:10 after B1; an event exactly at the watermark is late. 100 Wh; `late` 1 |
| 9 | `plug_in` 23:50; `unplug` 00:10 the next day | One 20-minute session |
| 10 | `plug_in` 10:00; `progress` +100 Wh 10:05; `plug_in` 10:10; `unplug` 10:20 | 100 Wh ending 10:05, `replaced_by_plug_in`; then 0 Wh, 10 min, `unplug` |
| 11 | `plug_in` 10:00; `progress` ID X −5 Wh 10:05; `progress` ID X +200 Wh 10:10; `unplug` 10:15 | 200 Wh; `rejected` 1; no duplicates |
| 12 | `plug_in` 10:00; `progress` +100 Wh 10:05; `stop` +50 Wh 10:10; `start` 10:20; `progress` +100 Wh 10:30; `unplug` 10:35 | One session: 250 Wh; 35 min; `unplug` |
| 13 | B1: `plug_in` 10:00, `progress` +100 Wh 10:30. B2: `progress` ID Y +50 Wh 10:15. B3: `progress` ID Y +50 Wh 10:25 | B2's copy is late (watermark 10:20) and not remembered, so B3's copy counts as a first arrival. 150 Wh; ends 10:30; `late` 1; no duplicates |
| 14 | Two VINs: `plug_in` at (−33.8675, 151.2095) and at (0.0005, −0.0005) | Rounded to (−33.868, 151.210) and (0.001, −0.001) |
| 15 | Battery readings at 14:14:59.999 and 14:15:00.000 | Two windows, starting 14:10 and 14:15 |
| 16 | Battery: one window with max cell temp 55.0, another with 55.1 | `alert` false, then true |
 
**`battery-health`**
 
| Question | Decision |
| --- | --- |
| Window | Tumbling 5 minutes by event time: \[start, start + 5 min) |
| Validation, lateness, duplicates | Same steps as charging, with a 2-minute watermark delay; duplicates dropped per VIN |
| When a window is emitted | Once the watermark reaches the window end; emitted windows are final |
| Alert | `max_cell_temp_c` strictly above 55.0 |
| Output | `output_id`, `vin`, `window_start`, `avg_soc_pct`, `min_soh_pct`, `max_cell_temp_c`, `alert`, `event_count` |
| Comparison | Averages within 1e-9 absolute; every other field exact |
 
**`output_id` encodings** (lowercase hex SHA-256, 64 characters, over UTF-8 text)
 
- Charging: the opening `plug_in`'s `event_id`. `00000000-0000-4000-8000-000000000001` gives `11e594f481958c10e3015d0bf0447a22f068a8a647f475df15ce2c7ab4b8f3f1`.
- Battery: `battery-health`, the VIN and `window_start`, joined by a vertical bar, with `window_start` as a base-10 epoch-millisecond integer, no padding. For VIN `TST00000000000001` and the window at 2026-09-21 14:15:00 UTC, the text is `battery-health|TST00000000000001|1790000100000`, which gives `924bbe50976ec7a530dd47f70c1c42f19bb46342e4e320a06a3e076bb70feda8`.
**Implementation notes for Scala**
 
Setting Spark's watermark option isn't a full specification of the procedure above, so:
 
- Pipeline order: validation, then `withWatermark`, then `flatMapGroupsWithState` keyed by VIN with an event-time timeout. Deduplication and session logic live in the state function. Spark's watermark comes from the rows that reach its watermark operator; with this order that's every valid row, which is step 3. Spark's own [examples](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html) also put watermark tracking before deduplication.
- Spark filters rows at or behind the watermark before the state function runs, which is why step 4 comes before step 5.
- **Counting late rows (a stage 3 check).** Spark reports [`numRowsDroppedByWatermark`](https://spark.apache.org/docs/3.5.6/api/java/org/apache/spark/sql/streaming/StateOperatorProgress.html) per stateful operator, per trigger. Its existence doesn't prove it equals the contract's `late`. So the harness also counts `late` on its own: for each batch, the valid fixture rows with `ts` at or before the watermark that batch ran with, read from the query progress. The metric, summed over every trigger including no-data batches, must equal that count in these cases: example 8 (exactly at the watermark), several late copies of one ID (each counted), late rows across several batches, a late row next to a conflicting duplicate that moves the watermark (example 4), and a run with no late rows. If the two disagree, stop and fix the contract or the pipeline before freezing the baseline.
- **Counting isn't precedence.** The metric only counts dropped rows; it doesn't remember their IDs. Under this contract that's intended, and example 13 checks it separately. If duplicate precedence for late first arrivals ever becomes a requirement, this pipeline can't provide it; that would need a different design, with dedup state that sees rows before the watermark filter.
- Spark doesn't guarantee iterator order inside a group, even with one partition. The state function sorts by `arrival_seq` before deduplicating, then by (`ts`, `arrival_seq`) before processing. Rust does the same.
- Spark keeps a batch's watermark fixed and advances it after the batch. The Rust runner must not update it mid-batch either.
- Timeout: the earlier of (earliest buffered `ts` − 1 ms) and (last event + 30 min), so the group wakes both to process buffered events and to close sessions.
- Emission timing is a hypothesis until stage 3's trace confirms it. Spark hands each batch the watermark from earlier batches, so Scala may emit one batch later than Rust. Final records and counters should match either way; the trace shows whether they do.
**Replay schedule**
 
- `parity/replay/<job>.jsonl` lists events in arrival order, each with its `arrival_seq` and batch number.
- Spark replays it through a `MemoryStream`, calling `processAllAvailable()` after each batch. Rust's replay runner follows the same batches. Both write a trace: for each batch, the input events, the watermark in effect, and the records emitted.
- **Flush.** Every fixture ends the same way. The target watermark T is the largest *valid* fixture `ts` plus 1 hour; rejected rows never count. Each job gets one control event on the reserved VIN `TSTZZZZZZZZZZZZZZ` with `ts` = T plus the job's delay: a `plug_in` for charging (T + 10 min), a valid reading for battery (T + 2 min). Then one empty batch, which in Spark is the no-data batch that runs when the watermark moves. That puts the watermark at exactly T. The reserved VIN is left out of outputs and counters.
- **Drain condition**, checked rather than assumed: after the flush, no VIN other than the reserved one has an open session (charging), an open window (battery), or buffered events. Seen IDs may remain; they're kept for the whole run.
  - Scala: every state-function call emits a status marker `{vin, batch, busy}`, where `busy` means the VIN has an open session or window or buffered events. Every VIN's last marker must be `busy = false`, except the reserved VIN's, which must be `busy = true` (its control event is still buffered). Cross-check: the stateful operator's `numRowsTotal` in the last progress equals the number of distinct VINs that reached state (valid, non-late rows) plus one for the reserved VIN.
  - Rust: inspect the state map directly with the same rule.
  - If the check fails, parity refuses to compare.
- One input partition, one worker. That's the boundary for this repo; parallelism is pilot scope.
 
**Resolved questions** (stage 2 review; these are part of the contract)
 
| Job | Question | Answer |
| --- | --- | --- |
| charging | Rejected `ts` and the watermark | Never moves it. Validation comes first. |
| charging | Flush T | Largest valid `ts` + 1 hour; rejected rows never count. |
| charging | Gap check | Both step 8 and step 9 apply, strict `>` 30 min. |
| charging | `start` in an open session | Accepted, no energy, resets the 30-minute gap timer. |
| charging | `end_ts` after `stop` | The `stop`'s `ts`. |
| charging | `energy_wh = 0` on `plug_in`/`start`/`unplug` | Adds nothing. |
| charging | Duplicate copy differs only in ignored fields | Counts as conflicting (all decoded fields compare). |
| charging | Same-ID `plug_in` while a session is open | Plain duplicate. |
| charging | Empty flush batch | As `parity/replay/FORMAT.md` says: control event in batch N, empty batch N+1. |
| charging | `close_reason` encoding | Enum on the wire; counters keyed by lowercase words. |
| charging | Watermark before any valid `ts` | Unset. It's only set at the end of a batch, so nothing in batch 1 is ever late. |
| battery | Output topic | `battery.health.v1`. |
| battery | Counters | Exactly `rejected`, `late`, `duplicate_events`, `conflicting_duplicates`. |
| battery | Is `ts` `optional`? | No, plain `int64`. |
| battery | Empty windows | Never emitted. |
| battery | `avg_soc_pct` | Double sum ÷ count; the 1e-9 tolerance covers it. |
| battery | Watermark before any valid `ts` | Same as charging: unset; nothing in batch 1 is ever late. |
| battery | Window emission | `watermark ≥ window_end`. |
| battery | Window alignment | Aligned to the Unix epoch. |
## Legacy Scala jobs
 
Two small jobs, each under about 200 lines. Each hides rules a reader could miss, because that's what makes a real port hard and gives Devin something to find.
 
| Job | What it does | Rules a reader could miss | Role |
| --- | --- | --- | --- |
| `battery-health` | 5-minute window per VIN: average SoC, minimum SoH, max cell temp, alert | 2-minute watermark delay; alert is strictly above 55 °C | Already ported: the reference job |
| `charging-sessions` | Groups `plug_in` → `unplug` into one session per VIN | Gap strictly over 30 minutes closes a session; late events drop per batch watermark; a `plug_in` splits an open session; start location rounded to 3 decimals | Ported to Rust later, in its own session |
 
**How they run**
 
- Each job's logic is one function with two entry points: a Kafka entry point (read, `from_protobuf`, write with `to_protobuf`) and a replay entry point (`MemoryStream`) that makes the golden files.
- State, lateness and duplicates follow the contract's steps, written explicitly with `flatMapGroupsWithState` and event-time timeouts. See the implementation notes for Scala.
- Rejected, late, duplicate and orphan events are counted into a counters output, so the baseline includes behavior, not just final records.
- The baseline runs once in the pinned Spark image and is committed with `baseline/manifest.json`. If Spark won't run, stop and fix it. There's no Python fallback: the claim is that Rust matches the Scala job, not someone's reading of it.
## Rust skeleton and reference job
 
Shared code stays small: only what these two jobs need. The second job decides what's worth sharing. A generic stateful runner with transactions, timers, metrics and recovery is a project of its own, and it's out of scope.
 
**`crates/common`**
 
- `Config` from env vars.
- Protobuf types generated from `proto/` with `prost`.
- Kafka plumbing: consume one partition, produce, commit offsets. No transactions in this version.
- A `Processor` trait with explicit event time: `on_event(event, state)` and `on_watermark(watermark_ms, state)`, both returning outputs. There is no wall-clock `now`.
- A replay runner that drives the trait from a replay schedule, batch by batch, the same way the Spark harness does.
- Known gap, written in the README: state lives in memory and offsets are committed after processing, so a crash loses open sessions. Recoverable state is pilot scope.
**`jobs/battery-health` (reference)**
 
- About 150 lines implementing `Processor`.
- Unit tests built from the hand-checked examples.
- Passes `make parity` (records, counters, examples) and `make privacy-check`.
- Its own `Dockerfile` and a minimal manifest. It's the example `docs/PORTING.md` points to.
**`docs/PORTING.md` (the checklist)**
 
1. Read the Scala job and list every rule. Compare the list with `contracts/<job>.md`; ask about any difference before coding.
2. Implement `Processor` in `jobs/<name>`, following `battery-health`.
3. Keep the output message, the `output_id` encoding and the counters exactly as the contract says.
4. Pass `make parity` and `make privacy-check`.
5. Don't touch anything the oracle guard protects: golden files, replay fixtures, acceptance examples, `legacy-spark/`, `contracts/`, `proto/`, the baseline manifest, `privacy/`, the comparison rules, the guard script or the CI config. CI blocks it.
6. Open an MR with the reports and a "What remains before production" section.
## Parity harness and correctness tests
 
The parity harness is the centerpiece, and it's built so a green result is hard to get by accident. `make parity JOB=x` runs every check below and writes one report for the MR.
 
| Check | What it does | Passes when |
| --- | --- | --- |
| Frozen baseline | Reads `baseline/manifest.json`: legacy commit, fixture checksum, proto version, replay config | All four match the current run; otherwise parity refuses to run |
| Drain condition | Checks that the flush closed everything (see the replay section) | No VIN but the reserved one has an open session, open window or buffered events |
| Output integrity | Looks for repeated `output_id`s in each run before any comparison | None; a repeat fails right away instead of hiding in a map |
| Record parity | Compares final records to the golden file by `output_id` | Same set of records; fields exact, except battery averages within 1e-9 absolute |
| Behavioral counters | Compares `rejected`, `late`, `duplicate_events`, `conflicting_duplicates`, `orphan` and sessions by `close_reason` | Same counts as the Scala run |
| Acceptance examples | The 16 hand-checked cases, with their batches and flush | Scala and Rust both match them |
| Oracle guard | Diffs the MR against the target branch | No changes to golden files, replay fixtures, acceptance examples, Scala source, contracts, `.proto` files, the baseline manifest, privacy policy and assertions, comparison rules, or the guard's own script and CI config |
 
No ordering is checked across vehicles; consumers only need the same set of records keyed by VIN.
 
The hand-checked examples matter because Devin helps build both the legacy job and the port. Parity alone could reproduce a shared misunderstanding; independent examples catch it.
 
**What this version does not claim**
 
No exactly-once and no crash recovery. Each mechanism proves something different:
 
| Mechanism | What it establishes |
| --- | --- |
| Kafka transaction | Output records and consumed offsets commit together |
| Dedup by `event_id` | Repeated events are recognized and handled |
| Parity run | This replay produced the expected results |
| Recoverable, consistent state | A stateful job continues correctly after a failure |
 
The concrete gap: Rust commits offsets while an open charging session lives only in memory. If it crashes, those inputs are already committed and the session is gone. Recoverable state is out of scope for this repo.
 
**Report format (attached to the MR)**
 
- One line per check: pass or fail, counts, and the first 5 differences if any.
- The counters side by side, Scala vs Rust.
- A short "what this proves, and what it doesn't" line in plain words, so the VP can read it too.
**Checking the checks**
 
Before the `charging-sessions` port, run `make check-the-checks` once. It applies two deliberate bugs on a throwaway branch (rounding dropped, one `energy` increment double-counted) and confirms parity fails on both. Keep the output; it's good evidence if they ask how you know the harness works.
 
A real cutover would write to `.shadow` topics and compare for days. That is not built here; the replay harness stands in for it.
 
## Privacy regression checks
 
Call these privacy regression checks for the declared outputs, nothing broader. They prove the port didn't widen what leaves the job. They are not a general lineage tool and not a substitute for the privacy reviewer.
 
**What `make privacy-check` runs**
 
1. An explicit field policy in `privacy/fields.yaml`: each field is `vin`, `location` or `none`.
2. An output allowlist per logical topic: the job may write only the declared fields to the declared topics. `.shadow` is normalized to the logical topic first.
3. Direct assertions that `start_lat` and `start_lon` equal the contract's rounding of the `plug_in` coordinates, on every output record.
4. Coordinate fixtures chosen to break rounding: negatives and exact boundaries.
**Reviewed field map for `charging-sessions`** (human-reviewed, kept in `privacy/`)
 
| Input field | Transformation | Output field |
| --- | --- | --- |
| `vin` | Copied | `vin` |
| `lat` of the opening `plug_in` | Rounded to 3 decimals, exact halves away from zero | `start_lat` |
| `lon` of the opening `plug_in` | Rounded to 3 decimals, exact halves away from zero | `start_lon` |
| `lat`/`lon` of every other event | Ignored; never emitted or logged | — |
| `ts` | Session boundaries | `start_ts`, `end_ts`, `duration_ms` |
| `energy_wh` | Summed | `total_energy_wh` |
| `charger_type` of the opening `plug_in` | Copied | `charger_type` |
| `event` sequence | Rules in the contract | `close_reason` |
 
Never describe the output as anonymized: 3 decimals is coordinate precision (roughly 100 m), and the VIN is still present.
 
## Stages
 
One stage per Devin session. Each session ends by opening an MR and stopping; the next stage starts only after a human reviews it. If something in this spec is ambiguous or looks wrong, ask before guessing.
 
| Stage | Exit condition |
| --- | --- |
| 1. Environment check | Devin can clone, build, run containers, run GitLab CI, and open a test MR |
| 2. Behavior contract | Rules, validation, schemas, `output_id` encodings, replay format and the 16 acceptance examples, drafted as files and reviewed by a human |
| 3. Risky semantics proven | The pinned Spark job and the Rust runner match hand-calculated outputs and counters for late duplicates, a conflicting duplicate that advances time, exact watermark boundaries and the final drain. A trace shows each batch, the watermark Scala observed, and the records emitted. Spark's dropped-row metric equals the harness's own late count in the listed cases, and example 13 confirms late IDs aren't remembered. |
| 4. Frozen baseline | Both Scala jobs run in the pinned image; golden files, counters and manifest committed |
| 5. Reference implementation | `battery-health` passes parity, counters, examples and privacy checks |
| 6. Recorded migration | `charging-sessions` ported from a clean baseline; `check-the-checks` run; clips recorded |
| 7. Presentation polish | Reports, playbook, clips, two rehearsals |
 
**Rules that apply to every stage**
 
- Keep shared Rust code to what these two jobs need. No generic streaming framework.
- If the pinned Spark image won't run, fix it. Never substitute a Python or other reference for the Scala baseline.
- If stage 3 shows Spark doesn't match the contract, stop and report it. The contract changes only before the baseline is frozen, and only with human approval.
- Expected results for the acceptance examples come from this spec's table, not from running either implementation.
- Event time only. Never use the wall clock in job logic.