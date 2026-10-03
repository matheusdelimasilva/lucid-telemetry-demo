# Stage 3a: legacy Scala jobs on the replay harness

What the pinned Spark (3.5.9, Scala 2.12.18, `docker/spark.Dockerfile`) did on the
16 acceptance examples and the 5 probes in `parity/probes/`, from
`make spark-examples` (per-case `trace.jsonl`, `drain.json`, `outputs.json`,
`result.json` under `build/spark-examples/`, kept as a CI artifact).

All 21 cases pass: records and counters match `expected.json`, every stated
watermark matches, `numRowsDroppedByWatermark` equals the harness's own late count
in every batch, and every drain check passes.

## Emission batch per case

"Contract batch" is the fixture batch whose end-of-batch watermark first satisfies
the contract's steps 7–9 (charging) or the window-end rule (battery). "Spark" is
where the record appeared: fixture batch and Spark micro-batch id.

| Case | Records | Contract batch | Spark | Same batch? |
| --- | --- | --- | --- | --- |
| 01 basic-session | unplug | 2 (flush) | 2, mb 3 | yes |
| 02 identical-duplicate | unplug | 2 (flush) | 2, mb 3 | yes |
| 03 conflicting-duplicate | unplug | 2 (flush) | 2, mb 3 | yes |
| 04 conflicting-copy-moves-watermark | inactivity_timeout | 4 (flush) | 4, mb 6 | yes |
| 05 gap-exactly-30-min | inactivity_timeout | 2 (flush) | 2, mb 3 | yes |
| 06 gap-over-30-min-orphan | inactivity_timeout | 2 (flush) | 2, mb 3 | yes |
| 07 timeout-after-flush | inactivity_timeout | 2 (flush) | 2, mb 3 | yes |
| 08 late-exactly-at-watermark | inactivity_timeout | 3 (flush) | 3, mb 4 | yes |
| 09 crosses-midnight | unplug | 2 (flush) | 2, mb 3 | yes |
| 10 plug-in-replaces-open-session | replaced_by_plug_in; unplug | 1; 2 (flush) | 1, mb 1; 2, mb 3 | yes |
| 11 invalid-first-then-valid-copy | unplug | 2 (flush) | 2, mb 3 | yes |
| 12 stop-then-resume | unplug | 2 (flush) | 2, mb 3 | yes |
| 13 late-first-arrival-not-remembered | inactivity_timeout | 4 (flush) | 4, mb 5 | yes |
| 14 coordinate-rounding | 2 × inactivity_timeout | 2 (flush) | 2, mb 3 | yes |
| 15 battery-window-boundary | windows 14:10, 14:15 | 2 (flush) | 2, mb 3 | yes |
| 16 battery-alert-strictly-above-55 | windows 10:00, 10:05 | 2 (flush) | 2, mb 3 | yes |
| P01 several-late-copies-one-id | inactivity_timeout | 4 (flush) | 4, mb 5 | yes |
| P02 late-rows-across-batches | unplug | 5 (flush) | 5, mb 9 | yes |
| P03 no-late-rows | unplug | 4 (flush) | 4, mb 7 | yes |
| P04 battery-late-row | windows 10:00; 10:05 | 2; 3 (flush) | 2, mb 3; 3, mb 5 | yes |
| P05 conflicting-copy-after-session-closed | unplug (VIN A); inactivity_timeout (VIN B) | 2; 4 (flush) | 2, mb 3; 4, mb 6 | yes |

Scala never emitted one batch later. In every case the record came from the
**no-data micro-batch** Spark runs right after the data micro-batch that advanced
the watermark. That no-data batch runs inside the same `processAllAvailable()`
call, so it lands in the same fixture batch.

## Where Spark differed from the contract's description

1. **The explicit empty flush batch does nothing in Spark.** FORMAT.md says the empty
   batch after the control event "in Spark is the no-data batch". In practice
   `processAllAvailable()` for the control batch already runs that no-data
   micro-batch, so the watermark reaches T and everything closes inside the
   control batch. The harness still issues the empty batch (`kind: flush_empty`
   in the trace); it runs zero micro-batches and the watermark stays at T.
   Results are unaffected.
2. **The late-row filter uses the previous micro-batch's watermark.**
   `FlatMapGroupsWithStateExec.eventTimeWatermarkForLateEvents` is the watermark of
   the previous micro-batch, while eviction and timeouts (`getCurrentWatermarkMs`)
   use the current one. In no-data micro-batches the two differ (for example 01
   mb 1: late filter 0, eviction 10:15). In every data micro-batch of all 21 cases,
   though, the late filter equalled the watermark in effect. That's because a
   no-data batch always runs between a watermark advance and the next data batch.
   So contract step 4 (late = `ts` ≤ the previous batch's watermark) holds, but only
   while `spark.sql.streaming.noDataMicroBatches.enabled` keeps its default `true`.
   The trace records both watermarks per micro-batch (`late_filter_watermark`,
   `eviction_watermark`).
3. **The watermark before any valid `ts` shows up as 0, not "unset".** Progress
   reports `1970-01-01T00:00:00Z` for batch 1. This is equivalent to "unset"
   (contract resolved question 12): validation rejects any `ts` ≤ 0, so nothing in
   batch 1 can be late. The trace shows `0`.
4. **`numInputRows` counts each row twice.** The logic splits the decoded stream into
   the valid branch (watermark, then state) and the rejected-counter branch, then
   unions them. Spark counts the source once per branch, so for example 01 it
   reports 8 input rows for 4 events. Outputs and counters are unaffected.
   `numRowsDroppedByWatermark` covers only the stateful branch.
5. **Idle triggers report progress too.** Before the first data arrives, the query
   can report a progress for batch 0 with no stateful operator. The harness keeps
   only executed micro-batches, meaning progresses with state-operator metrics.
6. **Drain check: the SPEC text conflicted with the contract; the behavior did not change.**
   The old drain rule ("one key" in the last progress) couldn't hold, because seen
   IDs are kept per VIN for the whole run, so every VIN keeps a state row. With
   review sign-off, SPEC.md and both contracts now state the drain condition as
   "no VIN but the reserved one has an open session/window or buffered events",
   checked with per-call status markers plus a `numRowsTotal` cross-check.
   Probe 05 pins the behavior this protects: an on-time conflicting copy on a VIN
   with no open session still counts `conflicting_duplicates`.

Nothing in the contracts required a workaround in Spark.

## Choices made where the spec doesn't say

- **Marker batch.** A state function can't see the micro-batch id, so the job emits
  `{vin, busy}` and the harness attaches the micro-batch id of the sink call as
  `batch`.
- **Harness late count.** Validity comes from the job's `decodeAndValidate`, run
  statically over the fixture. The late decision itself (`ts` ≤ the watermark read
  from the batch's first progress) belongs to the harness.
- **`watermark_after`.** For a fixture batch, this is the watermark of the next
  executed micro-batch, or the last progress's watermark for the final batch.
- **Control events.** `event_id` `ffffffff-ffff-4fff-bfff-ffffffffffff`. Charging uses
  lat/lon 0.0 and `charger_type` "flush". Battery uses soc 50, soh 100,
  temperature 20, voltage 400.
- **Session and state layout.** `local[1]`, one shuffle partition, UTC session
  timezone, and a Kryo encoder for the state object.
- **Kafka entry points.** Read with `startingOffsets=earliest`, using
  `arrival_seq = offset`. Records go out through `to_protobuf`, keyed by VIN.
  Counters are rewritten to `--counters-out` (JSON) after every batch, and `late`
  is summed from `numRowsDroppedByWatermark` by a `StreamingQueryListener`.
- **Getting fixtures into CI.** The fixtures are copied into the Spark image, and
  results come back out with `docker cp`, because a docker-in-docker job can't
  bind-mount the checkout.
- **Probe conventions.** VIN `TSTP` + number; `event_id`
  `00000000-0000-4000-8000-00000099NNii`.
