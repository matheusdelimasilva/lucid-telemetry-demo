# parity/probes/

Stage-3 probes for the late-count check (SPEC.md, "Parity harness and
correctness tests"): cases the 16 acceptance examples don't cover. Same layout
and format as `../examples/` (`<nn>-<slug>/input.jsonl` + `expected.json`, see
`../replay/FORMAT.md`).

Expected values are hand-entered from the contracts in `build_probes.py`, which
imports `../examples/build_examples.py`'s helpers and only converts clock times
and hashes `output_id`s. Re-run it after editing; `check_examples.py` checks the
folders structurally and `make spark-examples` replays them through Spark.

| Probe | Job | What it pins |
| --- | --- | --- |
| 01-several-late-copies-one-id | charging | three identical late copies + one copy exactly at the watermark → late 4; a later on-time copy is a first arrival |
| 02-late-rows-across-batches | charging | one late row in each of batches 2–4 (batch 3's exactly at the watermark) → late 3 |
| 03-no-late-rows | charging | watermark moves every batch, a row 1 ms above it → late 0 |
| 04-battery-late-row | battery | two late readings (one exactly at the watermark) that would raise an alert if counted → late 2 |
| 05-conflicting-copy-after-session-closed | charging | on-time conflicting copy of an ID on a VIN with no open session → conflicting_duplicates 1 (seen IDs kept for the run) |

Conventions: day 2026-09-21 UTC; VIN `TSTP` + zero-padded probe number;
`event_id` `00000000-0000-4000-8000-00000099NNii`.
