# parity/examples/

The spec's 16 acceptance examples, one folder each (`<nn>-<slug>/`), with
`input.jsonl` (replay fixture, see `../replay/FORMAT.md`) and `expected.json`
(output records, counters, and the per-batch watermark where the spec states it).

Expected values come from the spec's table only, never from running either
implementation. `build_examples.py` holds every value as hand-entered literals;
it only converts clock times to epoch milliseconds and computes `output_id`
hashes. Re-run it after editing; CI checks the folders against `FORMAT.md`
(`parity/replay/check_examples.py`).

Conventions: day 2026-09-21 UTC; VIN `TST` + zero-padded example number;
`event_id` `00000000-0000-4000-8000-00000000NNii` (NN example, ii event index).
