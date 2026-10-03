# generator/

Python 3.12 synthetic event generator: seeded fake VINs (TST prefix), protobuf
payloads, and replay schedules (events + batch boundaries).

- `generate.py`: writes the stage-4a replay fixtures `parity/replay/<job>.jsonl`
  (FORMAT.md) and `parity/replay/generator-run.json` (seed, Python and protobuf
  versions, sizes, case summary), and prints the summary table. Run it with
  `make fixtures`, which uses the pinned `python:3.12.11-slim` image; same seed and
  Python version give the same bytes. It re-classifies the finished fixture with
  `tools/contract.py` and fails if any injected row doesn't land in its intended
  class (rejected / late / duplicate / conflicting / accepted), if an `event_id`
  appears under two VINs, or if a fixture reaches 5 MB.
- `smoke_send.py`: one-message Kafka smoke check.
