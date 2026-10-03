# parity/replay/

Replay fixtures: ordered events plus batch boundaries and an explicit flush,
consumed deterministically by both implementations. The file format is in
`FORMAT.md`.

- `charging-sessions.jsonl`, `battery-health.jsonl`: frozen stage-4a fixtures from
  `make fixtures` (generator/generate.py). Never edit by hand; their sha256 is pinned
  in `baseline/manifest.json`.
- `generator-run.json`: seed, Python/protobuf versions and the per-fixture case summary.
- `check_examples.py`: format checks for the acceptance examples.
