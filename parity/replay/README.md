# parity/replay/

Replay fixtures: ordered events plus batch boundaries and an explicit flush,
consumed deterministically by both implementations. The file format is in
`FORMAT.md` (stage 2). The fixtures themselves (`<job>.jsonl`, `<job>.counters.json`)
are produced by the generator in stage 3 alongside the Scala jobs and the Rust
replay runner.
