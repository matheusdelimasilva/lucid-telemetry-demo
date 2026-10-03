# Porting a job to Rust

The checklist from SPEC.md ("Rust skeleton and reference job"), with the
mechanics the CI relies on. Seeded in stage 4b; stage 5 fills in what the
`battery-health` reference port teaches.

1. Read the Scala job (`legacy-spark/<job>/`) and list every rule. Compare the
   list with `contracts/<job>.md`; ask about any difference before coding.
2. Implement `Processor` in `stream-rs/jobs/<job>/`, following `battery-health`.
   The crate is a workspace member named after its directory and builds a binary
   that follows the CLI contract in `stream-rs/jobs/README.md` (fixture mode
   writing `<job>.{records.jsonl,counters.json,trace.jsonl,drain.json,result.json,run.json}`
   in the Spark harness shapes, and suite mode for the examples and probes).
3. Keep the output message, the `output_id` encoding and the counters exactly as
   the contract says.
4. Pass `make rust-parity` locally (it is what the `rust-parity` CI job runs:
   fixture replay, the 16 examples and the probes, then `tools/suite_check.py`
   on `build/rust-parity/examples/<job>/`, and `tools/parity.py` and
   `tools/privacy_check.py` on `build/rust-parity/`). Attach the three reports
   (`build/rust-parity/examples/<job>/<job>.suite.md`,
   `build/rust-parity/<job>.parity.md`, `<job>.privacy.md`) to the MR.
   The grading is done by `tools/` (protected), not by the job: the binary's
   own per-case PASS/FAIL lines are informational, `tools/suite_check.py`
   compares `outputs.json` with `expected.json` and `tools/parity.py` compares
   the fixture run with `parity/golden/`.
   Known limit: both engines report their own drain result (`drain.json`), since
   only the job can see its own state; the records and counters compared
   against the golden files and `expected.json` are still checked
   independently, so this is acceptable.
5. Don't touch anything the oracle guard protects: `parity/golden/`,
   `parity/replay/`, `parity/examples/`, `parity/probes/`, `legacy-spark/`,
   `contracts/`, `proto/`, `baseline/manifest.json`, `privacy/`, `tools/`, `ci/`,
   `.github/workflows/` or `.gitlab-ci.yml`. CI blocks it: the `oracle-guard`
   job runs the target branch's `ci/oracle_guard.sh` against the MR diff. Run
   `make oracle-guard` locally to check before pushing.
6. Open an MR with the reports and a "What remains before production" section.
