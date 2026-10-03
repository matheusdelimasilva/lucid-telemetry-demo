//! Running the frozen fixture twice through the real CLI must give
//! byte-identical artifacts: no map iteration order or other run-to-run
//! state may leak into records, counters or trace.

mod support;

use std::fs;

use anyhow::Result;
use battery_health::{encode, BatteryHealth};
use common::cli::{run, Args};
use common::job::BATTERY;
use support::repo_root;

#[test]
fn fixture_replay_is_byte_identical_across_runs() -> Result<()> {
    let fixture = repo_root().join("parity/replay/battery-health.jsonl");
    let dirs = [tempfile::tempdir()?, tempfile::tempdir()?];
    for dir in &dirs {
        let args = Args {
            out: Some(dir.path().to_path_buf()),
            fixtures: vec![(BATTERY.name.to_string(), fixture.clone())],
            ..Args::default()
        };
        assert!(run::<BatteryHealth>(&args, BATTERY, encode)?);
    }
    for file in [
        "records.jsonl",
        "counters.json",
        "trace.jsonl",
        "drain.json",
        "result.json",
        "run.json",
    ] {
        let name = format!("battery-health.{file}");
        let first = fs::read(dirs[0].path().join(&name))?;
        let second = fs::read(dirs[1].path().join(&name))?;
        assert!(!first.is_empty(), "{name} is empty");
        assert!(first == second, "{name} differs between two runs");
    }
    let records = fs::read_to_string(dirs[0].path().join("battery-health.records.jsonl"))?;
    assert!(
        records.lines().count() > 1000,
        "expected a non-trivial record count"
    );
    Ok(())
}
