mod support;

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use anyhow::{Context, Result};
use common::event::InputEvent;
use common::job::{BATTERY, CHARGING, RESERVED_VIN};
use common::replay::{run_fixture, ReplayRun};
use common::runner::Counters;
use serde_json::Value;
use support::{case_dirs, json_lines, parse_expected, repo_root, Recorder, SeenEvent};

#[test]
fn examples_and_probes_match_contract_runner_behavior() {
    let root = repo_root();
    let example_dirs = case_dirs(&root, "examples").expect("discover examples");
    let probe_dirs = case_dirs(&root, "probes").expect("discover probes");
    assert_eq!(example_dirs.len(), 16, "expected all 16 examples");
    assert_eq!(probe_dirs.len(), 5, "expected all 5 probes");

    let mut failures = Vec::new();
    for (suite, dirs) in [("examples", example_dirs), ("probes", probe_dirs)] {
        for case_dir in dirs {
            let case = case_dir.file_name().unwrap().to_string_lossy().to_string();
            match run_case(&root, suite, &case_dir) {
                Ok(case_failures) if case_failures.is_empty() => println!("PASS {suite}/{case}"),
                Ok(case_failures) => failures.extend(
                    case_failures
                        .into_iter()
                        .map(|failure| format!("{suite}/{case}: {failure}")),
                ),
                Err(error) => failures.push(format!("{suite}/{case}: {error:#}")),
            }
        }
    }
    assert!(
        failures.is_empty(),
        "Rust example/probe failures:\n{}",
        failures.join("\n")
    );
}

fn run_case(root: &Path, suite: &str, case_dir: &Path) -> Result<Vec<String>> {
    let fixture = case_dir.join("input.jsonl");
    let expected = parse_expected(&case_dir.join("expected.json"))?;
    let job = expected
        .get("job")
        .and_then(Value::as_str)
        .context("expected.json is missing job")?;
    let artifact_dir = root.join("build/rust-examples").join(suite).join(
        case_dir
            .file_name()
            .context("case folder has no file name")?,
    );

    if job == CHARGING.name {
        let run = run_fixture(&fixture, CHARGING, Recorder::default())?;
        run.write(&artifact_dir)?;
        check_run(&run, &fixture, &expected)
    } else if job == BATTERY.name {
        let run = run_fixture(&fixture, BATTERY, Recorder::default())?;
        run.write(&artifact_dir)?;
        check_run(&run, &fixture, &expected)
    } else {
        anyhow::bail!("unknown expected job {job:?}")
    }
}

fn check_run<P: common::processor::Processor>(
    run: &ReplayRun<P>,
    fixture_path: &Path,
    expected: &Value,
) -> Result<Vec<String>>
where
    P::Event: InputEvent,
{
    let mut failures = Vec::new();
    if !run.drain.passed {
        failures.push(format!("drain did not pass: {:?}", run.drain.failures));
    }
    check_counters(&run.counters, &expected["counters"], &mut failures);
    for (batch_key, watermark) in expected
        .get("watermarks")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|watermarks| watermarks.iter())
    {
        let batch = batch_key.parse::<u64>()?;
        let actual = run.trace.iter().find(|line| line.batch == batch);
        if actual.map(|line| line.watermark_after) != watermark.as_i64() {
            failures.push(format!(
                "watermark after batch {batch}: expected {watermark}, got {}",
                actual.map_or_else(
                    || "missing".to_string(),
                    |line| line.watermark_after.to_string()
                )
            ));
        }
    }
    for line in &run.trace {
        if line.harness_late != line.num_rows_dropped_by_watermark {
            failures.push(format!(
                "batch {}: harness_late {} != num_rows_dropped_by_watermark {}",
                line.batch, line.harness_late, line.num_rows_dropped_by_watermark
            ));
        }
    }
    check_receipts(run, fixture_path, &mut failures)?;
    Ok(failures)
}

fn check_counters(actual: &Counters, expected: &Value, failures: &mut Vec<String>) {
    for key in [
        "rejected",
        "late",
        "duplicate_events",
        "conflicting_duplicates",
    ] {
        let actual_value = match key {
            "rejected" => actual.rejected,
            "late" => actual.late,
            "duplicate_events" => actual.duplicate_events,
            _ => actual.conflicting_duplicates,
        };
        if expected.get(key).and_then(Value::as_u64) != Some(actual_value) {
            failures.push(format!(
                "counter {key}: expected {}, got {actual_value}",
                expected.get(key).unwrap_or(&Value::Null)
            ));
        }
    }
}

fn check_receipts<P: common::processor::Processor>(
    run: &ReplayRun<P>,
    fixture_path: &Path,
    failures: &mut Vec<String>,
) -> Result<()>
where
    P::Event: InputEvent,
{
    let events = json_lines(fixture_path)?;
    let trace_by_batch = run
        .trace
        .iter()
        .map(|line| (line.batch, line))
        .collect::<HashMap<_, _>>();
    let mut first_seen = HashMap::<(String, String), SeenEvent>::new();
    let mut arrival_by_id = HashMap::<(String, String), u64>::new();
    for row in &events[..events.len() - 1] {
        let sequence = row["arrival_seq"].as_u64().context("arrival_seq")?;
        let batch = row["batch"].as_u64().context("batch")?;
        let event = P::Event::from_fixture_json(&row["message"])?;
        let line = trace_by_batch
            .get(&batch)
            .context("missing fixture batch trace")?;
        if !(run.validate)(&event) {
            continue;
        }
        if line.watermark_in_effect > 0 && event.ts() <= line.watermark_in_effect {
            continue;
        }
        let key = (event.vin().to_owned(), event.event_id().to_owned());
        first_seen.entry(key.clone()).or_insert_with(|| SeenEvent {
            vin: event.vin().to_owned(),
            event_id: event.event_id().to_owned(),
            ts: event.ts(),
        });
        arrival_by_id.entry(key).or_insert(sequence);
    }

    let expected = first_seen
        .values()
        .map(|event| (event.vin.clone(), event.event_id.clone(), event.ts))
        .collect::<Vec<_>>();
    let mut received = Vec::new();
    for line in &run.trace {
        let mut released = Vec::<(String, String, i64, u64)>::new();
        for record in &line.records_emitted {
            if record.get("call").and_then(Value::as_str) != Some("on_event") {
                continue;
            }
            let vin = record["vin"].as_str().unwrap_or_default().to_string();
            let event_id = record["event_id"].as_str().unwrap_or_default().to_string();
            let ts = record["ts"].as_i64().unwrap_or_default();
            if vin == RESERVED_VIN {
                failures.push(format!(
                    "reserved control event was released in batch {}",
                    line.batch
                ));
                continue;
            }
            if ts > line.watermark_after {
                failures.push(format!(
                    "batch {} released event {event_id} at {ts} beyond watermark {}",
                    line.batch, line.watermark_after
                ));
            }
            let sequence = arrival_by_id
                .get(&(vin.clone(), event_id.clone()))
                .copied()
                .unwrap_or_default();
            released.push((vin.clone(), event_id.clone(), ts, sequence));
            received.push((vin, event_id, ts));
        }
        let mut by_vin = BTreeMap::<String, Vec<(i64, u64)>>::new();
        for (vin, _, ts, sequence) in released {
            by_vin.entry(vin).or_default().push((ts, sequence));
        }
        for (vin, events) in by_vin {
            let mut sorted = events.clone();
            sorted.sort();
            if events != sorted {
                failures.push(format!(
                    "batch {} releases VIN {vin} outside (ts, arrival_seq) order",
                    line.batch
                ));
            }
        }
    }
    received.sort();
    let mut expected = expected;
    expected.sort();
    if received != expected {
        failures.push(format!(
            "recorder received events differ: expected {expected:?}, got {received:?}"
        ));
    }
    Ok(())
}
