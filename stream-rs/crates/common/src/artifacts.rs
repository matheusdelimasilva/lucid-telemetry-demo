//! The files a job binary writes, in the Spark replay harness's shapes
//! (`stream-rs/jobs/README.md`). Job-agnostic: everything job-specific comes
//! from the `JobSpec` and the `JsonRecord` output.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{json, Value};

use crate::hash::sha256_hex;
use crate::job::JobSpec;
use crate::processor::Processor;
use crate::record::JsonRecord;
use crate::replay::{sort_records, ReplayRun, TraceLine};
use crate::runner::Counters;

pub const ENGINE: &str = "rust";
const TOOLCHAIN_TOML: &str = include_str!("../../../rust-toolchain.toml");
/// Informational comparison only (`tools/suite_check.py` grades with
/// `tools/compare_rules.json`); any numeric field may differ by this much.
const INFORMATIONAL_ABS_TOLERANCE: f64 = 1e-9;

#[derive(Serialize)]
struct ResultArtifact<'a> {
    job: &'a str,
    status: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    drain: Option<&'a crate::replay::DrainArtifact>,
    failures: &'a [String],
}

#[derive(Serialize)]
struct RunArtifact<'a> {
    job: &'a str,
    engine: &'a str,
    fixture_sha256: String,
    output_topics: [&'a str; 1],
    watermark_delay_ms: i64,
    replay: Value,
}

/// Fixture mode: `<out>/<job>.{records.jsonl,counters.json,trace.jsonl,drain.json,result.json,run.json}`.
/// Returns the harness status (`PASS` or `FAIL`).
pub fn write_fixture_artifacts<P>(
    run: &ReplayRun<P>,
    spec: &JobSpec<P::Event>,
    fixture_path: &Path,
    out_dir: &Path,
) -> Result<&'static str>
where
    P: Processor,
    P::Output: JsonRecord,
{
    fs::create_dir_all(out_dir)
        .with_context(|| format!("creating output directory {}", out_dir.display()))?;
    let job = spec.name;
    let file = |suffix: &str| out_dir.join(format!("{job}.{suffix}"));

    let mut records = run.outputs.clone();
    sort_records(&mut records);
    write_jsonl(&file("records.jsonl"), &records)?;
    write_json(&file("counters.json"), &run.counters)?;
    write_jsonl(&file("trace.jsonl"), &run.trace)?;
    write_json(&file("drain.json"), &run.drain)?;

    let status = if run.failures.is_empty() {
        "PASS"
    } else {
        "FAIL"
    };
    write_json(
        &file("result.json"),
        &ResultArtifact {
            job,
            status,
            drain: None,
            failures: &run.failures,
        },
    )?;

    let fixture_bytes = fs::read(fixture_path)
        .with_context(|| format!("reading fixture {}", fixture_path.display()))?;
    write_json(
        &file("run.json"),
        &RunArtifact {
            job,
            engine: ENGINE,
            fixture_sha256: sha256_hex(&fixture_bytes),
            output_topics: [spec.output_topic],
            watermark_delay_ms: spec.delay_ms,
            replay: json!({
                "toolchain_channel": toolchain_channel(),
                "common_version": env!("CARGO_PKG_VERSION"),
            }),
        },
    )?;
    Ok(status)
}

pub struct CaseOutcome {
    pub status: &'static str,
    pub failures: Vec<String>,
}

/// Suite mode, one case: `<out>/{outputs.json,trace.jsonl,drain.json,result.json}`.
/// `status` is `REFUSED` when the drain check failed (no comparison), else
/// `PASS`/`FAIL` from the informational comparison with `expected.json`.
pub fn write_case_artifacts<P>(
    run: &ReplayRun<P>,
    expected: &Value,
    out_dir: &Path,
) -> Result<CaseOutcome>
where
    P: Processor,
    P::Output: JsonRecord,
{
    run.write(out_dir)?;
    let refused = !run.drain.passed;
    let mut failures = run.failures.clone();
    if !refused {
        let mut records = run.outputs.clone();
        sort_records(&mut records);
        failures.extend(compare_expected(
            expected,
            &records,
            &run.counters,
            &run.trace,
        ));
    }
    let status = if refused {
        "REFUSED"
    } else if failures.is_empty() {
        "PASS"
    } else {
        "FAIL"
    };
    write_json(
        &out_dir.join("result.json"),
        &ResultArtifact {
            job: run.job,
            status,
            drain: Some(&run.drain),
            failures: &failures,
        },
    )?;
    Ok(CaseOutcome { status, failures })
}

/// Informational comparison with a case's `expected.json`: records by
/// `output_id`, counters, and `watermarks` (watermark after the given batches).
pub fn compare_expected(
    expected: &Value,
    records: &[Value],
    counters: &Counters,
    trace: &[TraceLine],
) -> Vec<String> {
    let mut failures = Vec::new();
    let id = |record: &Value| {
        record
            .get("output_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    let expected_records = expected
        .get("records")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let actual_ids = records.iter().map(id).collect::<Vec<_>>();
    for expected_record in &expected_records {
        let key = id(expected_record);
        match records.iter().find(|record| id(record) == key) {
            None => failures.push(format!("record {key}: missing")),
            Some(actual) => failures.extend(
                compare_record(expected_record, actual)
                    .into_iter()
                    .map(|diff| format!("record {key}: {diff}")),
            ),
        }
    }
    for key in &actual_ids {
        if !expected_records.iter().any(|record| id(record) == *key) {
            failures.push(format!("record {key}: unexpected"));
        }
    }
    let mut sorted_ids = actual_ids.clone();
    sorted_ids.sort();
    sorted_ids.dedup();
    if sorted_ids.len() != actual_ids.len() {
        failures.push("repeated output_id in records".to_string());
    }

    let actual_counters = serde_json::to_value(counters).unwrap_or(Value::Null);
    if let Some(expected_counters) = expected.get("counters") {
        if *expected_counters != actual_counters {
            failures.push(format!(
                "counters: expected {expected_counters} got {actual_counters}"
            ));
        }
    }

    for (batch, watermark) in expected
        .get("watermarks")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
    {
        let got = batch
            .parse::<u64>()
            .ok()
            .and_then(|batch| trace.iter().find(|line| line.batch == batch))
            .map(|line| line.watermark_after);
        if got != watermark.as_i64() {
            failures.push(format!(
                "watermark after batch {batch}: expected {watermark} got {}",
                got.map_or_else(|| "none".to_string(), |wm| wm.to_string())
            ));
        }
    }
    failures
}

fn compare_record(expected: &Value, actual: &Value) -> Vec<String> {
    let mut diffs = Vec::new();
    let (Some(expected), Some(actual)) = (expected.as_object(), actual.as_object()) else {
        return vec!["record is not a JSON object".to_string()];
    };
    for (field, expected_value) in expected {
        match actual.get(field) {
            None => diffs.push(format!("{field}: missing")),
            Some(actual_value) if !values_match(expected_value, actual_value) => diffs.push(
                format!("{field}: expected {expected_value} got {actual_value}"),
            ),
            Some(_) => {}
        }
    }
    for field in actual.keys() {
        if !expected.contains_key(field) {
            diffs.push(format!("{field}: unexpected field"));
        }
    }
    diffs
}

fn values_match(expected: &Value, actual: &Value) -> bool {
    match (expected.as_f64(), actual.as_f64()) {
        (Some(e), Some(a)) if expected.is_number() && actual.is_number() => {
            (e - a).abs() <= INFORMATIONAL_ABS_TOLERANCE
        }
        _ => expected == actual,
    }
}

/// `<nn>-<slug>/` case directories of a suite, sorted by name.
pub fn case_dirs(suite: &Path) -> Result<Vec<PathBuf>> {
    let mut dirs = fs::read_dir(suite)
        .with_context(|| format!("reading suite directory {}", suite.display()))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()?
        .into_iter()
        .filter(|path| {
            path.is_dir()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        let bytes = name.as_bytes();
                        bytes.len() >= 4
                            && bytes[0].is_ascii_digit()
                            && bytes[1].is_ascii_digit()
                            && bytes[2] == b'-'
                    })
        })
        .collect::<Vec<_>>();
    dirs.sort();
    Ok(dirs)
}

pub fn read_json(path: &Path) -> Result<Value> {
    serde_json::from_slice(&fs::read(path).with_context(|| format!("reading {}", path.display()))?)
        .with_context(|| format!("parsing JSON in {}", path.display()))
}

fn toolchain_channel() -> String {
    TOOLCHAIN_TOML
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once('=')?;
            (key.trim() == "channel").then(|| value.trim().trim_matches('"').to_string())
        })
        .unwrap_or_else(|| "unknown".to_string())
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut contents = serde_json::to_string_pretty(value)?;
    contents.push('\n');
    fs::write(path, contents).with_context(|| format!("writing {}", path.display()))
}

fn write_jsonl(path: &Path, values: &[impl Serialize]) -> Result<()> {
    let mut contents = String::new();
    for value in values {
        contents.push_str(&serde_json::to_string(value)?);
        contents.push('\n');
    }
    fs::write(path, contents).with_context(|| format!("writing {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn toolchain_channel_comes_from_rust_toolchain_toml() {
        assert!(TOOLCHAIN_TOML.contains(&format!("channel = \"{}\"", toolchain_channel())));
    }

    #[test]
    fn record_comparison_is_by_output_id_with_a_tiny_tolerance() {
        let expected = json!({
            "records": [{"output_id": "a", "x": 1.0, "s": "UNPLUG"}],
            "counters": {"rejected": 0, "late": 0, "duplicate_events": 0, "conflicting_duplicates": 0}
        });
        let ok = vec![json!({"output_id": "a", "x": 1.0 + 1e-12, "s": "UNPLUG"})];
        assert!(compare_expected(&expected, &ok, &Counters::default(), &[]).is_empty());
        let bad = vec![
            json!({"output_id": "a", "x": 1.1, "s": "UNPLUG", "extra": 1}),
            json!({"output_id": "b"}),
        ];
        let failures = compare_expected(&expected, &bad, &Counters::default(), &[]);
        assert_eq!(failures.len(), 3, "{failures:?}");
    }
}
