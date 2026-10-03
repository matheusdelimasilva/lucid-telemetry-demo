//! Extra check, batch by batch, against the frozen Spark trace
//! (`parity/golden/battery-health.trace.jsonl`): watermark in effect,
//! watermark after, late count and the records emitted. `tools/parity.py`
//! remains the grader; this only localises a divergence to a batch.

mod support;

use anyhow::Result;
use battery_health::BatteryHealth;
use common::job::BATTERY;
use common::replay::run_fixture;
use serde_json::Value;
use support::{json_lines, repo_root};

const ABS_TOLERANCE: f64 = 1e-9;

#[test]
fn fixture_trace_matches_the_golden_spark_trace_batch_by_batch() -> Result<()> {
    let root = repo_root();
    let run = run_fixture(
        &root.join("parity/replay/battery-health.jsonl"),
        BATTERY,
        BatteryHealth,
    )?;
    let golden = json_lines(&root.join("parity/golden/battery-health.trace.jsonl"))?;
    assert_eq!(golden.len(), 41, "golden trace has 41 batches");
    assert_eq!(run.trace.len(), golden.len(), "batch count");

    let mut failures = Vec::new();
    let mut total_records = 0;
    for (line, expected) in run.trace.iter().zip(&golden) {
        let batch = line.batch;
        assert_eq!(Some(batch), expected["batch"].as_u64(), "batch numbering");
        for (name, actual, golden_value) in [
            ("kind", Value::from(line.kind), &expected["kind"]),
            (
                "watermark_in_effect",
                Value::from(line.watermark_in_effect),
                &expected["watermark_in_effect"],
            ),
            (
                "watermark_after",
                Value::from(line.watermark_after),
                &expected["watermark_after"],
            ),
            (
                "num_rows_dropped_by_watermark",
                Value::from(line.num_rows_dropped_by_watermark),
                &expected["num_rows_dropped_by_watermark"],
            ),
            (
                "harness_late",
                Value::from(line.harness_late),
                &expected["harness_late"],
            ),
        ] {
            if actual != *golden_value {
                failures.push(format!(
                    "batch {batch} {name}: golden {golden_value} vs rust {actual}"
                ));
            }
        }

        let golden_records = by_output_id(expected["records_emitted"].as_array().unwrap());
        let rust_records = by_output_id(&line.records_emitted);
        total_records += rust_records.len();
        for (id, golden_record) in &golden_records {
            match rust_records.iter().find(|(rust_id, _)| rust_id == id) {
                None => failures.push(format!("batch {batch} record {id}: not emitted by rust")),
                Some((_, rust_record)) => failures.extend(
                    record_diffs(golden_record, rust_record)
                        .into_iter()
                        .map(|diff| format!("batch {batch} record {id}: {diff}")),
                ),
            }
        }
        for (id, _) in &rust_records {
            if !golden_records.iter().any(|(golden_id, _)| golden_id == id) {
                failures.push(format!(
                    "batch {batch} record {id}: emitted by rust, not in golden"
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} differences from the golden trace:\n{}",
        failures.len(),
        failures.join("\n")
    );
    assert_eq!(total_records, 2991, "records emitted across all batches");
    Ok(())
}

fn by_output_id(records: &[Value]) -> Vec<(String, &Value)> {
    let mut indexed = records
        .iter()
        .map(|record| {
            (
                record["output_id"].as_str().unwrap_or("").to_owned(),
                record,
            )
        })
        .collect::<Vec<_>>();
    indexed.sort_by(|a, b| a.0.cmp(&b.0));
    indexed
}

fn record_diffs(golden: &Value, rust: &Value) -> Vec<String> {
    let (Some(golden), Some(rust)) = (golden.as_object(), rust.as_object()) else {
        return vec!["record is not an object".to_string()];
    };
    let mut diffs = Vec::new();
    for (field, golden_value) in golden {
        let Some(rust_value) = rust.get(field) else {
            diffs.push(format!("{field}: missing"));
            continue;
        };
        let same = match (golden_value.as_f64(), rust_value.as_f64()) {
            (Some(g), Some(r)) if golden_value.is_number() && rust_value.is_number() => {
                golden_value.is_f64() == rust_value.is_f64() && (g - r).abs() <= ABS_TOLERANCE
            }
            _ => golden_value == rust_value,
        };
        if !same {
            diffs.push(format!(
                "{field}: golden {golden_value} vs rust {rust_value}"
            ));
        }
    }
    for field in rust.keys() {
        if !golden.contains_key(field) {
            diffs.push(format!("{field}: not in golden"));
        }
    }
    diffs
}
