mod support;

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::Context;
use common::job::{BATTERY, CHARGING};
use common::replay::{run_fixture, TraceLine};
use serde_json::Value;
use support::{case_dirs, json_lines, parse_expected, repo_root, Recorder};

#[test]
#[ignore]
fn spark_trace_matches_rust_replay() {
    let spark_dir = std::env::var("SPARK_EXAMPLES_DIR")
        .expect("SPARK_EXAMPLES_DIR must point to build/spark-examples");
    let spark_dir = Path::new(&spark_dir);
    let root = repo_root();
    let mut failures = Vec::new();
    let mut compared_examples = 0;
    let mut compared_probes = 0;

    for suite in ["examples", "probes"] {
        let dirs = case_dirs(&root, suite).expect("discover fixture directories");
        for case_dir in dirs {
            let case = case_dir.file_name().unwrap().to_string_lossy();
            let expected = parse_expected(&case_dir.join("expected.json"))
                .unwrap_or_else(|error| panic!("{suite}/{case}: {error:#}"));
            let spark_path = spark_dir
                .join(suite)
                .join(case.as_ref())
                .join("trace.jsonl");
            if !spark_path.is_file() {
                panic!("missing Spark trace {}", spark_path.display());
            }
            let spark_trace =
                json_lines(&spark_path).unwrap_or_else(|error| panic!("{suite}/{case}: {error:#}"));
            let rust_trace = match expected["job"].as_str().context("expected job") {
                Ok("charging-sessions") => {
                    run_fixture(&case_dir.join("input.jsonl"), CHARGING, Recorder::default())
                        .map(|run| run.trace)
                }
                Ok("battery-health") => {
                    run_fixture(&case_dir.join("input.jsonl"), BATTERY, Recorder::default())
                        .map(|run| run.trace)
                }
                Ok(job) => Err(anyhow::anyhow!("unknown job {job:?}")),
                Err(error) => Err(error),
            }
            .unwrap_or_else(|error| panic!("{suite}/{case}: {error:#}"));
            let case_failures = compare_trace(&rust_trace, &spark_trace);
            match suite {
                "examples" => compared_examples += 1,
                "probes" => compared_probes += 1,
                _ => unreachable!(),
            }
            if case_failures.is_empty() {
                println!("PASS {suite}/{case}");
            } else {
                failures.extend(
                    case_failures
                        .into_iter()
                        .map(|failure| format!("{suite}/{case}: {failure}")),
                );
            }
        }
    }
    assert_eq!(compared_examples, 16, "expected to compare all 16 examples");
    assert_eq!(compared_probes, 5, "expected to compare all 5 probes");
    assert!(
        failures.is_empty(),
        "Rust/Spark trace mismatches:\n{}",
        failures.join("\n")
    );
}

fn compare_trace(rust: &[TraceLine], spark: &[Value]) -> Vec<String> {
    let mut failures = Vec::new();
    let rust_by_batch = rust
        .iter()
        .map(|line| (line.batch, line))
        .collect::<BTreeMap<_, _>>();
    let spark_by_batch = spark
        .iter()
        .filter_map(|line| {
            line.get("batch")
                .and_then(Value::as_u64)
                .map(|batch| (batch, line))
        })
        .collect::<BTreeMap<_, _>>();
    if rust_by_batch.keys().collect::<Vec<_>>() != spark_by_batch.keys().collect::<Vec<_>>() {
        failures.push(format!(
            "batch sets differ: Rust {:?}, Spark {:?}",
            rust_by_batch.keys().collect::<Vec<_>>(),
            spark_by_batch.keys().collect::<Vec<_>>()
        ));
    }
    let mut rust_total_late = 0;
    let mut spark_total_late = 0;
    for (batch, rust_line) in &rust_by_batch {
        let Some(spark_line) = spark_by_batch.get(batch) else {
            continue;
        };
        for (field, rust_value) in [
            ("kind", serde_json::json!(rust_line.kind)),
            (
                "arrival_seqs",
                serde_json::json!(rust_line.arrival_seqs.clone()),
            ),
            (
                "watermark_in_effect",
                serde_json::json!(rust_line.watermark_in_effect),
            ),
            (
                "watermark_after",
                serde_json::json!(rust_line.watermark_after),
            ),
        ] {
            if spark_line.get(field) != Some(&rust_value) {
                failures.push(format!(
                    "batch {batch} field {field}: Rust {rust_value}, Spark {}",
                    spark_line.get(field).unwrap_or(&Value::Null)
                ));
            }
        }
        let spark_dropped = spark_line
            .get("num_rows_dropped_by_watermark")
            .and_then(Value::as_u64)
            .unwrap_or_else(|| {
                panic!(
                    "batch {batch}: Spark trace line missing integer num_rows_dropped_by_watermark"
                )
            });
        let spark_harness_late = spark_line
            .get("harness_late")
            .and_then(Value::as_u64)
            .unwrap_or_else(|| {
                panic!("batch {batch}: Spark trace line missing integer harness_late")
            });
        if rust_line.num_rows_dropped_by_watermark != spark_dropped
            || spark_dropped != spark_harness_late
        {
            failures.push(format!(
                "batch {batch} late counts: Rust {}, Spark dropped {}, Spark harness_late {}",
                rust_line.num_rows_dropped_by_watermark, spark_dropped, spark_harness_late
            ));
        }
        rust_total_late += rust_line.num_rows_dropped_by_watermark;
        spark_total_late += spark_dropped;
    }
    if rust_total_late != spark_total_late {
        failures.push(format!(
            "total late differs: Rust {rust_total_late}, Spark {spark_total_late}"
        ));
    }
    failures
}
