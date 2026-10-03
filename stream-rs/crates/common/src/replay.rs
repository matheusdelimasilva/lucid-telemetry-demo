use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;

use crate::event::InputEvent;
use crate::job::{JobSpec, RESERVED_VIN};
use crate::processor::Processor;
use crate::record::JsonRecord;
use crate::runner::{Counters, DrainReport, Runner};

const FLUSH_EXTENSION_MS: i64 = 3_600_000;

#[derive(Clone, Debug, Serialize)]
pub struct TraceLine {
    pub batch: u64,
    pub kind: &'static str,
    pub arrival_seqs: Vec<u64>,
    pub watermark_in_effect: i64,
    pub watermark_after: i64,
    pub num_rows_dropped_by_watermark: u64,
    pub harness_late: u64,
    pub records_emitted: Vec<Value>,
}

/// Same shape as the Spark harness's `drain.json`. Spark reports each VIN's last
/// state marker (`busy`) and the batch it was emitted in; the Rust runner visits
/// every VIN's state in every batch, so `batch` is the final flush batch for all.
#[derive(Clone, Debug, Serialize)]
pub struct DrainArtifact {
    pub passed: bool,
    pub final_watermark: i64,
    pub target_watermark: i64,
    pub state_rows_total_last_progress: usize,
    pub vins_reached_state_plus_reserved: usize,
    pub last_markers: Vec<LastMarker>,
    pub failures: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct LastMarker {
    pub vin: String,
    pub batch: u64,
    pub busy: bool,
}

#[derive(Serialize)]
struct OutputsArtifact<'a> {
    job: &'static str,
    records: &'a [Value],
    counters: &'a Counters,
}

pub struct ReplayRun<P: Processor> {
    pub job: &'static str,
    pub validate: fn(&P::Event) -> bool,
    pub trace: Vec<TraceLine>,
    pub drain: DrainArtifact,
    pub outputs: Vec<Value>,
    pub counters: Counters,
    /// Harness self-check failures (the Spark harness's `failures`): empty means
    /// the replay's own `result.json` status is PASS.
    pub failures: Vec<String>,
    _processor: std::marker::PhantomData<P>,
}

/// Records are written sorted by `output_id` (string order), as the Spark
/// harness does; records without one keep their emission order.
pub fn sort_records(records: &mut [Value]) {
    records.sort_by(|a, b| {
        let key = |v: &Value| {
            v.get("output_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
        };
        key(a).cmp(&key(b))
    });
}

impl<P: Processor> ReplayRun<P> {
    pub fn write(&self, out_dir: &Path) -> Result<()> {
        fs::create_dir_all(out_dir)
            .with_context(|| format!("creating replay output directory {}", out_dir.display()))?;
        let trace = self
            .trace
            .iter()
            .map(serde_json::to_string)
            .collect::<serde_json::Result<Vec<_>>>()?
            .join("\n");
        fs::write(out_dir.join("trace.jsonl"), format!("{trace}\n"))
            .with_context(|| format!("writing {}", out_dir.join("trace.jsonl").display()))?;

        write_json(out_dir.join("drain.json"), &self.drain)?;
        let mut records = self.outputs.clone();
        sort_records(&mut records);
        write_json(
            out_dir.join("outputs.json"),
            &OutputsArtifact {
                job: self.job,
                records: &records,
                counters: &self.counters,
            },
        )
    }
}

#[derive(Clone)]
struct FixtureEvent<E> {
    arrival_seq: u64,
    batch: u64,
    event: E,
}

struct Fixture<E> {
    events: Vec<FixtureEvent<E>>,
    target_watermark: i64,
    flush_batch: u64,
}

pub fn run_fixture<P>(path: &Path, spec: JobSpec<P::Event>, processor: P) -> Result<ReplayRun<P>>
where
    P: Processor,
    P::Output: JsonRecord,
{
    let fixture = parse_fixture::<P::Event>(path)?;
    let valid_ts = fixture
        .events
        .iter()
        .filter(|row| (spec.validate)(&row.event))
        .map(|row| row.event.ts())
        .collect::<Vec<_>>();
    let largest_valid_ts = valid_ts
        .into_iter()
        .max()
        .context("fixture has no contract-valid input events")?;
    let expected_target = largest_valid_ts + FLUSH_EXTENSION_MS;
    anyhow::ensure!(
        fixture.target_watermark == expected_target,
        "flush target {} does not equal largest valid fixture timestamp plus one hour ({expected_target})",
        fixture.target_watermark
    );

    let validate = spec.validate;
    let control_event = spec.control_event;
    let job = spec.name;
    let mut runner = Runner::new(spec, processor);
    let mut trace = Vec::new();
    let mut reached_vins = BTreeSet::new();
    let max_arrival_seq = fixture.events.last().map_or(0, |row| row.arrival_seq);

    for batch_num in 1..fixture.flush_batch {
        let rows = fixture
            .events
            .iter()
            .filter(|row| row.batch == batch_num)
            .collect::<Vec<_>>();
        let in_effect = runner.watermark();
        let mut harness_late = 0;
        for row in &rows {
            if validate(&row.event) {
                let is_late = in_effect.is_some_and(|wm| wm > 0 && row.event.ts() <= wm);
                if is_late {
                    harness_late += 1;
                } else if row.event.vin() != RESERVED_VIN {
                    reached_vins.insert(row.event.vin().to_owned());
                }
            }
        }
        let outcome = runner.process_batch(
            rows.iter()
                .map(|row| (row.arrival_seq, row.event.clone()))
                .collect(),
        );
        anyhow::ensure!(
            harness_late == outcome.late,
            "batch {batch_num}: harness_late {harness_late} != runner late {}",
            outcome.late
        );
        trace.push(trace_line(
            batch_num,
            "events",
            rows.iter().map(|row| row.arrival_seq).collect(),
            harness_late,
            outcome,
        )?);
    }

    let control = control_event(fixture.target_watermark);
    let control_seq = max_arrival_seq + 1;
    let control_batch = runner.process_batch(vec![(control_seq, control)]);
    trace.push(trace_line(
        fixture.flush_batch,
        "flush_control",
        vec![control_seq],
        0,
        control_batch,
    )?);

    let empty = runner.process_batch(Vec::new());
    trace.push(trace_line(
        fixture.flush_batch + 1,
        "flush_empty",
        Vec::new(),
        0,
        empty,
    )?);

    let drain = drain_artifact(
        runner.drain_report(),
        runner.watermark().unwrap_or_default(),
        fixture.target_watermark,
        reached_vins.len() + 1,
        fixture.flush_batch + 1,
    );
    let mut failures = Vec::new();
    if !drain.passed {
        failures.push(format!(
            "drain check failed ({}); parity refuses to compare",
            drain.failures.join("; ")
        ));
    }
    let outputs = trace
        .iter()
        .flat_map(|line| line.records_emitted.iter().cloned())
        .collect::<Vec<_>>();

    Ok(ReplayRun {
        job,
        validate,
        trace,
        drain,
        outputs,
        counters: runner.counters().clone(),
        failures,
        _processor: std::marker::PhantomData,
    })
}

fn trace_line<O: JsonRecord>(
    batch: u64,
    kind: &'static str,
    arrival_seqs: Vec<u64>,
    harness_late: u64,
    outcome: crate::runner::BatchOutcome<O>,
) -> Result<TraceLine> {
    let records_emitted = outcome
        .emitted
        .iter()
        .filter(|(vin, _)| vin != RESERVED_VIN)
        .map(|(_, output)| output.to_json())
        .collect::<Vec<_>>();
    Ok(TraceLine {
        batch,
        kind,
        arrival_seqs,
        watermark_in_effect: outcome.watermark_in_effect.unwrap_or_default(),
        watermark_after: outcome.watermark_after.unwrap_or_default(),
        num_rows_dropped_by_watermark: outcome.late,
        harness_late,
        records_emitted,
    })
}

fn drain_artifact(
    report: DrainReport,
    final_watermark: i64,
    target_watermark: i64,
    vins_reached_state_plus_reserved: usize,
    last_batch: u64,
) -> DrainArtifact {
    let mut failures = Vec::new();
    if final_watermark != target_watermark {
        failures.push(format!(
            "final watermark {final_watermark} != target {target_watermark}"
        ));
    }
    let last_markers = report
        .vins
        .iter()
        .map(|entry| LastMarker {
            vin: entry.vin.clone(),
            batch: last_batch,
            busy: entry.buffered > 0 || entry.open,
        })
        .collect::<Vec<_>>();
    let busy_others = last_markers
        .iter()
        .filter(|marker| marker.busy && marker.vin != RESERVED_VIN)
        .map(|marker| marker.vin.as_str())
        .collect::<Vec<_>>();
    if !busy_others.is_empty() {
        failures.push(format!(
            "VINs still busy after the flush: {}",
            busy_others.join(",")
        ));
    }
    if !report
        .vins
        .iter()
        .any(|entry| entry.vin == RESERVED_VIN && entry.buffered > 0)
    {
        failures.push("reserved VIN's last marker is not busy = true".to_string());
    }
    if report.state_rows_total != vins_reached_state_plus_reserved {
        failures.push(format!(
            "state rows total {} != VINs that reached state + 1 ({vins_reached_state_plus_reserved})",
            report.state_rows_total
        ));
    }
    DrainArtifact {
        passed: failures.is_empty(),
        final_watermark,
        target_watermark,
        state_rows_total_last_progress: report.state_rows_total,
        vins_reached_state_plus_reserved,
        last_markers,
        failures,
    }
}

fn parse_fixture<E: InputEvent>(path: &Path) -> Result<Fixture<E>> {
    let input = fs::read_to_string(path)
        .with_context(|| format!("reading replay fixture {}", path.display()))?;
    let lines = input
        .lines()
        .enumerate()
        .map(|(index, line)| {
            serde_json::from_str::<Value>(line)
                .with_context(|| format!("parsing JSON on fixture line {}", index + 1))
        })
        .collect::<Result<Vec<_>>>()?;
    anyhow::ensure!(!lines.is_empty(), "replay fixture is empty");

    let flush = lines.last().context("replay fixture is empty")?;
    exact_keys(flush, &["advance_watermark_to", "batch"], "flush line")?;
    let target_watermark = int64(flush, "advance_watermark_to")?;
    let flush_batch = positive_u64(flush, "batch")?;
    let mut events = Vec::with_capacity(lines.len() - 1);
    let mut expected_seq = 1;
    let mut previous_batch = 0_u64;
    for (index, line) in lines[..lines.len() - 1].iter().enumerate() {
        exact_keys(line, &["arrival_seq", "batch", "message"], "event line")?;
        let arrival_seq = positive_u64(line, "arrival_seq")?;
        anyhow::ensure!(
            arrival_seq == expected_seq,
            "fixture line {} has arrival_seq {arrival_seq}, expected {expected_seq}",
            index + 1
        );
        expected_seq += 1;
        let batch = positive_u64(line, "batch")?;
        let next_allowed_batch = previous_batch.checked_add(1);
        anyhow::ensure!(
            (index == 0 && batch == 1)
                || (index > 0
                    && batch >= previous_batch
                    && next_allowed_batch.is_some_and(|next| batch <= next)),
            "fixture line {} has invalid batch {batch} after batch {previous_batch}",
            index + 1
        );
        previous_batch = batch;
        let message = line
            .get("message")
            .context("event line is missing message")?;
        events.push(FixtureEvent {
            arrival_seq,
            batch,
            event: E::from_fixture_json(message)
                .with_context(|| format!("decoding fixture message on line {}", index + 1))?,
        });
    }
    anyhow::ensure!(!events.is_empty(), "fixture has no event lines");
    anyhow::ensure!(
        previous_batch.checked_add(1) == Some(flush_batch) && flush_batch < u64::MAX,
        "flush batch {flush_batch} must equal last event batch plus one ({})",
        previous_batch + 1
    );
    Ok(Fixture {
        events,
        target_watermark,
        flush_batch,
    })
}

fn exact_keys(value: &Value, expected: &[&str], name: &str) -> Result<()> {
    let object = value
        .as_object()
        .with_context(|| format!("{name} must be a JSON object"))?;
    anyhow::ensure!(
        object.len() == expected.len() && object.keys().all(|key| expected.contains(&key.as_str())),
        "{name} must have exactly these keys: {}",
        expected.join(", ")
    );
    Ok(())
}

fn positive_u64(value: &Value, key: &str) -> Result<u64> {
    let number = value
        .get(key)
        .with_context(|| format!("missing integer field {key}"))?
        .as_u64()
        .with_context(|| format!("field {key} must be an unsigned integer"))?;
    anyhow::ensure!(number > 0, "field {key} must be at least 1");
    Ok(number)
}

fn int64(value: &Value, key: &str) -> Result<i64> {
    value
        .get(key)
        .with_context(|| format!("missing integer field {key}"))?
        .as_i64()
        .with_context(|| format!("field {key} must be an int64 JSON integer"))
}

fn write_json(path: std::path::PathBuf, value: &impl Serialize) -> Result<()> {
    let mut contents = serde_json::to_string_pretty(value)?;
    contents.push('\n');
    fs::write(&path, contents).with_context(|| format!("writing {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_keys_are_strict() {
        assert!(exact_keys(
            &serde_json::json!({"arrival_seq":1,"batch":1,"message":{},"extra":1}),
            &["arrival_seq", "batch", "message"],
            "event line"
        )
        .is_err());
    }

    #[test]
    fn positive_fields_reject_floats_and_zero() {
        assert!(positive_u64(&serde_json::json!({"batch":0}), "batch").is_err());
        assert!(positive_u64(&serde_json::json!({"batch":1.0}), "batch").is_err());
    }
}
