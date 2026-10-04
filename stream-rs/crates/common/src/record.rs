use serde_json::Value;

/// How a job's output record is written to `records.jsonl`, `outputs.json` and
/// the trace's `records_emitted`. Implemented next to the job's `Processor`, so
/// each job owns its JSON encoding (field names from the `.proto`, enum fields
/// as their value names, as the Spark harness writes them).
pub trait JsonRecord {
    fn to_json(&self) -> Value;
}
