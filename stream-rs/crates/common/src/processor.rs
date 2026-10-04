use serde_json::Value;

use crate::event::InputEvent;

pub trait Processor {
    type Event: InputEvent;
    type State: Default;
    type Output;

    fn on_event(&mut self, event: &Self::Event, state: &mut Self::State) -> Vec<Self::Output>;
    fn on_watermark(
        &mut self,
        vin: &str,
        watermark_ms: i64,
        state: &mut Self::State,
    ) -> Vec<Self::Output>;
    fn is_open(&self, state: &Self::State) -> bool;

    /// The job's own counters, written after the runner's four (`rejected`,
    /// `late`, `duplicate_events`, `conflicting_duplicates`) in `counters.json`
    /// and `outputs.json`, in this order. The processor instance lives for the
    /// whole run, so it can tally them itself. Default: none.
    fn counters(&self) -> Vec<(&'static str, Value)> {
        Vec::new()
    }
}
