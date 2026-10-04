use std::collections::{BTreeMap, HashMap};

use serde::Serialize;
use serde_json::Value;

use crate::event::InputEvent;
use crate::job::{JobSpec, RESERVED_VIN};
use crate::processor::Processor;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Counters {
    pub rejected: u64,
    pub late: u64,
    pub duplicate_events: u64,
    pub conflicting_duplicates: u64,
}

#[derive(Debug)]
pub struct BatchOutcome<O> {
    pub watermark_in_effect: Option<i64>,
    pub watermark_after: Option<i64>,
    pub late: u64,
    pub emitted: Vec<(String, O)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct VinDrain {
    pub vin: String,
    pub buffered: usize,
    pub open: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DrainReport {
    pub state_rows_total: usize,
    pub vins: Vec<VinDrain>,
}

impl DrainReport {
    pub fn is_drained(&self) -> bool {
        self.vins.iter().all(|entry| {
            if entry.vin == RESERVED_VIN {
                entry.buffered > 0
            } else {
                entry.buffered == 0 && !entry.open
            }
        })
    }
}

struct VinState<P: Processor> {
    seen: HashMap<String, P::Event>,
    buffer: Vec<(u64, P::Event)>,
    state: P::State,
}

pub struct Runner<P: Processor> {
    spec: JobSpec<P::Event>,
    processor: P,
    watermark: Option<i64>,
    max_valid_ts: Option<i64>,
    states: BTreeMap<String, VinState<P>>,
    counters: Counters,
}

impl<P: Processor> Runner<P> {
    pub fn new(spec: JobSpec<P::Event>, processor: P) -> Self {
        Self {
            spec,
            processor,
            watermark: None,
            max_valid_ts: None,
            states: BTreeMap::new(),
            counters: Counters::default(),
        }
    }

    pub fn process_batch(&mut self, batch: Vec<(u64, P::Event)>) -> BatchOutcome<P::Output> {
        let watermark_in_effect = self.watermark;
        let mut groups = BTreeMap::<String, Vec<(u64, P::Event)>>::new();
        let mut late = 0;

        for (arrival_seq, event) in batch {
            if !(self.spec.validate)(&event) {
                if event.vin() != RESERVED_VIN {
                    self.counters.rejected += 1;
                }
                continue;
            }

            self.max_valid_ts = Some(
                self.max_valid_ts
                    .map_or(event.ts(), |max_ts| max_ts.max(event.ts())),
            );
            if watermark_in_effect.is_some_and(|watermark| event.ts() <= watermark) {
                late += 1;
                if event.vin() != RESERVED_VIN {
                    self.counters.late += 1;
                }
                continue;
            }
            groups
                .entry(event.vin().to_owned())
                .or_default()
                .push((arrival_seq, event));
        }

        for (vin, events) in &mut groups {
            events.sort_by_key(|(arrival_seq, _)| *arrival_seq);
            let state = self.states.entry(vin.clone()).or_insert_with(|| VinState {
                seen: HashMap::new(),
                buffer: Vec::new(),
                state: P::State::default(),
            });
            for (arrival_seq, event) in events.drain(..) {
                if let Some(first) = state.seen.get(event.event_id()) {
                    if vin != RESERVED_VIN {
                        if first == &event {
                            self.counters.duplicate_events += 1;
                        } else {
                            self.counters.conflicting_duplicates += 1;
                        }
                    }
                    continue;
                }
                state
                    .seen
                    .insert(event.event_id().to_owned(), event.clone());
                state.buffer.push((arrival_seq, event));
            }
        }

        if let Some(max_valid_ts) = self.max_valid_ts {
            let new_watermark = max_valid_ts - self.spec.delay_ms;
            self.watermark = Some(
                self.watermark
                    .map_or(new_watermark, |watermark| watermark.max(new_watermark)),
            );
        }

        let mut emitted = Vec::new();
        if let Some(watermark) = self.watermark {
            for (vin, state) in &mut self.states {
                let mut pending = Vec::new();
                for (arrival_seq, event) in std::mem::take(&mut state.buffer) {
                    if event.ts() <= watermark {
                        pending.push((arrival_seq, event));
                    } else {
                        state.buffer.push((arrival_seq, event));
                    }
                }
                pending.sort_by_key(|(arrival_seq, event)| (event.ts(), *arrival_seq));
                for (_, event) in pending {
                    emitted.extend(
                        self.processor
                            .on_event(&event, &mut state.state)
                            .into_iter()
                            .map(|output| (vin.clone(), output)),
                    );
                }
                emitted.extend(
                    self.processor
                        .on_watermark(vin, watermark, &mut state.state)
                        .into_iter()
                        .map(|output| (vin.clone(), output)),
                );
            }
        }

        BatchOutcome {
            watermark_in_effect,
            watermark_after: self.watermark,
            late,
            emitted,
        }
    }

    pub fn record_decode_rejection(&mut self, vin: Option<&str>) {
        if vin != Some(RESERVED_VIN) {
            self.counters.rejected += 1;
        }
    }

    pub fn counters(&self) -> &Counters {
        &self.counters
    }

    /// The runner's counters followed by the processor's (`Processor::counters`),
    /// as one JSON object in that key order: the contract's counters file.
    pub fn counters_json(&self) -> Value {
        let mut counters = serde_json::to_value(&self.counters)
            .expect("Counters serialize")
            .as_object()
            .cloned()
            .expect("Counters serialize as an object");
        for (key, value) in self.processor.counters() {
            counters.insert(key.to_owned(), value);
        }
        Value::Object(counters)
    }

    pub fn processor(&self) -> &P {
        &self.processor
    }

    pub fn watermark(&self) -> Option<i64> {
        self.watermark
    }

    pub fn drain_report(&self) -> DrainReport {
        DrainReport {
            state_rows_total: self.states.len(),
            vins: self
                .states
                .iter()
                .map(|(vin, state)| VinDrain {
                    vin: vin.clone(),
                    buffered: state.buffer.len(),
                    open: self.processor.is_open(&state.state),
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::CHARGING;
    use crate::proto::charging::{ChargingEvent, ChargingEventType};
    use serde::Serialize;

    #[derive(Default)]
    struct Recorder {
        events: Vec<(String, i64, u64)>,
        is_open: bool,
    }

    #[derive(Default)]
    struct Recorded;

    #[derive(Serialize)]
    struct Receipt {
        vin: String,
        event_id: String,
        ts: i64,
    }

    impl Processor for Recorder {
        type Event = ChargingEvent;
        type State = Recorded;
        type Output = Receipt;

        fn on_event(&mut self, event: &Self::Event, _state: &mut Self::State) -> Vec<Self::Output> {
            self.events
                .push((event.event_id.clone(), event.ts, event.energy_wh as u64));
            vec![Receipt {
                vin: event.vin.clone(),
                event_id: event.event_id.clone(),
                ts: event.ts,
            }]
        }

        fn on_watermark(
            &mut self,
            _vin: &str,
            _watermark_ms: i64,
            _state: &mut Self::State,
        ) -> Vec<Self::Output> {
            Vec::new()
        }

        fn is_open(&self, _state: &Self::State) -> bool {
            self.is_open
        }
    }

    fn event(id: u8, ts: i64, event: ChargingEventType, energy_wh: i64) -> ChargingEvent {
        ChargingEvent {
            event_id: format!("00000000-0000-4000-8000-0000000000{id:02x}"),
            vin: "TST00000000000001".to_string(),
            ts,
            event: event as i32,
            energy_wh,
            lat: (event == ChargingEventType::PlugIn).then_some(0.0),
            lon: (event == ChargingEventType::PlugIn).then_some(0.0),
            charger_type: (event == ChargingEventType::PlugIn).then(|| "type".to_string()),
        }
    }

    fn runner() -> Runner<Recorder> {
        Runner::new(CHARGING, Recorder::default())
    }

    #[test]
    fn deduplication_uses_lowest_arrival_sequence_not_input_order() {
        let mut runner = runner();
        let duplicate = event(1, 1_600_000_000_000, ChargingEventType::Progress, 20);
        let first = event(1, 1_600_000_000_000, ChargingEventType::Progress, 10);
        runner.process_batch(vec![(2, duplicate), (1, first)]);
        assert_eq!(runner.counters().conflicting_duplicates, 1);
        assert_eq!(
            runner.states["TST00000000000001"].seen["00000000-0000-4000-8000-000000000001"]
                .energy_wh,
            10
        );
    }

    #[test]
    fn equal_timestamp_release_uses_arrival_sequence() {
        let mut runner = runner();
        let ts = 1_600_000_000_000;
        let outcome = runner.process_batch(vec![
            (9, event(2, ts, ChargingEventType::Progress, 0)),
            (3, event(1, ts, ChargingEventType::Progress, 0)),
            (
                10,
                event(
                    3,
                    ts + CHARGING.delay_ms + 1,
                    ChargingEventType::Progress,
                    0,
                ),
            ),
        ]);
        let ids = outcome
            .emitted
            .into_iter()
            .map(|(_, receipt)| receipt.event_id)
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![
                event(1, ts, ChargingEventType::Progress, 0).event_id,
                event(2, ts, ChargingEventType::Progress, 0).event_id
            ]
        );
    }

    #[test]
    fn watermark_is_fixed_for_the_whole_batch() {
        let mut runner = runner();
        runner.process_batch(vec![(
            1,
            event(1, 1_600_000_000_000, ChargingEventType::Progress, 0),
        )]);
        let old_watermark = runner.watermark().unwrap();
        let outcome = runner.process_batch(vec![
            (2, event(2, old_watermark, ChargingEventType::Progress, 0)),
            (
                3,
                event(
                    3,
                    old_watermark + CHARGING.delay_ms + 10_000,
                    ChargingEventType::Progress,
                    0,
                ),
            ),
        ]);
        assert_eq!(outcome.watermark_in_effect, Some(old_watermark));
        assert_eq!(outcome.late, 1);
        assert_eq!(runner.watermark(), Some(old_watermark + 10_000));
    }

    #[test]
    fn rejected_timestamp_does_not_advance_watermark() {
        let mut runner = runner();
        let mut invalid = event(1, 4_000_000_000_000, ChargingEventType::Progress, 0);
        invalid.event_id = "BAD".to_string();
        runner.process_batch(vec![(1, invalid)]);
        assert_eq!(runner.watermark(), None);
        assert_eq!(runner.counters().rejected, 1);
    }

    #[test]
    fn late_copy_does_not_reserve_id() {
        let mut runner = runner();
        runner.process_batch(vec![(
            1,
            event(1, 1_600_000_000_000, ChargingEventType::Progress, 0),
        )]);
        let late_ts = runner.watermark().unwrap();
        let mut late = event(2, late_ts, ChargingEventType::Progress, 0);
        late.event_id = "00000000-0000-4000-8000-000000000099".to_string();
        runner.process_batch(vec![(2, late.clone())]);
        late.ts = late_ts + 600_001;
        let anchor = event(
            3,
            late.ts + CHARGING.delay_ms + 1,
            ChargingEventType::Progress,
            0,
        );
        let outcome = runner.process_batch(vec![(3, late.clone()), (4, anchor)]);
        assert_eq!(runner.counters().late, 1);
        assert_eq!(runner.counters().duplicate_events, 0);
        assert!(runner
            .drain_report()
            .vins
            .iter()
            .any(|entry| entry.vin == late.vin));
        assert!(outcome
            .emitted
            .iter()
            .any(|(_, receipt)| receipt.event_id == late.event_id));
    }

    #[test]
    fn drain_rejects_non_reserved_buffered_events() {
        let mut runner = runner();
        runner.process_batch(vec![(
            1,
            event(1, 1_600_000_000_000, ChargingEventType::Progress, 0),
        )]);
        assert!(!runner.drain_report().is_drained());
        let report = runner.drain_report();
        assert!(report
            .vins
            .iter()
            .any(|entry| entry.vin != RESERVED_VIN && entry.buffered > 0));
        assert!(report.state_rows_total > 0);
    }

    #[test]
    fn drain_detects_open_processor_state() {
        let recorder = Recorder {
            is_open: true,
            ..Recorder::default()
        };
        let mut runner = Runner::new(CHARGING, recorder);
        runner.process_batch(vec![(
            1,
            event(1, 1_600_000_000_000, ChargingEventType::Progress, 0),
        )]);
        runner
            .states
            .get_mut("TST00000000000001")
            .unwrap()
            .buffer
            .clear();
        assert!(!runner.drain_report().is_drained());
        assert!(runner.drain_report().vins[0].open);
    }
}
