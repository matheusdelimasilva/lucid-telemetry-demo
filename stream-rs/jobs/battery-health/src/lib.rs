//! battery-health: one final record per (VIN, 5-minute window) from valid
//! `vehicle.battery.v1` readings (`contracts/battery-health.md`). Validation,
//! lateness, deduplication, watermarks and the per-VIN `(ts, arrival_seq)`
//! release order all live in `common::runner`; this crate only aggregates the
//! readings it is handed, by event time, and closes windows on watermarks.

use std::collections::BTreeMap;

use common::hash::sha256_hex;
use common::processor::Processor;
use common::proto::battery_input::BatteryReading;
use common::proto::battery_output::BatteryWindow;
use common::record::JsonRecord;
use prost::Message;
use serde_json::{json, Value};

/// Tumbling window size; windows are aligned to the Unix epoch.
pub const WINDOW_MS: i64 = 300_000;
/// `alert` is `max_cell_temp_c` strictly above this.
pub const ALERT_ABOVE_C: f64 = 55.0;

#[derive(Debug, Default)]
pub struct BatteryHealth;

/// Per-VIN open windows keyed by `window_start`; a window exists only once a
/// reading fell into it, so an emitted window always has `event_count >= 1`.
pub type Windows = BTreeMap<i64, Window>;

#[derive(Clone, Debug, PartialEq)]
pub struct Window {
    soc_sum: f64,
    count: i64,
    min_soh: f64,
    max_temp: f64,
}

/// `BatteryWindow` as written to records/trace JSON and encoded for Kafka.
#[derive(Clone, Debug, PartialEq)]
pub struct BatteryWindowRecord(pub BatteryWindow);

pub fn window_start(ts: i64) -> i64 {
    ts - ts.rem_euclid(WINDOW_MS)
}

/// Lowercase hex SHA-256 of `battery-health|<vin>|<window_start>`.
pub fn output_id(vin: &str, window_start: i64) -> String {
    sha256_hex(format!("battery-health|{vin}|{window_start}").as_bytes())
}

impl Window {
    fn first(reading: &BatteryReading) -> Self {
        Self {
            soc_sum: reading.soc_pct.unwrap_or_default(),
            count: 1,
            min_soh: reading.soh_pct.unwrap_or_default(),
            max_temp: reading.cell_temp_max_c.unwrap_or_default(),
        }
    }

    fn add(&mut self, reading: &BatteryReading) {
        self.soc_sum += reading.soc_pct.unwrap_or_default();
        self.count += 1;
        self.min_soh = self.min_soh.min(reading.soh_pct.unwrap_or_default());
        self.max_temp = self
            .max_temp
            .max(reading.cell_temp_max_c.unwrap_or_default());
    }

    fn close(self, vin: &str, window_start: i64) -> BatteryWindowRecord {
        BatteryWindowRecord(BatteryWindow {
            output_id: output_id(vin, window_start),
            vin: vin.to_owned(),
            window_start,
            avg_soc_pct: self.soc_sum / self.count as f64,
            min_soh_pct: self.min_soh,
            max_cell_temp_c: self.max_temp,
            alert: self.max_temp > ALERT_ABOVE_C,
            event_count: self.count,
        })
    }
}

impl Processor for BatteryHealth {
    type Event = BatteryReading;
    type State = Windows;
    type Output = BatteryWindowRecord;

    fn on_event(&mut self, reading: &BatteryReading, windows: &mut Windows) -> Vec<Self::Output> {
        windows
            .entry(window_start(reading.ts))
            .and_modify(|window| window.add(reading))
            .or_insert_with(|| Window::first(reading));
        Vec::new()
    }

    /// Emits, in `window_start` order, every window whose end is at or before
    /// the watermark (`window_start + 5 min <= watermark`); they are final.
    fn on_watermark(
        &mut self,
        vin: &str,
        watermark_ms: i64,
        windows: &mut Windows,
    ) -> Vec<Self::Output> {
        let still_open = windows.split_off(&(watermark_ms - WINDOW_MS + 1));
        std::mem::replace(windows, still_open)
            .into_iter()
            .map(|(window_start, window)| window.close(vin, window_start))
            .collect()
    }

    fn is_open(&self, windows: &Windows) -> bool {
        !windows.is_empty()
    }
}

impl JsonRecord for BatteryWindowRecord {
    fn to_json(&self) -> Value {
        let w = &self.0;
        json!({
            "output_id": w.output_id,
            "vin": w.vin,
            "window_start": w.window_start,
            "avg_soc_pct": w.avg_soc_pct,
            "min_soh_pct": w.min_soh_pct,
            "max_cell_temp_c": w.max_cell_temp_c,
            "alert": w.alert,
            "event_count": w.event_count,
        })
    }
}

/// Kafka output: `battery.health.v1.BatteryWindow` protobuf bytes keyed by VIN
/// (what Spark's `to_protobuf` wrote).
pub fn encode(record: &BatteryWindowRecord) -> (String, Vec<u8>) {
    (record.0.vin.clone(), record.0.encode_to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    const VIN: &str = "TST00000000000001";

    fn reading(ts: i64, soc: f64, soh: f64, temp: f64) -> BatteryReading {
        BatteryReading {
            event_id: String::new(),
            vin: VIN.to_string(),
            ts,
            soc_pct: Some(soc),
            soh_pct: Some(soh),
            cell_temp_max_c: Some(temp),
            pack_voltage_v: Some(400.0),
        }
    }

    #[test]
    fn output_id_matches_the_contract_example() {
        assert_eq!(
            output_id(VIN, 1_790_000_100_000),
            "924bbe50976ec7a530dd47f70c1c42f19bb46342e4e320a06a3e076bb70feda8"
        );
    }

    #[test]
    fn windows_are_epoch_aligned_and_the_end_is_exclusive() {
        assert_eq!(window_start(1_790_000_099_999), 1_789_999_800_000);
        assert_eq!(window_start(1_790_000_100_000), 1_790_000_100_000);
    }

    #[test]
    fn windows_close_only_when_the_watermark_reaches_their_end() {
        let mut job = BatteryHealth;
        let mut windows = Windows::default();
        job.on_event(&reading(1_790_000_000_000, 80.0, 95.0, 30.0), &mut windows);
        job.on_event(&reading(1_790_000_000_001, 70.0, 94.0, 55.0), &mut windows);
        job.on_event(&reading(1_790_000_100_000, 60.0, 96.0, 55.1), &mut windows);
        assert!(job
            .on_watermark(VIN, 1_790_000_099_999, &mut windows)
            .is_empty());
        assert!(job.is_open(&windows));

        let first = job.on_watermark(VIN, 1_790_000_100_000, &mut windows);
        assert_eq!(first.len(), 1);
        let w = &first[0].0;
        assert_eq!(
            (w.window_start, w.event_count, w.alert),
            (1_789_999_800_000, 2, false)
        );
        assert_eq!(
            (w.avg_soc_pct, w.min_soh_pct, w.max_cell_temp_c),
            (75.0, 94.0, 55.0)
        );

        let second = job.on_watermark(VIN, 1_790_000_400_000, &mut windows);
        assert_eq!(second.len(), 1);
        assert!(second[0].0.alert, "55.1 is strictly above 55.0");
        assert!(!job.is_open(&windows));
    }

    #[test]
    fn json_uses_the_proto_field_names_and_kafka_is_keyed_by_vin() {
        let record = Window::first(&reading(1_790_000_100_000, 80.0, 95.0, 30.0))
            .close(VIN, 1_790_000_100_000);
        let json = record.to_json();
        assert_eq!(json["vin"], VIN);
        assert_eq!(json["event_count"], 1);
        assert_eq!(json["avg_soc_pct"], 80.0);
        let (key, payload) = encode(&record);
        assert_eq!(key, VIN);
        assert_eq!(BatteryWindow::decode(payload.as_slice()).unwrap(), record.0);
    }
}
