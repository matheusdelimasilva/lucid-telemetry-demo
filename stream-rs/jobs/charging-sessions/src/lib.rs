//! charging-sessions: one final record per charging session from valid
//! `vehicle.charging.v1` events (`contracts/charging-sessions.md`). Validation,
//! lateness, deduplication, watermarks and the per-VIN `(ts, arrival_seq)`
//! release order all live in `common::runner`; this crate only runs the session
//! state machine over the events it is handed, by event time, and closes idle
//! sessions on watermarks.

use common::hash::sha256_hex;
use common::job::RESERVED_VIN;
use common::processor::Processor;
use common::proto::charging::{ChargingEvent, ChargingEventType};
use common::proto::charging_output::{ChargingSession, CloseReason};
use common::record::JsonRecord;
use prost::Message;
use serde_json::{json, Value};

/// A gap strictly longer than this between accepted session events, or between
/// the last one and the watermark, closes the session with `INACTIVITY_TIMEOUT`.
pub const INACTIVITY_MS: i64 = 1_800_000;

/// Session close counters, `sessions_by_close_reason` in the contract's
/// counters; `orphan` is the job's other counter. Both exclude the reserved VIN,
/// as the Scala job's `close()` did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChargingSessions {
    pub orphan: u64,
    pub unplug: u64,
    pub inactivity_timeout: u64,
    pub replaced_by_plug_in: u64,
}

/// The open session of one VIN, if any.
pub type OpenSession = Option<Session>;

#[derive(Clone, Debug, PartialEq)]
pub struct Session {
    output_id: String,
    start_ts: i64,
    last_ts: i64,
    total_energy_wh: i64,
    start_lat: f64,
    start_lon: f64,
    charger_type: String,
}

/// `ChargingSession` as written to records/trace JSON and encoded for Kafka.
#[derive(Clone, Debug, PartialEq)]
pub struct ChargingSessionRecord(pub ChargingSession);

/// Lowercase hex SHA-256 of the opening `PLUG_IN`'s `event_id`.
pub fn output_id(plug_in_event_id: &str) -> String {
    sha256_hex(plug_in_event_id.as_bytes())
}

/// Three decimals, exact halves away from zero, on the shortest decimal form of
/// the double (`SPEC.md`: `Decimal(repr(x)).quantize(0.001, ROUND_HALF_UP)`;
/// Scala's `BigDecimal(x).setScale(3, HALF_UP)`). Float arithmetic would see
/// `-33.8675` as `-33.86749999...` and round it the wrong way. `Decimal::from_str`
/// (unlike `from_str_exact`) rounds digits past the 28th decimal place, so the
/// shortest form of a tiny double like `1e-30` parses and rounds to `0.0`.
pub fn round_coordinate(value: f64) -> f64 {
    value
}

/// The runner only hands us contract-valid events (`common::validate::charging_event`):
/// a known event type, and lat/lon/charger_type present on every `PLUG_IN`.
fn event_type(event: &ChargingEvent) -> ChargingEventType {
    ChargingEventType::try_from(event.event)
        .ok()
        .filter(|kind| *kind != ChargingEventType::EventUnspecified)
        .expect("validated by the runner")
}

fn present<T>(value: Option<T>) -> T {
    value.expect("validated by the runner")
}

impl Session {
    fn open(plug_in: &ChargingEvent) -> Self {
        Self {
            output_id: output_id(&plug_in.event_id),
            start_ts: plug_in.ts,
            last_ts: plug_in.ts,
            total_energy_wh: 0,
            start_lat: round_coordinate(present(plug_in.lat)),
            start_lon: round_coordinate(present(plug_in.lon)),
            charger_type: present(plug_in.charger_type.clone()),
        }
    }

    fn record(self, vin: &str, end_ts: i64, reason: CloseReason) -> ChargingSessionRecord {
        ChargingSessionRecord(ChargingSession {
            output_id: self.output_id,
            vin: vin.to_owned(),
            start_ts: self.start_ts,
            end_ts,
            duration_ms: end_ts - self.start_ts,
            total_energy_wh: self.total_energy_wh,
            start_lat: self.start_lat,
            start_lon: self.start_lon,
            charger_type: self.charger_type,
            close_reason: reason as i32,
        })
    }
}

impl ChargingSessions {
    /// Closes `session`: one record and one close-reason count, neither for the
    /// reserved VIN.
    fn close(
        &mut self,
        vin: &str,
        session: Session,
        end_ts: i64,
        reason: CloseReason,
    ) -> Option<ChargingSessionRecord> {
        if vin == RESERVED_VIN {
            return None;
        }
        match reason {
            CloseReason::Unplug => self.unplug += 1,
            CloseReason::InactivityTimeout => self.inactivity_timeout += 1,
            CloseReason::ReplacedByPlugIn => self.replaced_by_plug_in += 1,
            CloseReason::Unspecified => unreachable!("sessions close for a reason"),
        }
        Some(session.record(vin, end_ts, reason))
    }
}

impl Processor for ChargingSessions {
    type Event = ChargingEvent;
    type State = OpenSession;
    type Output = ChargingSessionRecord;

    fn on_event(&mut self, event: &ChargingEvent, session: &mut OpenSession) -> Vec<Self::Output> {
        let vin = event.vin.as_str();
        let mut emitted = Vec::new();
        if let Some(open) = session.take_if(|open| event.ts - open.last_ts > INACTIVITY_MS) {
            let last_ts = open.last_ts;
            emitted.extend(self.close(vin, open, last_ts, CloseReason::InactivityTimeout));
        }
        match event_type(event) {
            ChargingEventType::PlugIn => {
                if let Some(open) = session.take() {
                    let last_ts = open.last_ts;
                    emitted.extend(self.close(vin, open, last_ts, CloseReason::ReplacedByPlugIn));
                }
                *session = Some(Session::open(event));
            }
            _ if session.is_none() => {
                if vin != RESERVED_VIN {
                    self.orphan += 1;
                }
            }
            ChargingEventType::Progress | ChargingEventType::Stop => {
                let open = session.as_mut().expect("checked above");
                open.last_ts = event.ts;
                open.total_energy_wh += event.energy_wh;
            }
            ChargingEventType::Start => {
                session.as_mut().expect("checked above").last_ts = event.ts;
            }
            ChargingEventType::Unplug => {
                let open = session.take().expect("checked above");
                emitted.extend(self.close(vin, open, event.ts, CloseReason::Unplug));
            }
            ChargingEventType::EventUnspecified => unreachable!("validated by the runner"),
        }
        emitted
    }

    /// Closes the session once the watermark is strictly past `last_ts + 30 min`,
    /// at `last_ts`.
    fn on_watermark(
        &mut self,
        vin: &str,
        watermark_ms: i64,
        session: &mut OpenSession,
    ) -> Vec<Self::Output> {
        match session.take_if(|open| open.last_ts + INACTIVITY_MS < watermark_ms) {
            Some(open) => {
                let last_ts = open.last_ts;
                self.close(vin, open, last_ts, CloseReason::InactivityTimeout)
                    .into_iter()
                    .collect()
            }
            None => Vec::new(),
        }
    }

    fn is_open(&self, session: &OpenSession) -> bool {
        session.is_some()
    }

    /// `orphan` and `sessions_by_close_reason`, after the runner's four counters.
    fn counters(&self) -> Vec<(&'static str, Value)> {
        vec![
            ("orphan", json!(self.orphan)),
            (
                "sessions_by_close_reason",
                json!({
                    "unplug": self.unplug,
                    "inactivity_timeout": self.inactivity_timeout,
                    "replaced_by_plug_in": self.replaced_by_plug_in,
                }),
            ),
        ]
    }
}

impl JsonRecord for ChargingSessionRecord {
    fn to_json(&self) -> Value {
        let s = &self.0;
        json!({
            "output_id": s.output_id,
            "vin": s.vin,
            "start_ts": s.start_ts,
            "end_ts": s.end_ts,
            "duration_ms": s.duration_ms,
            "total_energy_wh": s.total_energy_wh,
            "start_lat": s.start_lat,
            "start_lon": s.start_lon,
            "charger_type": s.charger_type,
            "close_reason": s.close_reason().as_str_name(),
        })
    }
}

/// Kafka output: `charging.sessions.v1.ChargingSession` protobuf bytes keyed by
/// VIN (what Spark's `to_protobuf` wrote).
pub fn encode(record: &ChargingSessionRecord) -> (String, Vec<u8>) {
    (record.0.vin.clone(), record.0.encode_to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    const VIN: &str = "TST00000000000001";
    const T0: i64 = 1_789_984_800_000;

    fn event(id: u8, ts: i64, kind: ChargingEventType, energy_wh: i64) -> ChargingEvent {
        let plug_in = kind == ChargingEventType::PlugIn;
        ChargingEvent {
            event_id: format!("00000000-0000-4000-8000-0000000000{id:02x}"),
            vin: VIN.to_string(),
            ts,
            event: kind as i32,
            energy_wh,
            lat: plug_in.then_some(37.4),
            lon: plug_in.then_some(-122.1),
            charger_type: plug_in.then(|| "dc_fast".to_string()),
        }
    }

    fn summary(record: &ChargingSessionRecord) -> (i64, i64, i64, CloseReason) {
        let s = &record.0;
        (s.start_ts, s.end_ts, s.total_energy_wh, s.close_reason())
    }

    #[test]
    fn output_id_matches_the_contract_example() {
        assert_eq!(
            output_id("00000000-0000-4000-8000-000000000001"),
            "11e594f481958c10e3015d0bf0447a22f068a8a647f475df15ce2c7ab4b8f3f1"
        );
    }

    #[test]
    fn coordinates_round_half_away_from_zero_on_the_decimal_form() {
        assert_eq!(round_coordinate(-33.8675), -33.868);
        assert_eq!(round_coordinate(151.2095), 151.21);
        assert_eq!(round_coordinate(0.0005), 0.001);
        assert_eq!(round_coordinate(-0.0005), -0.001);
        assert_eq!(round_coordinate(37.4), 37.4);
        assert_eq!(round_coordinate(0.0), 0.0);
        assert_eq!(round_coordinate(-23.0), -23.0);
        assert_eq!(round_coordinate(2.0005), 2.001);
        assert_eq!(round_coordinate(1.0004999), 1.0);
    }

    #[test]
    fn coordinates_below_half_a_thousandth_round_to_positive_zero() {
        for value in [
            1e-30,
            -1e-30,
            4e-4,
            -4e-4,
            0.00049999999,
            -0.00049999999,
            -0.0,
        ] {
            let rounded = round_coordinate(value);
            assert_eq!(rounded, 0.0, "{value}");
            assert!(
                rounded.is_sign_positive(),
                "{value} must round to +0.0, not -0.0"
            );
        }
        assert_eq!(round_coordinate(f64::MIN_POSITIVE), 0.0);
        assert_eq!(round_coordinate(5e-324), 0.0);
    }

    #[test]
    fn plug_in_with_tiny_coordinates_opens_a_session_at_zero() {
        let mut job = ChargingSessions::default();
        let mut session = OpenSession::default();
        let mut plug_in = event(1, T0, ChargingEventType::PlugIn, 0);
        plug_in.lat = Some(1e-30);
        plug_in.lon = Some(-1e-30);
        assert!(job.on_event(&plug_in, &mut session).is_empty());
        let out = job.on_event(
            &event(2, T0 + 60_000, ChargingEventType::Unplug, 0),
            &mut session,
        );
        assert_eq!(out.len(), 1);
        let json = out[0].to_json();
        assert_eq!(json["start_lat"], json!(0.0));
        assert_eq!(json["start_lon"], json!(0.0));
        assert_eq!(json["start_lat"].to_string(), "0.0");
        assert_eq!(json["start_lon"].to_string(), "0.0");
    }

    #[test]
    fn stop_then_resume_sums_energy_and_unplug_closes_at_its_own_ts() {
        let mut job = ChargingSessions::default();
        let mut session = OpenSession::default();
        let rows = [
            event(1, T0, ChargingEventType::PlugIn, 0),
            event(2, T0 + 300_000, ChargingEventType::Progress, 100),
            event(3, T0 + 600_000, ChargingEventType::Stop, 50),
            event(4, T0 + 1_200_000, ChargingEventType::Start, 0),
            event(5, T0 + 1_800_000, ChargingEventType::Progress, 100),
        ];
        for row in &rows {
            assert!(job.on_event(row, &mut session).is_empty());
            assert!(job.is_open(&session));
        }
        let out = job.on_event(
            &event(6, T0 + 2_100_000, ChargingEventType::Unplug, 0),
            &mut session,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(
            summary(&out[0]),
            (T0, T0 + 2_100_000, 250, CloseReason::Unplug)
        );
        assert_eq!(out[0].0.duration_ms, 2_100_000);
        assert_eq!(out[0].0.output_id, output_id(&rows[0].event_id));
        assert!(!job.is_open(&session));
        assert_eq!(job.unplug, 1);
    }

    #[test]
    fn gap_of_exactly_30_minutes_continues_the_session() {
        let mut job = ChargingSessions::default();
        let mut session = OpenSession::default();
        job.on_event(&event(1, T0, ChargingEventType::PlugIn, 0), &mut session);
        let out = job.on_event(
            &event(2, T0 + INACTIVITY_MS, ChargingEventType::Progress, 100),
            &mut session,
        );
        assert!(out.is_empty());
        assert_eq!(job.orphan, 0);
        assert_eq!(session.as_ref().unwrap().total_energy_wh, 100);
    }

    #[test]
    fn gap_over_30_minutes_closes_at_last_ts_and_the_event_is_an_orphan() {
        let mut job = ChargingSessions::default();
        let mut session = OpenSession::default();
        job.on_event(&event(1, T0, ChargingEventType::PlugIn, 0), &mut session);
        let out = job.on_event(
            &event(2, T0 + INACTIVITY_MS + 1, ChargingEventType::Progress, 100),
            &mut session,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(
            summary(&out[0]),
            (T0, T0, 0, CloseReason::InactivityTimeout)
        );
        assert!(!job.is_open(&session));
        assert_eq!((job.orphan, job.inactivity_timeout), (1, 1));
    }

    #[test]
    fn plug_in_replaces_an_open_session_ending_it_at_its_last_event() {
        let mut job = ChargingSessions::default();
        let mut session = OpenSession::default();
        job.on_event(&event(1, T0, ChargingEventType::PlugIn, 0), &mut session);
        job.on_event(
            &event(2, T0 + 300_000, ChargingEventType::Progress, 100),
            &mut session,
        );
        let out = job.on_event(
            &event(3, T0 + 600_000, ChargingEventType::PlugIn, 0),
            &mut session,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(
            summary(&out[0]),
            (T0, T0 + 300_000, 100, CloseReason::ReplacedByPlugIn)
        );
        assert_eq!(session.as_ref().unwrap().start_ts, T0 + 600_000);
        assert_eq!(job.replaced_by_plug_in, 1);
    }

    #[test]
    fn plug_in_after_a_long_gap_times_the_old_session_out_instead() {
        let mut job = ChargingSessions::default();
        let mut session = OpenSession::default();
        job.on_event(&event(1, T0, ChargingEventType::PlugIn, 0), &mut session);
        let out = job.on_event(
            &event(2, T0 + INACTIVITY_MS + 1, ChargingEventType::PlugIn, 0),
            &mut session,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0.close_reason(), CloseReason::InactivityTimeout);
        assert_eq!((job.inactivity_timeout, job.replaced_by_plug_in), (1, 0));
        assert!(job.is_open(&session));
    }

    #[test]
    fn watermark_times_out_only_strictly_past_last_ts_plus_30_minutes() {
        let mut job = ChargingSessions::default();
        let mut session = OpenSession::default();
        job.on_event(&event(1, T0, ChargingEventType::PlugIn, 0), &mut session);
        assert!(job
            .on_watermark(VIN, T0 + INACTIVITY_MS, &mut session)
            .is_empty());
        let out = job.on_watermark(VIN, T0 + INACTIVITY_MS + 1, &mut session);
        assert_eq!(out.len(), 1);
        assert_eq!(
            summary(&out[0]),
            (T0, T0, 0, CloseReason::InactivityTimeout)
        );
        assert!(!job.is_open(&session));
    }

    #[test]
    fn orphans_do_not_open_or_touch_a_session() {
        let mut job = ChargingSessions::default();
        let mut session = OpenSession::default();
        assert!(job
            .on_event(&event(1, T0, ChargingEventType::Unplug, 0), &mut session)
            .is_empty());
        assert!(!job.is_open(&session));
        assert_eq!(job.orphan, 1);
    }

    #[test]
    fn reserved_vin_emits_neither_records_nor_counters() {
        let mut job = ChargingSessions::default();
        let mut session = OpenSession::default();
        let mut plug_in = event(1, T0, ChargingEventType::PlugIn, 0);
        plug_in.vin = RESERVED_VIN.to_string();
        let mut unplug = event(2, T0 + 1_000, ChargingEventType::Unplug, 0);
        unplug.vin = RESERVED_VIN.to_string();
        job.on_event(&plug_in, &mut session);
        assert!(job.on_event(&unplug, &mut session).is_empty());
        assert!(job.on_event(&unplug, &mut session).is_empty());
        assert_eq!(job, ChargingSessions::default());
    }

    #[test]
    fn json_uses_the_proto_names_with_the_enum_as_text_and_kafka_is_keyed_by_vin() {
        let mut job = ChargingSessions::default();
        let mut session = OpenSession::default();
        let mut plug_in = event(1, T0, ChargingEventType::PlugIn, 0);
        plug_in.lat = Some(-33.8675);
        plug_in.lon = Some(0.0);
        job.on_event(&plug_in, &mut session);
        let out = job.on_event(
            &event(2, T0 + 1_000, ChargingEventType::Unplug, 0),
            &mut session,
        );
        let json = out[0].to_json();
        assert_eq!(json["vin"], VIN);
        assert_eq!(json["close_reason"], "UNPLUG");
        assert_eq!(json["start_lat"], -33.868);
        assert_eq!(json["start_lon"], 0.0);
        assert!(json["start_lon"].is_f64());
        assert_eq!(json["total_energy_wh"], 0);
        assert!(json["total_energy_wh"].is_i64());
        assert_eq!(serde_json::to_string(&json["start_lon"]).unwrap(), "0.0");
        let (key, payload) = encode(&out[0]);
        assert_eq!(key, VIN);
        assert_eq!(
            ChargingSession::decode(payload.as_slice()).unwrap(),
            out[0].0
        );
        assert_eq!(
            job.counters(),
            vec![
                ("orphan", json!(0)),
                (
                    "sessions_by_close_reason",
                    json!({"unplug": 1, "inactivity_timeout": 0, "replaced_by_plug_in": 0})
                ),
            ]
        );
    }
}
