use crate::proto::{battery_input::BatteryReading, charging::ChargingEvent};

const MIN_TS: i64 = 1_577_836_800_000;
const MAX_TS: i64 = 4_102_444_800_000;

pub fn charging_event(event: &ChargingEvent) -> bool {
    common(&event.event_id, &event.vin, event.ts)
        && (1..=5).contains(&event.event)
        && event.energy_wh >= 0
        && (event.energy_wh == 0 || matches!(event.event, 3 | 4))
        && (event.event != 1
            || matches!(
                (event.lat, event.lon, event.charger_type.as_deref()),
                (Some(lat), Some(lon), Some(charger_type))
                    if lat.is_finite()
                        && (-90.0..=90.0).contains(&lat)
                        && lon.is_finite()
                        && (-180.0..=180.0).contains(&lon)
                        && !charger_type.is_empty()
            ))
}

pub fn battery_reading(reading: &BatteryReading) -> bool {
    common(&reading.event_id, &reading.vin, reading.ts)
        && matches!(
            (
                reading.soc_pct,
                reading.soh_pct,
                reading.cell_temp_max_c,
                reading.pack_voltage_v
            ),
            (Some(soc), Some(soh), Some(temp), Some(voltage))
                if soc.is_finite()
                    && (0.0..=100.0).contains(&soc)
                    && soh.is_finite()
                    && (0.0..=100.0).contains(&soh)
                    && temp.is_finite()
                    && (-60.0..=120.0).contains(&temp)
                    && voltage.is_finite()
                    && voltage > 0.0
                    && voltage <= 1000.0
        )
}

fn common(event_id: &str, vin: &str, ts: i64) -> bool {
    canonical_uuid(event_id)
        && vin.len() == 17
        && vin
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        && (MIN_TS..MAX_TS).contains(&ts)
}

fn canonical_uuid(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 36
        && [8, 13, 18, 23].iter().all(|index| bytes[*index] == b'-')
        && bytes.iter().enumerate().all(|(index, byte)| {
            [8, 13, 18, 23].contains(&index)
                || byte.is_ascii_digit()
                || (b'a'..=b'f').contains(byte)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::charging::ChargingEventType;

    fn charging() -> ChargingEvent {
        ChargingEvent {
            event_id: "00000000-0000-4000-8000-000000000001".to_string(),
            vin: "TST00000000000001".to_string(),
            ts: MIN_TS,
            event: ChargingEventType::PlugIn as i32,
            energy_wh: 0,
            lat: Some(0.0),
            lon: Some(0.0),
            charger_type: Some("type".to_string()),
        }
    }

    fn battery() -> BatteryReading {
        BatteryReading {
            event_id: "00000000-0000-4000-8000-000000000001".to_string(),
            vin: "TST00000000000001".to_string(),
            ts: MIN_TS,
            soc_pct: Some(0.0),
            soh_pct: Some(100.0),
            cell_temp_max_c: Some(-60.0),
            pack_voltage_v: Some(1000.0),
        }
    }

    #[test]
    fn validates_boundaries_and_charging_event_rules() {
        assert!(charging_event(&charging()));
        let mut event = charging();
        event.ts = MAX_TS;
        assert!(!charging_event(&event));
        let mut event = charging();
        event.event_id = "abcdefab-cdef-4abc-8def-abcdefabcdef".to_string();
        assert!(charging_event(&event));
        event.event_id.make_ascii_uppercase();
        assert!(!charging_event(&event));
        let mut event = charging();
        event.lat = None;
        assert!(!charging_event(&event));
        let mut event = charging();
        event.event = ChargingEventType::Start as i32;
        event.energy_wh = 1;
        assert!(!charging_event(&event));
    }

    #[test]
    fn validates_battery_numeric_presence_and_ranges() {
        assert!(battery_reading(&battery()));
        let mut reading = battery();
        reading.soc_pct = Some(f64::NAN);
        assert!(!battery_reading(&reading));
        let mut reading = battery();
        reading.pack_voltage_v = Some(0.0);
        assert!(!battery_reading(&reading));
        assert!(battery_reading(&battery()));
        let mut reading = battery();
        reading.pack_voltage_v = None;
        assert!(!battery_reading(&reading));
    }
}
