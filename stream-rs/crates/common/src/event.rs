use anyhow::{bail, Result};
use prost::Message;
use serde_json::Value;

use crate::proto::{battery_input::BatteryReading, charging::ChargingEvent};

pub trait InputEvent: Message + Clone + PartialEq + Default + 'static {
    fn event_id(&self) -> &str;
    fn vin(&self) -> &str;
    fn ts(&self) -> i64;
    fn from_fixture_json(value: &Value) -> Result<Self>;
}

impl InputEvent for ChargingEvent {
    fn event_id(&self) -> &str {
        &self.event_id
    }

    fn vin(&self) -> &str {
        &self.vin
    }

    fn ts(&self) -> i64 {
        self.ts
    }

    fn from_fixture_json(value: &Value) -> Result<Self> {
        check_object(
            value,
            &[
                "event_id",
                "vin",
                "ts",
                "event",
                "energy_wh",
                "lat",
                "lon",
                "charger_type",
            ],
        )?;
        Ok(Self {
            event_id: string(value, "event_id")?.unwrap_or_default(),
            vin: string(value, "vin")?.unwrap_or_default(),
            ts: integer(value, "ts")?.unwrap_or_default(),
            event: match string(value, "event")?.as_deref() {
                None | Some("EVENT_UNSPECIFIED") => 0,
                Some("PLUG_IN") => 1,
                Some("START") => 2,
                Some("PROGRESS") => 3,
                Some("STOP") => 4,
                Some("UNPLUG") => 5,
                Some(name) => bail!("unknown ChargingEventType name {name:?}"),
            },
            energy_wh: integer(value, "energy_wh")?.unwrap_or_default(),
            lat: double(value, "lat")?,
            lon: double(value, "lon")?,
            charger_type: string(value, "charger_type")?,
        })
    }
}

impl InputEvent for BatteryReading {
    fn event_id(&self) -> &str {
        &self.event_id
    }

    fn vin(&self) -> &str {
        &self.vin
    }

    fn ts(&self) -> i64 {
        self.ts
    }

    fn from_fixture_json(value: &Value) -> Result<Self> {
        check_object(
            value,
            &[
                "event_id",
                "vin",
                "ts",
                "soc_pct",
                "soh_pct",
                "cell_temp_max_c",
                "pack_voltage_v",
            ],
        )?;
        Ok(Self {
            event_id: string(value, "event_id")?.unwrap_or_default(),
            vin: string(value, "vin")?.unwrap_or_default(),
            ts: integer(value, "ts")?.unwrap_or_default(),
            soc_pct: double(value, "soc_pct")?,
            soh_pct: double(value, "soh_pct")?,
            cell_temp_max_c: double(value, "cell_temp_max_c")?,
            pack_voltage_v: double(value, "pack_voltage_v")?,
        })
    }
}

fn check_object(value: &Value, allowed: &[&str]) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("fixture message must be a JSON object"))?;
    if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
        bail!("unknown fixture message field {key:?}");
    }
    Ok(())
}

fn get<'a>(value: &'a Value, key: &str) -> Result<Option<&'a Value>> {
    match value.get(key) {
        None => Ok(None),
        Some(Value::Null) => bail!("fixture field {key:?} must not be null"),
        Some(value) => Ok(Some(value)),
    }
}

fn string(value: &Value, key: &str) -> Result<Option<String>> {
    get(value, key)?
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| anyhow::anyhow!("fixture field {key:?} must be a string"))
        })
        .transpose()
}

fn integer(value: &Value, key: &str) -> Result<Option<i64>> {
    get(value, key)?
        .map(|value| {
            value.as_i64().ok_or_else(|| {
                anyhow::anyhow!("fixture field {key:?} must be a JSON int64 integer")
            })
        })
        .transpose()
}

fn double(value: &Value, key: &str) -> Result<Option<f64>> {
    get(value, key)?
        .map(|value| {
            value
                .as_f64()
                .ok_or_else(|| anyhow::anyhow!("fixture field {key:?} must be a JSON number"))
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_decoder_is_strict_and_preserves_option_presence() {
        let event = ChargingEvent::from_fixture_json(
            &serde_json::json!({"event":"PLUG_IN", "lat":0, "lon":0, "charger_type":""}),
        )
        .unwrap();
        assert_eq!(event.event, 1);
        assert_eq!(event.lat, Some(0.0));
        assert_eq!(event.lon, Some(0.0));
        assert_eq!(event.charger_type.as_deref(), Some(""));
        assert!(
            ChargingEvent::from_fixture_json(&serde_json::json!({"event":"NOT_AN_EVENT"})).is_err()
        );
        assert!(ChargingEvent::from_fixture_json(&serde_json::json!({"ts":null})).is_err());
        assert!(ChargingEvent::from_fixture_json(&serde_json::json!({"ts":"1"})).is_err());
        assert!(ChargingEvent::from_fixture_json(&serde_json::json!({"unknown":1})).is_err());
    }

    #[test]
    fn integer_fields_must_be_json_integers() {
        assert!(BatteryReading::from_fixture_json(&serde_json::json!({"ts":1.0})).is_err());
    }
}
