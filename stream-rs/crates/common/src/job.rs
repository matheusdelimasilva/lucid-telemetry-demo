use crate::event::InputEvent;
use crate::proto::{
    battery_input::BatteryReading,
    charging::{ChargingEvent, ChargingEventType},
};
use crate::validate::{battery_reading, charging_event};

pub struct JobSpec<E: InputEvent> {
    pub name: &'static str,
    pub delay_ms: i64,
    pub validate: fn(&E) -> bool,
    pub control_event: fn(target_watermark_ms: i64) -> E,
}

pub const RESERVED_VIN: &str = "TSTZZZZZZZZZZZZZZ";
pub const CONTROL_EVENT_ID: &str = "ffffffff-ffff-4fff-bfff-ffffffffffff";

pub const CHARGING: JobSpec<ChargingEvent> = JobSpec {
    name: "charging-sessions",
    delay_ms: 600_000,
    validate: charging_event,
    control_event: charging_control,
};

pub const BATTERY: JobSpec<BatteryReading> = JobSpec {
    name: "battery-health",
    delay_ms: 120_000,
    validate: battery_reading,
    control_event: battery_control,
};

fn charging_control(target_watermark_ms: i64) -> ChargingEvent {
    ChargingEvent {
        event_id: CONTROL_EVENT_ID.to_string(),
        vin: RESERVED_VIN.to_string(),
        ts: target_watermark_ms + CHARGING.delay_ms,
        event: ChargingEventType::PlugIn as i32,
        energy_wh: 0,
        lat: Some(0.0),
        lon: Some(0.0),
        charger_type: Some("flush".to_string()),
    }
}

fn battery_control(target_watermark_ms: i64) -> BatteryReading {
    BatteryReading {
        event_id: CONTROL_EVENT_ID.to_string(),
        vin: RESERVED_VIN.to_string(),
        ts: target_watermark_ms + BATTERY.delay_ms,
        soc_pct: Some(50.0),
        soh_pct: Some(100.0),
        cell_temp_max_c: Some(20.0),
        pack_voltage_v: Some(400.0),
    }
}
