pub mod proto {
    pub mod battery_input {
        include!(concat!(env!("OUT_DIR"), "/vehicle.battery.v1.rs"));
    }
    pub mod battery_output {
        include!(concat!(env!("OUT_DIR"), "/battery.health.v1.rs"));
    }
    pub mod charging {
        include!(concat!(env!("OUT_DIR"), "/vehicle.charging.v1.rs"));
    }
    pub mod charging_output {
        include!(concat!(env!("OUT_DIR"), "/charging.sessions.v1.rs"));
    }
}

pub mod artifacts;
pub mod cli;
pub mod config;
pub mod event;
pub mod hash;
pub mod job;
pub mod kafka;
pub mod processor;
pub mod record;
pub mod replay;
pub mod runner;
pub mod validate;
