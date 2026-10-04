use std::process::ExitCode;

use battery_health::{encode, BatteryHealth};
use common::job::BATTERY;

fn main() -> ExitCode {
    common::cli::main::<BatteryHealth>(BATTERY, encode)
}
