use std::process::ExitCode;

use charging_sessions::{encode, ChargingSessions};
use common::job::CHARGING;

fn main() -> ExitCode {
    common::cli::main::<ChargingSessions>(CHARGING, encode)
}
