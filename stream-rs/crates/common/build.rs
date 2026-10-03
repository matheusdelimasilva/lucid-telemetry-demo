fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    prost_build::compile_protos(
        &[
            "../../../proto/vehicle_charging_v1.proto",
            "../../../proto/vehicle_battery_v1.proto",
            "../../../proto/charging_sessions_v1.proto",
            "../../../proto/battery_health_v1.proto",
        ],
        &["../../../proto"],
    )?;
    Ok(())
}
