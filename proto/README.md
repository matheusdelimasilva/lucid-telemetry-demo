# proto/

The one source of truth for Kafka message schemas, one `.proto` per topic.

| File | Package / logical topic | Role |
| --- | --- | --- |
| `vehicle_charging_v1.proto` | `vehicle.charging.v1` | Input to `charging-sessions` |
| `vehicle_battery_v1.proto` | `vehicle.battery.v1` | Input to `battery-health` |
| `charging_sessions_v1.proto` | `charging.sessions.v1` | Output of `charging-sessions` |
| `battery_health_v1.proto` | `battery.health.v1` | Output of `battery-health` |

Field meanings and validation live in `contracts/`. proto3 `optional` is used
exactly where the contract distinguishes "missing" from zero (charging
`lat`/`lon`/`charger_type`, every battery reading). Compiled in Python
(`grpcio-tools`), Scala (ScalaPB, protoc 3.25.5) and Rust (`prost`) in CI.
