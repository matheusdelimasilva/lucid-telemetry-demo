# proto/

The one source of truth for Kafka message schemas: one `.proto` per topic
(`vehicle.battery.v1`, `vehicle.charging.v1`), drafted and reviewed in stage 2
as part of the behavior contract.

`smoke.proto` is a stage-1 throwaway used to prove the protoc → Python / Rust /
Spark paths end to end. It is deleted in stage 2 when the real schemas land.
