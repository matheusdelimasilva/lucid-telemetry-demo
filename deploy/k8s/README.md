# deploy/k8s/

Minimal Kubernetes manifest showing the target runtime for the Rust jobs.
Added in stage 5 alongside the `battery-health` reference implementation.
Illustrative only — nothing here is applied in this demo.

- `battery-health.yaml`: a `ConfigMap` with the job's environment
  (`KAFKA_BROKER`, `INPUT_TOPIC`, `OUTPUT_TOPIC`, `GROUP_ID`, batch settings)
  and a one-replica `Deployment` running the image from
  `deploy/battery-health.Dockerfile` with `--kafka`.

Build the image from the repo root with `make battery-image`.
