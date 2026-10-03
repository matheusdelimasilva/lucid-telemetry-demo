# privacy/

Privacy regression inputs: `fields.yaml`, the output allowlist, and the
reviewed field map that decides which sensitive fields (vin, lat, lon) may
leave the jobs. Checked in stage 5 as part of the reference implementation.

- `fields.yaml`: per-topic classification of every proto field as
  `vin`, `location` or `none`.
- `output-allowlist.yaml`: per job, the logical output topics and the exact
  fields a record may carry (`.shadow` names normalize first).
- `field-map.yaml`: the reviewed input→output field map for
  `charging-sessions` plus assertions that `start_lat`/`start_lon` are the
  contract rounding of the opening `plug_in`'s coordinates. `battery-health`
  has no field map (SPEC.md gives none) and no location outputs.

All three files carry `review.status: pending-human-review` — drafted in
stage 4a, changes need privacy-reviewer approval. Checked by
`make privacy-check JOB=<job> RUN=<run-dir>` (tools/privacy_check.py).
