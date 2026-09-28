#!/bin/sh
# Stage-1 throwaway check: prove the pinned protoc handles proto3 `optional`
# (needed for lat/lon/charger_type presence semantics in the real schemas,
# which arrive in stage 2). Writes the check proto to /tmp so nothing
# throwaway lands under proto/.
set -eu

cat > /tmp/opt.proto <<'EOF'
syntax = "proto3";

package check;

message Opt {
  optional double lat = 1;
  optional string charger_type = 2;
}
EOF

protoc --version
protoc -I /tmp --descriptor_set_out=/tmp/opt.desc /tmp/opt.proto
protoc -I /tmp --python_out=/tmp /tmp/opt.proto
test -s /tmp/opt.desc
test -s /tmp/opt_pb2.py
echo "proto3 optional check OK"
