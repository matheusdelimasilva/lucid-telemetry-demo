.PHONY: up down smoke-rust smoke-py spark-image spark-hello proto-py proto-check check-examples spark-examples

VENV := .venv
PY := $(VENV)/bin/python

up:
	docker compose up -d --wait

down:
	docker compose down -v

smoke-rust:
	cd stream-rs && cargo run --release -p smoke

proto-py:
	python -m grpc_tools.protoc -I proto --python_out=generator proto/*.proto

$(VENV):
	python3 -m venv $(VENV)
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -r generator/requirements.txt

smoke-py: $(VENV)
	$(PY) -m grpc_tools.protoc -I proto --python_out=generator proto/*.proto
	$(PY) generator/smoke_send.py

check-examples: $(VENV)
	$(PY) -m grpc_tools.protoc -I proto --python_out=generator proto/*.proto
	$(PY) parity/replay/check_examples.py

spark-image:
	docker build -f docker/spark.Dockerfile -t lucid-spark-hello:dev .

spark-hello:
	docker run --rm lucid-spark-hello:dev

proto-check:
	docker run --rm --entrypoint sh lucid-spark-hello:dev /app/check-proto-optional.sh

SPARK_EXAMPLES_OUT := build/spark-examples

spark-examples: spark-image
	rm -rf $(SPARK_EXAMPLES_OUT)
	mkdir -p $(dir $(SPARK_EXAMPLES_OUT))
	docker rm -f lucid-spark-examples >/dev/null 2>&1 || true
	status=0; \
	docker run --name lucid-spark-examples -w /legacy-spark --entrypoint sbt lucid-spark-hello:dev \
		"replayHarness/run --descriptor /app/telemetry.desc --out /out /repo/parity/examples /repo/parity/probes" \
		|| status=$$?; \
	docker cp lucid-spark-examples:/out $(SPARK_EXAMPLES_OUT) || true; \
	docker rm -f lucid-spark-examples >/dev/null 2>&1 || true; \
	exit $$status
