.PHONY: up down smoke-rust smoke-py spark-image spark-hello proto-py proto-check check-examples spark-examples spark-baseline baseline-repro baseline parity privacy-check fixtures

VENV := .venv
PY := $(VENV)/bin/python
PYTHON ?= python3

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

SPARK_BASELINE_OUT := build/spark-baseline

spark-baseline: spark-image
	rm -rf $(SPARK_BASELINE_OUT)
	mkdir -p $(dir $(SPARK_BASELINE_OUT))
	docker rm -f lucid-spark-baseline >/dev/null 2>&1 || true
	status=0; \
	start=$$(date +%s); \
	docker run --name lucid-spark-baseline -w /legacy-spark --entrypoint sbt lucid-spark-hello:dev \
		"replayHarness/run --descriptor /app/telemetry.desc --out /out --fixture charging-sessions=/repo/parity/replay/charging-sessions.jsonl --fixture battery-health=/repo/parity/replay/battery-health.jsonl" \
		|| status=$$?; \
	echo "spark-baseline wall: $$(( $$(date +%s) - start ))s"; \
	docker cp lucid-spark-baseline:/out $(SPARK_BASELINE_OUT) || true; \
	docker rm -f lucid-spark-baseline >/dev/null 2>&1 || true; \
	exit $$status

baseline-repro: spark-baseline
	@status=0; \
	for job in charging-sessions battery-health; do \
		for f in records.jsonl counters.json trace.jsonl; do \
			if cmp -s $(SPARK_BASELINE_OUT)/$$job.$$f parity/golden/$$job.$$f; then \
				echo "OK $$job.$$f"; \
			else \
				echo "DIFFER $$job.$$f"; status=1; \
			fi; \
		done; \
	done; \
	exit $$status

baseline: spark-baseline
	for job in charging-sessions battery-health; do \
		for f in records.jsonl counters.json trace.jsonl; do \
			cp $(SPARK_BASELINE_OUT)/$$job.$$f parity/golden/$$job.$$f; \
		done; \
	done
	$(PYTHON) tools/manifest.py write --run $(SPARK_BASELINE_OUT)

parity:
	$(PYTHON) tools/parity.py --job $(JOB) --run $(RUN)

privacy-check:
	$(PYTHON) tools/privacy_check.py --job $(JOB) --run $(RUN)

# Fixtures are generated in a pinned Python image so the recorded version is the one used.
PY_IMAGE := python:3.12.11-slim
FIXTURES_OUT := parity/replay

fixtures:
	mkdir -p $(FIXTURES_OUT)
	docker run --rm -u $$(id -u):$$(id -g) -e HOME=/tmp -v "$(CURDIR)":/w -w /w $(PY_IMAGE) sh -c '\
		pip install -q --disable-pip-version-check --target /tmp/deps -r generator/requirements.txt && \
		mkdir -p /tmp/pb && PYTHONPATH=/tmp/deps python -m grpc_tools.protoc -I proto --python_out=/tmp/pb proto/*.proto && \
		PYTHONPATH=/tmp/deps PB2_DIR=/tmp/pb python generator/generate.py --out $(FIXTURES_OUT)'
