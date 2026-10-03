.PHONY: up down smoke-rust smoke-py spark-image spark-hello proto-py proto-check check-examples

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
