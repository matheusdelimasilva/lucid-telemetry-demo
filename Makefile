.PHONY: up down smoke-rust smoke-py spark-image spark-hello proto-py

VENV := .venv
PY := $(VENV)/bin/python

up:
	docker compose up -d --wait

down:
	docker compose down -v

smoke-rust:
	cd stream-rs && cargo run --release -p smoke

proto-py:
	python -m grpc_tools.protoc -I proto --python_out=generator proto/smoke.proto

smoke-py:
	python3 -m venv $(VENV)
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -r generator/requirements.txt
	$(PY) -m grpc_tools.protoc -I proto --python_out=generator proto/smoke.proto
	$(PY) generator/smoke_send.py

spark-image:
	docker build -f docker/spark.Dockerfile -t lucid-spark-hello:stage1 .

spark-hello:
	docker run --rm lucid-spark-hello:stage1
