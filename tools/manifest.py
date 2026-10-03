"""baseline/manifest.json computation (stage 4a; split in 4b).

`manifest.py compute --run <dir>` prints the manifest;
`manifest.py write --run <dir>` writes baseline/manifest.json
(indent 2, sorted keys, trailing newline).

The manifest has two halves. `compute_repo()` hashes what is pinned in the
repo (legacy-spark/ tree, proto/, fixtures, generator run, Dockerfile) and is
what tools/parity.py verifies against the committed manifest. `compute_run()`
reads the Spark replay settings out of a baseline run's `<job>.run.json`
(Spark/Scala/Java versions, Spark conf, watermark delays) and is only used
when the baseline is written. `compute()` is both, merged.
"""

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
GENERATOR_RUN = REPO / "parity" / "replay" / "generator-run.json"
DOCKERFILE = "docker/spark.Dockerfile"
GENERATOR_IMAGE = "python:3.12.11-slim"
MANIFEST = REPO / "baseline" / "manifest.json"

SPARK_CONF_KEYS = (
    "spark.master",
    "spark.sql.shuffle.partitions",
    "spark.sql.session.timeZone",
    "spark.sql.streaming.noDataMicroBatches.enabled",
)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def legacy_spark_tree() -> str:
    """Tree sha1 of the WORKING TREE legacy-spark/, honouring .gitignore,
    via a temporary index."""
    with tempfile.NamedTemporaryFile(delete=True) as tmp:
        env = dict(os.environ, GIT_INDEX_FILE=tmp.name)
        for args in (["read-tree", "HEAD"], ["add", "-A", "legacy-spark"],
                     ["write-tree", "--prefix=legacy-spark/"]):
            out = subprocess.run(["git", "-C", str(REPO)] + args, env=env,
                                 capture_output=True, text=True)
            if out.returncode != 0:
                sys.exit(f"git {' '.join(args)} failed:\n{out.stderr}")
        return out.stdout.strip()


def proto_sha256() -> str:
    """sha256 of the text '<sha256 of file bytes>  <repo-relative path>\n'
    per file under proto/, sorted by path."""
    files = sorted(p for p in (REPO / "proto").rglob("*") if p.is_file())
    text = "".join(f"{sha256_file(p)}  {p.relative_to(REPO)}\n" for p in files)
    return sha256_bytes(text.encode("utf-8"))


def compute_repo() -> dict:
    """Everything the manifest pins that lives in the repo, not in a run."""
    gen = json.loads(GENERATOR_RUN.read_text(encoding="utf-8"))
    jobs = list(gen["fixtures"])
    return {
        "legacy_spark_tree": legacy_spark_tree(),
        "proto_sha256": proto_sha256(),
        "fixtures": {
            job: {
                "path": f"parity/replay/{job}.jsonl",
                "sha256": sha256_file(REPO / "parity" / "replay" / f"{job}.jsonl"),
            } for job in jobs
        },
        "generator": {
            "seed": gen["seed"],
            "python_version": gen["python_version"],
            "protobuf_version": gen["protobuf_version"],
            "image": GENERATOR_IMAGE,
        },
        "replay": {
            "dockerfile": DOCKERFILE,
            "dockerfile_sha256": sha256_file(REPO / DOCKERFILE),
        },
    }


def compute_run(run_dir) -> dict:
    """The Spark replay settings of a baseline run (every job's run.json must agree)."""
    run_dir = Path(run_dir)
    gen = json.loads(GENERATOR_RUN.read_text(encoding="utf-8"))
    jobs = list(gen["fixtures"])

    runs = {}
    for job in jobs:
        path = run_dir / f"{job}.run.json"
        if not path.is_file():
            sys.exit(f"missing run metadata {path}")
        runs[job] = json.loads(path.read_text(encoding="utf-8"))

    envs = {job: runs[job].get("replay") for job in jobs}
    if len({json.dumps(e, sort_keys=True) for e in envs.values()}) != 1:
        sys.exit("run.json replay environments disagree between jobs: "
                 + json.dumps(envs, indent=2, sort_keys=True))
    env = envs[jobs[0]]

    conf = env.get("spark_conf", {})
    missing = [k for k in SPARK_CONF_KEYS if k not in conf]
    if missing:
        sys.exit(f"run.json spark_conf missing keys: {missing}")

    return {
        "replay": {
            "spark_version": env["spark_version"],
            "scala_version": env["scala_version"],
            "java_version": env["java_version"],
            "spark_conf": {k: conf[k] for k in SPARK_CONF_KEYS},
            "watermark_delay_ms": {job: runs[job]["watermark_delay_ms"] for job in jobs},
        },
    }


def compute(run_dir) -> dict:
    manifest = compute_repo()
    manifest["replay"].update(compute_run(run_dir)["replay"])
    return manifest


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=("compute", "write"))
    p.add_argument("--run", required=True)
    args = p.parse_args()
    manifest = compute(args.run)
    if args.command == "compute":
        print(json.dumps(manifest, indent=2, sort_keys=True))
    else:
        MANIFEST.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                            encoding="utf-8")
        print(f"wrote {MANIFEST.relative_to(REPO)}")


if __name__ == "__main__":
    main()
