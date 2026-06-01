"""
Data Caterer Integration Module for NEXUS.

Bridges NEXUS data generation pipeline with the Data Caterer tool
(pflooky/data-caterer), a metadata-driven data generation engine
that runs via Docker + Spark and supports batch, streaming, and
API-based data generation with error injection.

Data Caterer docs: https://github.com/pflooky/data-caterer
NEXUS TPC plans live in: ingestion/data_caterer/plans/
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PLANS_DIR = PROJECT_ROOT / "ingestion" / "data_caterer" / "plans"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "runtime" / "datasets"
DOCKER_IMAGE = "pflooky/data-caterer:0.5.0"

def _ensure_dir(path: Path) -> Path:
    try:
        path.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        import tempfile
        alt = Path(tempfile.gettempdir()) / "nexus-datasets"
        alt.mkdir(parents=True, exist_ok=True)
        return alt
    return path

# Error injection presets
ERROR_PRESETS = {
    "none": {
        "null_ratio": 0.0,
        "duplicate_ratio": 0.0,
        "type_error_ratio": 0.0,
        "overflow_ratio": 0.0,
        "fk_violation_ratio": 0.0,
        "schema_drift_ratio": 0.0,
    },
    "low": {
        "null_ratio": 0.01,
        "duplicate_ratio": 0.005,
        "type_error_ratio": 0.005,
        "overflow_ratio": 0.002,
        "fk_violation_ratio": 0.002,
        "schema_drift_ratio": 0.001,
    },
    "moderate": {
        "null_ratio": 0.05,
        "duplicate_ratio": 0.02,
        "type_error_ratio": 0.02,
        "overflow_ratio": 0.01,
        "fk_violation_ratio": 0.01,
        "schema_drift_ratio": 0.005,
    },
    "high": {
        "null_ratio": 0.15,
        "duplicate_ratio": 0.05,
        "type_error_ratio": 0.05,
        "overflow_ratio": 0.03,
        "fk_violation_ratio": 0.03,
        "schema_drift_ratio": 0.02,
    },
    "extreme": {
        "null_ratio": 0.30,
        "duplicate_ratio": 0.10,
        "type_error_ratio": 0.10,
        "overflow_ratio": 0.05,
        "fk_violation_ratio": 0.05,
        "schema_drift_ratio": 0.05,
    },
}

EXPORT_FORMATS = {
    "csv": {"extension": ".csv", "write_mode": "overwrite", "header": True},
    "json": {"extension": ".jsonl", "write_mode": "overwrite", "header": False},
    "parquet": {"extension": ".parquet", "write_mode": "overwrite", "header": False},
    "orc": {"extension": ".orc", "write_mode": "overwrite", "header": False},
}


@dataclass
class DataCatererConfig:
    plan_name: str
    plan_dir: Path = field(default=PLANS_DIR)
    output_dir: Path = field(default=DEFAULT_OUTPUT_DIR)
    output_formats: list[str] = field(default_factory=lambda: ["csv", "parquet"])
    scale_factor: int = 1
    error_profile: str = "moderate"
    mode: str = "batch"
    kafka_bootstrap: str = "localhost:29092"
    kafka_topic_prefix: str = "nexus.tpc"
    api_port: int = 8080
    run_id: str = ""
    spark_master: str = "local[*]"
    docker_image: str = DOCKER_IMAGE

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_name": self.plan_name,
            "output_dir": str(self.output_dir),
            "output_formats": self.output_formats,
            "scale_factor": self.scale_factor,
            "error_profile": self.error_profile,
            "mode": self.mode,
            "run_id": self.run_id,
        }


def load_plan(plan_name: str) -> dict[str, Any]:
    plan_path = PLANS_DIR / f"{plan_name}.yml"
    if not plan_path.exists():
        raise FileNotFoundError(f"Data Caterer plan not found: {plan_path}")
    with plan_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def list_plans() -> list[str]:
    return sorted(
        p.stem for p in PLANS_DIR.glob("*.yml")
        if not p.name.startswith("data_caterer_global")
    )


def load_global_config() -> dict[str, Any]:
    path = PLANS_DIR / "data_caterer_global.yml"
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    return {}


def _build_docker_command(config: DataCatererConfig) -> list[str]:
    plan_mount = str(config.plan_dir)
    output_mount = str(config.output_dir)
    output_mount_parent = str(config.output_dir.parent)

    cmd = [
        "docker", "run", "--rm",
        "-v", f"{plan_mount}:/opt/app/plans:ro",
        "-v", f"{output_mount_parent}:/opt/app/data",
        "-e", f"PLAN_NAME={config.plan_name}",
        "-e", f"SCALE_FACTOR={config.scale_factor}",
        "-e", f"ERROR_PROFILE={config.error_profile}",
        "-e", f"OUTPUT_FORMATS={','.join(config.output_formats)}",
        "-e", f"OUTPUT_DIR=/opt/app/data/{config.output_dir.name}",
        "-e", f"RUN_ID={config.run_id}",
        "-e", f"SPARK_MASTER={config.spark_master}",
    ]

    if config.mode == "streaming":
        cmd.extend([
            "-e", f"KAFKA_BOOTSTRAP={config.kafka_bootstrap}",
            "-e", f"KAFKA_TOPIC_PREFIX={config.kafka_topic_prefix}",
        ])
    elif config.mode == "api":
        cmd.extend([
            "-p", f"{config.api_port}:8080",
        ])

    cmd.append(config.docker_image)
    return cmd


def _build_direct_command(config: DataCatererConfig) -> list[str]:
    """Build command for running data-caterer directly (no Docker).
    Requires data-caterer JAR and Spark installation."""
    plan_path = PLANS_DIR / f"{config.plan_name}.yml"

    return [
        "spark-submit",
        "--master", config.spark_master,
        "--class", "io.github.datacatering.datacaterer.Launcher",
        "data-caterer.jar",
        "--plan", str(plan_path),
        "--scale-factor", str(config.scale_factor),
        "--error-profile", config.error_profile,
        "--output-dir", str(config.output_dir),
        "--output-formats", ",".join(config.output_formats),
        "--run-id", config.run_id,
        "--mode", config.mode,
    ]


def run_plan(
    config: DataCatererConfig,
    use_docker: bool = True,
    dry_run: bool = False,
    timeout_seconds: int = 3600,
) -> dict[str, Any]:
    """Execute a Data Caterer data generation plan.

    Args:
        config: DataCatererConfig with plan settings
        use_docker: Use Docker to run data-caterer
        dry_run: Print command but don't execute
        timeout_seconds: Max runtime in seconds

    Returns:
        Dict with status, output_paths, record_counts, errors
    """
    config.output_dir = _ensure_dir(config.output_dir)

    if use_docker:
        cmd = _build_docker_command(config)
    else:
        cmd = _build_direct_command(config)

    if dry_run:
        return {
            "status": "dry_run",
            "command": " ".join(cmd),
            "config": config.to_dict(),
        }

    start_time = time.time()
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            cwd=str(PROJECT_ROOT),
        )
        elapsed = time.time() - start_time

        output_files = _discover_output_files(config)
        record_counts = _count_records(output_files)

        return {
            "status": "success" if result.returncode == 0 else "failed",
            "exit_code": result.returncode,
            "stdout": result.stdout[-5000:],
            "stderr": result.stderr[-5000:],
            "elapsed_seconds": round(elapsed, 1),
            "output_files": output_files,
            "record_counts": record_counts,
            "config": config.to_dict(),
        }
    except subprocess.TimeoutExpired:
        return {
            "status": "timeout",
            "elapsed_seconds": timeout_seconds,
            "config": config.to_dict(),
        }
    except FileNotFoundError:
        return {
            "status": "error",
            "error": "Docker not found. Install Docker or set use_docker=False with Spark installed.",
            "config": config.to_dict(),
        }


def _discover_output_files(config: DataCatererConfig) -> list[str]:
    """Discover generated output files in the output directory."""
    files = []
    if config.output_dir.exists():
        for p in config.output_dir.rglob("*"):
            if p.is_file():
                files.append(str(p.relative_to(config.output_dir)))
    return sorted(files)


def _count_records(file_list: list[str]) -> dict[str, int]:
    """Count records in generated output files."""
    counts: dict[str, int] = {}
    return counts


def generate_tpcdi(
    scale_factor: int = 1,
    error_profile: str = "moderate",
    output_formats: list[str] | None = None,
    run_id: str = "",
    dry_run: bool = False,
) -> dict[str, Any]:
    """Generate TPC-DI benchmark data (SF=1 by default)."""
    if output_formats is None:
        output_formats = ["csv", "parquet"]
    config = DataCatererConfig(
        plan_name="tpcdi_tasks",
        scale_factor=scale_factor,
        error_profile=error_profile,
        output_formats=output_formats,
        output_dir=DEFAULT_OUTPUT_DIR / "tpcdi",
        mode="batch",
        run_id=run_id,
    )
    return run_plan(config, dry_run=dry_run, timeout_seconds=7200)


def generate_all(
    error_profile: str = "moderate",
    run_id: str = "",
    dry_run: bool = False,
) -> dict[str, Any]:
    return {"tpcdi": generate_tpcdi(error_profile=error_profile, run_id=run_id, dry_run=dry_run)}


# =============================================================================
# Schema generation helpers — write JSON schemas from TPC definitions
# =============================================================================

def write_tpc_schemas_to_runtime() -> dict[str, Path]:
    """Copy TPC schemas to runtime catalog for ingestion pipeline use."""
    schemas_src = PROJECT_ROOT / "domains" / "tpc" / "schemas"
    schemas_dst = PROJECT_ROOT / "runtime" / "catalog" / "schemas" / "tpc"
    schemas_dst.mkdir(parents=True, exist_ok=True)

    written: dict[str, Path] = {}
    for schema_file in sorted(schemas_src.glob("*.schema.json")):
        dst = schemas_dst / schema_file.name
        dst.write_bytes(schema_file.read_bytes())
        written[schema_file.stem] = dst
    return written
