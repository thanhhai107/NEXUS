#!/usr/bin/env python3
"""Environment diagnostics for NEXUS.

Displays current execution environment information:
- Local vs VM mode
- Distributed vs local execution
- Worker configuration
- Storage paths

Usage:
    python scripts/diagnose_environment.py
"""

from __future__ import annotations

import platform
import socket
from pathlib import Path


def main() -> int:
    print("=" * 60)
    print("NEXUS ENVIRONMENT DIAGNOSTICS")
    print("=" * 60)

    # Import after sys.path setup
    import sys
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    from common.config import (
        is_vm_mode,
        is_distributed_mode,
        get_execution_mode,
        get_runtime_mode,
        RUNTIME_DIR,
        LAKE_DIR,
        BRONZE_DIR,
    )
    from common.worker import get_worker_info

    # Samples directory
    SAMPLES_DIR = project_root / "assets" / "samples"

    # Section 1: Basic Environment
    print("\n[1] BASIC ENVIRONMENT")
    print("-" * 40)
    print(f"  Platform:      {platform.system()} {platform.release()}")
    print(f"  Hostname:      {socket.gethostname()}")
    print(f"  Python:        {platform.python_version()}")
    print(f"  PID:           {__import__('os').getpid()}")

    # Section 2: Data Storage
    print("\n[2] DATA STORAGE")
    print("-" * 40)
    print(f"  Runtime Mode:  {get_runtime_mode()}")
    print(f"  Is VM Mode:    {is_vm_mode()}")
    print(f"  Runtime Dir:   {RUNTIME_DIR}")
    print(f"  Lake Dir:      {LAKE_DIR}")
    print(f"  Bronze Dir:    {BRONZE_DIR}")
    print(f"  Samples Dir:   {SAMPLES_DIR}")

    # Section 3: Distributed Execution
    print("\n[3] DISTRIBUTED EXECUTION")
    print("-" * 40)
    print(f"  Distributed:   {is_distributed_mode()}")
    print(f"  Execution:     {get_execution_mode()}")
    print(f"  Auto-detect sources:")
    print(f"    - AIRFLOW_WORKER_NUMBER: {__import__('os').getenv('AIRFLOW_WORKER_NUMBER', '(not set)')}")
    print(f"    - KUBERNETES_SERVICE_HOST: {'(set)' if __import__('os').getenv('KUBERNETES_SERVICE_HOST') else '(not set)'}")
    print(f"    - SPARK_EXECUTOR_ID: {__import__('os').getenv('SPARK_EXECUTOR_ID', '(not set)')}")

    # Section 4: Worker Info
    print("\n[4] WORKER CONFIGURATION")
    print("-" * 40)
    info = get_worker_info()
    print(f"  Worker ID:          {info.worker_id}")
    print(f"  Worker Index:       {info.worker_index}")
    print(f"  Total Workers:       {info.total_workers}")
    print(f"  Is Coordinator:      {info.is_coordinator}")
    print(f"  Is Distributed:      {info.is_distributed}")
    print(f"  Supports Parallel I/O: {info.supports_parallel_io}")
    print(f"  Supports Multiprocess: {info.supports_multiprocess}")

    # Section 5: Execution Mode Summary
    print("\n[5] EXECUTION MODE SUMMARY")
    print("-" * 40)
    execution_mode = get_execution_mode()
    mode_descriptions = {
        "local_single": "Single process on local machine (no distributed)",
        "local_multi": "Multiple workers on local machine (no distributed)",
        "distributed_single": "Single worker in distributed cluster",
        "distributed_multi": "Multiple workers in distributed cluster",
    }
    desc = mode_descriptions.get(execution_mode, "Unknown")
    print(f"  Mode: {execution_mode}")
    print(f"  Description: {desc}")

    # Section 6: Recommendations
    print("\n[6] RECOMMENDATIONS")
    print("-" * 40)

    if is_vm_mode() and is_distributed_mode():
        print("  [*] Running in distributed mode on VM")
        print("  [*] Use worker coordination features")
        print("  [*] Data stored in /data/lake/")
    elif is_vm_mode() and not is_distributed_mode():
        print("  [!] VM mode but not distributed")
        print("  -> Set NEXUS_DISTRIBUTED_MODE=true to enable worker features")
    elif not is_vm_mode() and is_distributed_mode():
        print("  [!] Distributed mode but not VM")
        print("  -> Data will be stored in runtime/lake/")
    else:
        print("  [*] Running in local single-worker mode")
        print("  -> Set NEXUS_DISTRIBUTED_MODE=true for multi-worker")

    print("\n" + "=" * 60)
    print("END OF DIAGNOSTICS")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
