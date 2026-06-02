"""Shared test fixtures and environment setup.

Force local storage for all tests so they don't require external
infrastructure (MinIO S3, PostgreSQL, OpenLineage).
"""

import os

os.environ["NEXUS_RUNTIME_MODE"] = "local"
os.environ["NEXUS_GOVERNANCE_STORAGE"] = "local"
os.environ["NEXUS_FORCE_VM"] = ""
os.environ["NEXUS_DISTRIBUTED_MODE"] = "false"
