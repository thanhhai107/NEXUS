"""
TPC-DI Schema-Definition Error Injector.

Mutates JSON schema files in ``domains/tpc/schemas/`` to simulate schema drift
scenarios.  Always backs up the original before mutating; call ``revert()``
to restore (or use as a context manager).

Supported mutation_types:
  remove_required_field_from_schema — Remove a field from the ``required`` list.
  change_field_type_in_schema       — Change a field's ``type`` value.
  add_unknown_field_to_schema       — Insert an extra field into ``properties``.
  remove_downstream_field           — Remove a field used by a downstream Gold job.

Usage::

    injector = SchemaInjector(seed=42)
    rec = injector.inject(
        "tpcdi_dim_trade",
        "change_field_type_in_schema",
        field="trade_dts",
        new_type="integer",
    )
    # ... run pipeline ...
    injector.revert_all()
"""

from __future__ import annotations

import json
import random
import shutil
from pathlib import Path
from typing import Any

from common.tpcdi_sources import get_schema_path

PROJECT_ROOT = Path(__file__).resolve().parents[3]


class SchemaInjector:
    """Mutate JSON schema files to inject schema drift scenarios."""

    def __init__(self, seed: int = 42):
        self.seed = seed
        self.rng = random.Random(seed)
        # Track (schema_path, backup_path) pairs for batch revert
        self._backups: list[tuple[Path, Path]] = []

    # ── Public API ───────────────────────────────────────────────────────────

    def inject(
        self,
        schema_name: str,
        mutation_type: str,
        *,
        field: str | None = None,
        new_type: str | None = None,
        backup_dir: Path | None = None,
    ) -> dict[str, Any]:
        """Mutate one schema file and return a mutation record.

        Parameters
        ----------
        schema_name:
            Dataset schema name, e.g. ``tpcdi_dim_trade``.
        mutation_type:
            One of the supported mutation types.
        field:
            Target field name; randomly chosen if None.
        new_type:
            New JSON Schema type string (used by ``change_field_type_in_schema``).
        backup_dir:
            Directory to store the backup file.  Defaults to a ``schema_backups/``
            sibling of the schema file.
        """
        schema_path = get_schema_path(schema_name)
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")

        schema = json.loads(schema_path.read_text(encoding="utf-8"))

        # Backup
        bdir = Path(backup_dir) if backup_dir else schema_path.parent / "schema_backups"
        bdir.mkdir(parents=True, exist_ok=True)
        backup_path = bdir / schema_path.name
        shutil.copy(schema_path, backup_path)
        self._backups.append((schema_path, backup_path))

        # Mutate
        resolved_field = self._apply_mutation(schema, mutation_type, field, new_type)
        schema_path.write_text(json.dumps(schema, indent=2), encoding="utf-8")

        return {
            "injector_type": "schema",
            "mutation_type": mutation_type,
            "schema_name": schema_name,
            "field": resolved_field,
            "schema_path": str(schema_path),
            "backup_path": str(backup_path),
        }

    def revert_all(self) -> None:
        """Restore all mutated schema files from their backups."""
        for schema_path, backup_path in self._backups:
            if backup_path.exists():
                shutil.copy(backup_path, schema_path)
        self._backups.clear()

    def revert_one(self, schema_name: str, backup_path: str | Path) -> None:
        """Restore a single schema file from a specific backup."""
        schema_path = get_schema_path(schema_name)
        shutil.copy(Path(backup_path), schema_path)

    # ── Context manager ──────────────────────────────────────────────────────

    def __enter__(self) -> "SchemaInjector":
        return self

    def __exit__(self, *_: Any) -> None:
        self.revert_all()

    # ── Internal ─────────────────────────────────────────────────────────────

    def _apply_mutation(
        self,
        schema: dict[str, Any],
        mutation_type: str,
        field: str | None,
        new_type: str | None,
    ) -> str | None:
        """Mutate the schema dict in-place. Returns the resolved field name."""
        properties: dict[str, Any] = schema.get("properties", {})
        required: list[str] = schema.get("required", [])

        if mutation_type == "remove_required_field_from_schema":
            if not required:
                raise ValueError("Schema has no required fields to remove")
            f = field if field and field in required else self.rng.choice(required)
            schema["required"] = [x for x in required if x != f]
            return f

        if mutation_type == "change_field_type_in_schema":
            if not properties:
                raise ValueError("Schema has no properties to change")
            f = field if field and field in properties else self.rng.choice(list(properties))
            old_type = properties[f].get("type")
            # Choose a different type so the change is meaningful
            target = new_type or ("string" if old_type != "string" else "integer")
            properties[f]["type"] = target
            return f

        if mutation_type == "add_unknown_field_to_schema":
            marker = "__injected_unknown__"
            properties[marker] = {"type": "string"}
            return marker

        if mutation_type == "remove_downstream_field":
            if not required:
                raise ValueError("Schema has no required fields to remove")
            f = field if field and field in required else self.rng.choice(required)
            properties.pop(f, None)
            schema["required"] = [x for x in required if x != f]
            return f

        raise ValueError(f"Unknown schema mutation_type: {mutation_type!r}")


__all__ = ["SchemaInjector"]
