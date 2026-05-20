# Source Discovery

Generated source inventory used by NEXUS.

These files are the compact output from the original `Akapi895/data-bigdata`
schema discovery work. Collector scripts are intentionally not kept here; this
project only needs the generated source catalog and schema metadata.

```text
all_schemas.json                    Consolidated source and schema catalog
endpoint_verification_report.json   Latest endpoint verification summary
discovery_report.txt                Human-readable discovery report
ingestion_coverage_map.json         Source/schema-to-ingestion coverage map
schemas/                            Per-source generated schema files
```

Use the root CLI to inspect or export schemas:

```powershell
python -m cli.nexus source-discovery summary
python -m cli.nexus source-discovery schemas
python -m cli.nexus source-discovery sync
python -m cli.nexus source-discovery coverage
```
