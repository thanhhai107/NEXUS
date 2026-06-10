# Ingestion

Current ingestion code is scoped to the TPC-DI benchmark.

Active pieces:

- `ingestion/tpcdi/parsers/` reads DIGen source files configured in
  `domains/tpc/tpcdi_sources.yml`.
- `ingestion/tpcdi/error_injection/` creates deterministic source-file mutation
  scenarios for detect, recover, and score runs.
- `ingestion/data_caterer/` can generate synthetic TPC-DI table data through the
  Docker/Spark generator path.
- `ingestion/canonical/` and `ingestion/batch/common.py` provide shared record
  envelope and CSV/JSONL helpers used by the local CLI.

The old live-source domain docs and adapters have been removed from this tree;
the benchmark surface is TPC-DI only.
