# SQL Server Bulk Copy Benchmarks

Benchmarks comparing every practical way to bulk-insert data into SQL Server from Python.

13 methods tested across three tiers — from 1K to 10M rows. Everything runs against a local SQL Server 2022 docker container so the results are reproducible.

## Methods tested

| Category | Methods |
|----------|---------|
| Raw drivers | pyodbc executemany, pyodbc fast_executemany, pymssql, mssql-python |
| pandas / SQLAlchemy | to_sql (default), to_sql (fast), to_sql (multi), Core insert |
| BCP-based | bcp CLI, bcpandas, arrowsqlbcpy (SqlBulkCopy) |
| Server-side | BULK INSERT (T-SQL), Table-Valued Parameters |

## Quick start

Everything runs from the notebook — no need to jump to terminal.

```bash
pip install -r requirements.txt
jupyter notebook notebook.ipynb
```

The notebook handles docker-compose, database setup, and result loading in the first few cells. Just run them in order.

If you prefer to set up manually:

```bash
docker-compose up -d
cp .env.example .env      # edit if needed
python db_setup.py
jupyter notebook notebook.ipynb
```

## Requirements

- Docker (for SQL Server 2022)
- Python 3.10+
- ODBC Driver 17 for SQL Server
- bcp CLI (for bcp/bcpandas/BULK INSERT tests — comes with `mssql-tools`)

Optional: `mssql-python`, `turbodbc`, `arrowsqlbcpy` — picked up automatically if installed.

## Project structure

```
config.py          connection strings and settings
test_data.py       generates reproducible test DataFrames
db_setup.py        creates database, tables, TVP types, stored procs
timing.py          benchmark timing harness
methods.py         all 14 insert method implementations
notebook.ipynb     full results with charts and analysis
benchmarks/        richbench-compatible files for terminal runs
results/           pre-computed benchmark CSVs and chart PNGs
staging/           mount point for BULK INSERT file access
glue/              AWS Glue + bcp integration for production
docker-compose.yml SQL Server 2022 container
```

## How each method works

### Raw driver methods

**pyodbc executemany** — the default. Sends one row at a time over ODBC. It's what you get when you follow most tutorials. Painfully slow for anything over a few hundred rows.

**pyodbc fast_executemany** — same driver, one flag flip (`cursor.fast_executemany = True`). Packs the entire rowset into memory and sends it in one shot. Massive improvement, basically free to enable.

**pymssql** — uses the TDS protocol directly instead of going through ODBC. Fewer moving parts, but no equivalent of fast_executemany.

**mssql-python** — Microsoft's new driver (2025). Bypasses ODBC entirely with a native C++ binding.

### pandas / SQLAlchemy methods

**to_sql (default)** — `df.to_sql()` with no tricks. Uses SQLAlchemy's default INSERT path.

**to_sql (fast_executemany)** — same thing but with `create_engine(..., fast_executemany=True)`. This is what StackOverflow tells you to do, and it actually works well.

**to_sql (method='multi')** — builds multi-row `INSERT INTO ... VALUES (...), (...), (...)` statements. SQL Server has a 2100 parameter limit per statement, so you're forced to chunk at ~350 rows for a 6-column table.

**SQLAlchemy Core insert** — skip the pandas overhead, go through `table.insert()` with a list of dicts. Paired with fast_executemany on the engine.

### BCP-based methods

**bcp CLI** — shell out to Microsoft's `bcp` utility via `subprocess`. Write a CSV, call bcp, let it handle the bulk copy protocol. No extra pip dependencies, just the bcp binary on PATH.

**bcpandas** — wraps `bcp` behind a pandas-friendly API. Writes a CSV temp file, then calls bcp under the hood. Convenient, but breaks if your data contains the delimiter character.

**arrowsqlbcpy** — uses .NET's `SqlBulkCopy` via a Python bridge. Same bulk copy protocol that SSIS uses internally. Keeps data in Arrow columnar format.

### Server-side methods

**BULK INSERT** — T-SQL statement where the server reads a file directly from disk. We mount a volume into the Docker container so it can see our CSV.

**Table-Valued Parameters (TVP)** — pass the entire dataset as a single table-typed parameter to a stored procedure. No temp files, no row-by-row. Needs a TYPE and stored proc on the server.

## Benchmark tiers

- **Tier 1**: All 13 methods at 1K, 10K, and 100K rows
- **Tier 2**: 10 fastest methods pushed to 1M rows
- **Tier 3**: Top 4 contenders at 10M rows (~1.9 GB in memory)

Most methods don't survive past 1M rows.

## Results summary

At 100K rows:
1. **TVP** — 254K rows/sec (fastest)
2. **BULK INSERT** — 66K rows/sec
3. **bcp CLI** — 36K rows/sec
4. **arrowsqlbcpy** — 28K rows/sec
5. **pyodbc fast_executemany** — 27K rows/sec
6. **pyodbc executemany** (baseline) — ~1K rows/sec

At 10M rows, only **BULK INSERT** survived (~33,600 rows/sec, ~5 minutes). TVP, bcp, and arrowsqlbcpy all failed with memory or parameter size limits.

## When to use what

| Scenario | Recommendation | Why |
|----------|---------------|-----|
| Quick script, < 10K rows | `pyodbc fast_executemany` | Fast enough, zero extra deps |
| pandas pipeline, up to 100K | `to_sql(fast_executemany=True)` | One flag on the engine |
| Maximum speed, < 1M rows | **TVP via stored proc** | 254K rows/sec, nothing else comes close |
| Bulk loading 100K-1M rows | `BULK INSERT` or `bcp CLI` | Server-side, reliable |
| Millions of rows, no ceiling | **BULK INSERT** via staged files | Only method that survives 10M+ |
| Production ETL / AWS Glue | `bcp` CLI via subprocess | Works everywhere |
| Transactional safety | TVP via stored proc | Atomic insert, no temp files |

## Gotchas

- **TVP is fast until it isn't** — 254K rows/sec at 1M, dies at 10M (parameter size limits)
- **fast_executemany + NULLs** — significantly slower with lots of NULL values
- **method='multi' hits a wall** — 2100 parameter limit means chunking at ~350 rows for 6 columns
- **pyodbc fast_executemany breaks at 1M** — parameter buffer gets too large
- **bcpandas delimiter problems** — silently corrupts data if strings contain the delimiter
- **BULK INSERT needs file access** — CSV must be readable from inside the SQL Server process
- **mssql-python is mid** — despite "2-8x over pyodbc" marketing, it landed mid-pack for bulk inserts

## AWS Glue integration

The `glue/` directory has a working example of deploying bcp inside an AWS Glue PySpark job to bulk-load SQL Server. Spark's JDBC write does row-by-row INSERTs under the hood — bcp uses native bulk copy protocol instead.

The key challenge: Glue is managed, so you can't install packages. The workaround is to package bcp + shared libraries into a zip on the exact same OS that Glue runs, upload to S3, and unpack to `/tmp` at job start.

Watch out for glibc version mismatches:

| Environment | OS | glibc |
|---|---|---|
| Glue 5.0 | Amazon Linux 2023 | 2.34 |
| Glue 4.0 | Amazon Linux 2 | 2.26 |

Binary built on AL2023 crashes on Glue 4.0. **Match the base OS exactly.** See [glue/README.md](glue/README.md) for build and deployment instructions.

## Reproducing the benchmarks

```bash
docker-compose up -d
pip install -r requirements.txt
python db_setup.py

# re-run from Python
python -c "
from methods import *
from timing import run_benchmark_suite, save_results
import db_setup

methods = {
    'pyodbc_executemany': insert_pyodbc_executemany,
    'pyodbc_fast_executemany': insert_pyodbc_fast_executemany,
    'pymssql_executemany': insert_pymssql_executemany,
    'sa_to_sql_default': insert_sa_to_sql_default,
    'sa_to_sql_fast': insert_sa_to_sql_fast,
    'sa_to_sql_multi': insert_sa_to_sql_multi,
    'sa_core_executemany': insert_sa_core_executemany,
    'bulk_insert_tsql': insert_bulk_insert_tsql,
    'bcp_cli': insert_bcp_cli,
    'tvp_pyodbc': insert_tvp_pyodbc,
    'bcpandas': insert_bcpandas,
}

results = run_benchmark_suite(
    methods, [1000, 10000, 100000],
    n_runs=3, warmup=1, teardown_fn=db_setup.truncate_table,
)
save_results(results, 'results/tier1_all_methods.csv')
"

# or quick terminal benchmark
richbench benchmarks/ --repeat 3 --times 3
```

**Tested on:** Windows 10 + Python 3.10, ODBC Driver 17 for SQL Server, SQL Server 2022 Developer in Docker (WSL2).
