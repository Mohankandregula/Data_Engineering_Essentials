# Data Engineering Essentials

Hands-on projects covering core data engineering concepts — storage formats, object storage, table formats, distributed compute, and data version control — with a focus on building infrastructure for Large Language Models.

Each project is self-contained with detailed notes and runnable Jupyter notebooks.

---

## Projects

### [1. Data Engineering for LLMs](de_for_llms/)

Building the data infrastructure that powers LLM training pipelines — from raw web data to training-ready datasets.

**Based on:** [Data Engineering for Large Models](https://datascale-ai.github.io/data_engineering_book/en/) by DataScale AI

**What's covered:**

| Topic | What You'll Learn | Notebook |
|-------|------------------|----------|
| **Parquet Deep Dive** | Columnar storage internals, row groups, compression algorithms (Snappy/ZSTD/LZ4), column pruning, partition pruning | [ch02_s1_parquet_deep_dive.ipynb](de_for_llms/notebooks/ch02_s1_parquet_deep_dive.ipynb) |
| **MinIO (Local S3)** | Object storage, flat namespace, boto3 API, pipeline pattern (raw → processed → training), object metadata for lineage | [ch02_s2_minio.ipynb](de_for_llms/notebooks/ch02_s2_minio.ipynb) |
| **Apache Iceberg** | Three-level metadata tree, ACID transactions, snapshots, time travel, schema evolution, row-level deletes, copy-on-write vs merge-on-read | [ch02_s3_iceberg.ipynb](de_for_llms/notebooks/ch02_s3_iceberg.ipynb) |
| **FineWeb Analysis** | Analyzing HuggingFace's 15T token dataset — quality heuristics, filtering pipeline, data funnel simulation | [ch01_fineweb_analysis.ipynb](de_for_llms/notebooks/ch01_fineweb_analysis.ipynb) |

**Key notes:**
- [Chapter 1: Data Revolution in the LLM Era](de_for_llms/chapters/ch01_data_revolution/NOTES.md) — Scaling laws, Chinchilla, data quality vs quantity
- [Chapter 2: Data Infrastructure Selection](de_for_llms/chapters/ch02_infrastructure/NOTES.md) — Storage, compute, formats, version control, enterprise data lake architectures, Iceberg internals

**Status:** In progress — Spark vs Ray and DVC sessions coming next

---

## Tech Stack

- **Storage:** MinIO (S3-compatible), Apache Iceberg (table format)
- **Formats:** Parquet, JSONL, CSV (for comparison)
- **Libraries:** PyArrow, boto3, PyIceberg, pandas, matplotlib
- **Infrastructure:** Docker (MinIO), SQLite (Iceberg catalog)

## Getting Started

1. Clone the repo
   ```bash
   git clone https://github.com/Mohankandregula/Date_engineering_essentials.git
   cd Date_engineering_essentials
   ```

2. Set up a Python environment
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install pandas pyarrow matplotlib datasets boto3 "pyiceberg[pyarrow,s3fs]"
   ```

3. Start MinIO (needed for S3 and Iceberg notebooks)
   ```bash
   docker run -d --name minio \
     -p 9000:9000 -p 9001:9001 \
     minio/minio server /data --console-address ":9001"
   ```

4. Open any notebook and run it

## License

See [LICENSE](LICENSE) for details.
