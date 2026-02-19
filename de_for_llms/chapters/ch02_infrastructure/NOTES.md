# Chapter 2: Data Infrastructure Selection — Notes

## Chapter Overview

Before handling TB-PB level data, infrastructure selection determines project success. Four pillars: **Storage, Compute, Format, Version Control**.

Core principle: **"Start simple, evolve on demand, avoid over-engineering."**

---

## 2.1 Storage Layer

### Object Storage (S3/MinIO)
- Flat namespace — no real folders, just `bucket/key`
- Theoretically unlimited storage
- Extreme durability: S3 claims 11 nines (99.999999999%)
- Pay per use

| Option | When to use |
|--------|------------|
| **AWS S3** | Production default |
| **MinIO** | Self-hosted S3 clone, local dev, private deployment |
| **GCS/Azure Blob** | If in their ecosystem |

### Data Lake Table Formats
Add metadata layer on top of object storage → ACID transactions, time travel, schema evolution.

| Feature | Apache Iceberg | Apache Hudi | Delta Lake |
|---------|---------------|-------------|------------|
| Backing | Netflix → Apache | Uber → Apache | Databricks |
| Engine compat | Spark, Flink, Trino, DuckDB | Spark, Flink, Presto | Primarily Spark |
| Best for | Multi-engine, vendor neutral | Stream-batch unification | Databricks ecosystem |

**Recommendation for LLM work: Iceberg** (best engine neutrality).

### Storage Decision Tree
- Data < 100TB, no ACID needed → **S3/MinIO + raw Parquet**
- Data > 100TB, multi-engine, ACID needed → **Iceberg + S3**
- Small team prototyping → **local disk + Parquet**

---

## 2.2 Compute Layer: Spark vs Ray

### Apache Spark
- Berkeley AMPLab (2009), 15 years of production validation
- PB-scale proven, powerful SQL ecosystem (Spark SQL)
- **Downside:** JVM-based, Python UDFs serialized across JVM↔Python boundary (slow), weak GPU support

### Ray Data
- Berkeley RISELab (2017), originally for reinforcement learning
- Python-native (no JVM overhead), native GPU scheduling
- Seamless PyTorch/HuggingFace integration
- **Downside:** younger, less documentation, weak SQL support

### When to Pick Which
- GPU required (BERT scoring, embeddings) → **Ray**
- Heavy SQL/BI needs → **Spark**
- Existing Spark infrastructure → evaluate migration cost
- New project → depends on team background

**Reality: They coexist.** Spark handles ETL/data lake, Ray handles ML-intensive processing.

---

## 2.3 Data Formats

### Parquet — Columnar Storage (Session 1 Deep Dive)

### Why Parquet Exists
Imagine you have a table with 1 million rows and 10 columns. You want to compute the average of just one column.

### Row storage (CSV, JSONL, databases like MySQL):


Row 1: name, age, url, text, score, date, lang, tokens, id, dump
Row 2: name, age, url, text, score, date, lang, tokens, id, dump
Row 3: ...

To read score, you have to read every row — scanning through name, url, text (which might be 10KB of text per row) just to get a single number. You read 100GB to get 8MB of useful data.

### Columnar storage (Parquet):


Column "name":  [name1, name2, name3, ...]     → stored together on disk
Column "age":   [25, 31, 42, ...]               → stored together on disk
Column "score": [0.87, 0.92, 0.45, ...]         → stored together on disk
To read score, you jump directly to that column's chunk and read only it. 8MB read for 8MB of useful data. That's column pruning.

And because each column is the same data type, compression works much better — a column of integers compresses way tighter than a jumbled row of string + int + float + string.

**How it works:**
- Data from the same column stored physically contiguous on disk
- Same-type data compresses much better than mixed rows
- Reading specific columns = jump to that column's chunk, skip everything else

**File anatomy:**
```
A Parquet file is structured like this:


┌──────────────────────────┐
│       Row Group 1        │  ← chunk of rows (typically 128MB)
│  ┌─────┬─────┬─────┐    │
│  │Col A│Col B│Col C│    │  ← each column stored separately within the group
│  └─────┴─────┴─────┘    │
├──────────────────────────┤
│       Row Group 2        │
│  ┌─────┬─────┬─────┐    │
│  │Col A│Col B│Col C│    │
│  └─────┴─────┴─────┘    │
├──────────────────────────┤
│        Footer            │  ← metadata: schema, row counts, min/max per column
└──────────────────────────┘
Key things:

Row Groups — horizontal chunks (~128MB each). This is how Parquet parallelizes reads.
Column Chunks — within each row group, each column is stored contiguously. This is what makes column pruning possible.
Footer — at the end of the file. Contains schema, row counts, and min/max statistics per column (this enables predicate pushdown — skipping row groups where min > your filter value).
```

**Key optimizations:**
1. **Column pruning** — only read columns you need. At TB scale: 5 min query → 5 sec query
2. **Partition pruning** — organize by date/language, skip irrelevant folders at read time
3. **File sizing** — target 128MB–1GB per file. Never thousands of tiny files (metadata overhead, slow S3 ListObjects)
4. **Predicate pushdown** — footer has min/max stats per column, skip row groups that can't match your filter

### JSONL (JSON Lines)
- One JSON object per line
- Human readable — can `head`, `cat` to inspect
- Flexible schema, streaming line-by-line reads
- **3-5x larger** than Parquet, slow (must parse every line)
- **Use for:** SFT instruction data, small datasets needing manual inspection

### WebDataset
- NVIDIA design: packages related files (image + caption) into TAR archives
- Streaming reads without decompression
- Each TAR = independent data shard for distributed processing
- **Use for:** Image-text pairs (LAION), video datasets

### Format Comparison

| Feature | Parquet | JSONL | WebDataset |
|---------|---------|-------|-----------|
| Storage efficiency | High (columnar) | Low (text redundancy) | Medium |
| Read speed | Fast (vectorized) | Slow (line parsing) | Medium (sequential) |
| Human readable | No | Yes | No |
| Multimodal support | Weak | Weak | Strong (native) |
| Best for | Pre-training text | SFT instruction | Image-text, video |

### Compression Algorithms

| Algorithm | Ratio | Write Speed | Read Speed | Use Case |
|-----------|-------|-------------|------------|----------|
| **Snappy** | Medium | Fast | Fast | Default choice, hot data |
| **LZ4** | Lower | Very fast | Very fast | Read latency sensitive |
| **ZSTD** | High | Medium | Fast | Cold/archived storage |
| **Gzip** | High | Slow | Medium | High compatibility needs |

**Rule: hot data = Snappy/LZ4, cold data = ZSTD**

---

## 2.4 Data Version Control

### Why Version Data?
- **Reproducibility** — restore data state at any moment
- **Traceability** — track chain from raw input to final output
- **Collaboration safety** — multiple people modifying without conflicts
- **Rollback** — return to previous version when issues found

### DVC (Data Version Control)
- "Git for Data" — data in S3, Git tracks small `.dvc` metadata files
- `dvc init` → `dvc add` → `git commit` → `dvc push`
- Switch versions: `git checkout <tag>` → `dvc checkout`
- **Good for:** < 1TB, ML experiments, small teams, Git-familiar workflows

### LakeFS
- "Git for Data Lakes" — proxy layer in front of S3
- Provides branches, commits, merges on object storage
- **Zero-copy branching** — creating branch doesn't copy data, only metadata
- Fully S3 compatible (existing tools work via LakeFS gateway)
- **Good for:** TB+ scale, multiple teams, production data lakes

| Feature | DVC | LakeFS |
|---------|-----|--------|
| Philosophy | Git extension | Version layer over S3 |
| Granularity | File-level | Object-level (finer) |
| Branch overhead | Copies .dvc files | Zero-copy |
| Complexity | Low (CLI tool) | Medium (requires server) |
| Best for | ML experiments, < 1TB | Data lake, TB+ scale |

### Data Lineage Tracking
Version control = "what is the data". Lineage = "where did the data come from".
- Track: upstream sources, processing scripts + params, execution details
- Tools: OpenLineage (Spark), Airflow + Marquez, DataHub, Apache Atlas
- Simple approach: metadata JSON per output (version, timestamp, inputs, script, git commit, parameters)

---

## 2.5 Common Mistakes

### 1. Premature Over-Engineering
5 people, 500GB → build Spark + Iceberg + Airflow + LakeFS → 80% time on infra, 20% on data.
**Fix:** Start with single machine + Parquet + DVC. Scale when you hit 10TB.

### 2. Chasing New Technology
Abandon Spark for Ray without evaluating existing data assets and dependencies.
**Fix:** Evaluate full ecosystem compatibility before switching.

### 3. Aggressive Cost Optimization
ZSTD-22 everything + Glacier Deep Archive → 12 hours to thaw + 4 hours to decompress.
**Fix:** Hot data = S3 Standard + Snappy. Cold data (6+ months unused) = Glacier.

---

## Session 1 Hands-On: Parquet Deep Dive

**Notebook:** `notebooks/ch02_s1_parquet_deep_dive.ipynb`

Key findings:
- Format comparison: CSV/JSONL are 3-5x larger than Parquet for same data
- Compression: ZSTD gives best size, Snappy/LZ4 give best speed
- Column pruning: reading 2 columns from Parquet is dramatically faster than JSONL
- Small files problem: 100 small files slower and larger than 2 big files
- Partition pruning: filtering by partition column skips reading irrelevant data

---

## Session 2 Hands-On: MinIO (Local S3)

**Notebook:** `notebooks/ch02_s2_minio.ipynb`

### What is MinIO?
- Open-source object storage server, 100% S3-compatible
- Same API, same boto3 code, same bucket/key structure — just on your own machine
- Zero code changes when moving to production AWS S3
- Every tool in the LLM stack (Iceberg, Spark, Ray, DVC, LakeFS) talks to S3 — MinIO lets them all work locally

### How Object Storage Works
- **Flat namespace** — no real folders. `2024/english/doc1.parquet` is just a key string. The `/` is cosmetic.
- This is why S3 scales infinitely — no directory tree to maintain
- Three concepts:
  1. **Bucket** — top-level container (like a drive)
  2. **Key** — full "path" to an object (just a string, no hierarchy)
  3. **Object** — the actual data (file content + metadata)

### Connecting with boto3
```python
s3 = boto3.client('s3',
    endpoint_url='http://localhost:9000',  # Only difference from real S3
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
)
```
Remove `endpoint_url` + use real AWS creds = exact same code talks to real S3.

### Simulating Folders
- `list_objects_v2(Prefix='crawl-2024/')` — string matching, not folder browsing
- `list_objects_v2(Delimiter='/')` — groups keys by `/` to simulate top-level folders
- API returns `CommonPrefixes` — not actual directories

### Pipeline Pattern: Three Buckets
```
raw-data/          →  processed-data/       →  training-data/
(crawled web pages)   (cleaned + filtered)     (text + token_count only)
(all formats)         (Parquet, Snappy)         (Parquet, ZSTD compressed)
(biggest)             (smaller)                 (smallest)
```
- Each bucket = a pipeline stage
- Re-run processing without re-downloading raw data
- Version each stage independently
- Set different storage tiers (raw in Glacier, training in Standard)

### Object Metadata for Lineage
- Every S3 object can carry custom metadata key-value pairs
- Attach: source, pipeline version, filters applied, row counts, timestamps
- Simplest form of data lineage — no extra database needed
- `s3.put_object(..., Metadata={'source': 'fineweb', 'filters': 'word_count>=50'})`
- Read back with `s3.head_object()` — inspect any object and know how it was produced

### ETag
- Hash of object content — if two objects have same ETag, they're identical
- Used for: cache validation, deduplication checks, multipart upload verification

### Key Takeaways
- MinIO = local S3. Learn once, use everywhere.
- Object storage is flat — "folders" are just prefix string matching
- Separate buckets per pipeline stage (raw → processed → training)
- Attach metadata to objects for lightweight lineage tracking

---

## Session 2.5: Enterprise Data Lake Architectures (Concept Deep Dive)

Understanding the evolution is crucial — Iceberg doesn't make sense unless you understand what problems it solves.

### The Evolution: How We Got Here

#### Era 1: Data Warehouses (1990s–2010s)
```
Application DBs  →  ETL  →  Data Warehouse  →  BI Reports
(MySQL, Oracle)              (Teradata, Oracle, Redshift)
```
- One big, expensive, structured database optimized for analytics
- **Schema-on-write** — define columns and types *before* loading data
- Products: Teradata ($1M+/year), Oracle Exadata, IBM Netezza; later Redshift, BigQuery, Snowflake
- **Problems:** Expensive, rigid schemas, storage+compute coupled, structured data only (no images, logs, JSON)

#### Era 2: Data Lakes (2010–2018)
Hadoop/HDFS arrived. Idea: dump *everything* raw into cheap storage, figure out the schema later.
```
Everything  →  Dump into HDFS/S3  →  Process with Spark/Hive  →  Analytics
(structured,                         (schema-on-read)
 unstructured,
 logs, images)
```
- Products: Hadoop clusters (Cloudera, Hortonworks), later S3 + Spark (EMR, Databricks), AWS Lake Formation
- **The "Data Swamp" problem:**
  - No ACID → job crashes mid-write = half-written corrupt files
  - No schema enforcement → garbage in, garbage out
  - No updates/deletes → can't comply with GDPR ("delete user X" is impossible on immutable Parquet)
  - No consistency → reader sees partial data while writer is still going
  - No time travel → accidentally overwrote yesterday's data? Gone forever
- **Real horror story pattern:** Company builds lake → 50 teams dump data in random formats → no one knows what's in there → data scientists spend 80% of time finding/cleaning → leadership says "data lake isn't working"

#### Era 3: Data Lakehouse (2018–present)
**Combine the best of both:** cheap flexible storage of data lake + reliability and performance of data warehouse.
```
┌─────────────────────────────────────────────────────────┐
│                    Query Engines                         │
│         Spark    Trino    Flink    DuckDB    Presto      │
├─────────────────────────────────────────────────────────┤
│              Table Format (Metadata Layer)                │
│           Iceberg  /  Delta Lake  /  Hudi                │
│    ┌──────────────────────────────────────────────┐      │
│    │  ACID transactions, schema evolution,        │      │
│    │  time travel, partition pruning,             │      │
│    │  row-level updates/deletes                   │      │
│    └──────────────────────────────────────────────┘      │
├─────────────────────────────────────────────────────────┤
│              File Format (Data Layer)                     │
│              Parquet  /  ORC  /  Avro                     │
├─────────────────────────────────────────────────────────┤
│              Object Storage (Storage Layer)               │
│              S3  /  GCS  /  Azure Blob  /  MinIO         │
└─────────────────────────────────────────────────────────┘
```
**Key innovation:** Table formats (Iceberg/Delta/Hudi) sit *between* storage and compute. They don't store data — they add a **metadata layer** tracking which files belong to which table, version, schema, etc.

---

### The Three Table Formats — Who Uses What

#### Delta Lake (Databricks)
- Uses a **transaction log** (`_delta_log/` folder) — JSON files tracking every change
- Tightly integrated with Spark and Databricks platform
- **Real users:** Apple, Comcast, Conde Nast, Shell
- **Limitation:** Best experience on Databricks. Other engines have limited support.

#### Apache Hudi (Uber → Apache)
- Built to solve Uber's specific problem: **streaming upserts at massive scale**
- Specializes in **Change Data Capture (CDC)** — database changes streamed to the lake
- **Real users:** Uber, Amazon (internally), ByteDance, Robinhood
- **Limitation:** More complex, steeper learning curve, primarily Spark-focused

#### Apache Iceberg (Netflix → Apple → Apache)
- Design philosophy: **engine-agnostic** — works equally with Spark, Trino, Flink, DuckDB, Snowflake
- **Hidden partitioning** — no need to know partition details to query efficiently
- **Real users:** Netflix, Apple, LinkedIn, Airbnb, Expedia, Adobe, Stripe
- **Why it's winning:** Snowflake, AWS, Google, Dremio all adopted Iceberg. Becoming the industry standard.

#### Comparison Table

| | Delta Lake | Hudi | Iceberg |
|--|-----------|------|---------|
| Created by | Databricks | Uber | Netflix |
| Sweet spot | Databricks ecosystem | Streaming upserts/CDC | Multi-engine, vendor neutral |
| Engine support | Spark (best), others partial | Spark (best), Flink | Spark, Trino, Flink, DuckDB, Snowflake, Dremio |
| ACID | Yes | Yes | Yes |
| Time travel | Yes | Yes (limited) | Yes (snapshot-based, very clean) |
| Schema evolution | Yes | Yes | Yes (best — add, drop, rename, reorder) |
| Row-level deletes | Yes (GDPR) | Yes (native) | Yes (merge-on-read or copy-on-write) |
| Adoption trend | Databricks customers | Niche (streaming-heavy) | Broadest, industry converging here |

---

### AWS Lake Formation — Where It Fits

**AWS Lake Formation is NOT a table format** — it's a managed service that helps you:
1. Set up a data lake on S3 — crawl data sources, define schemas in Glue Catalog
2. Access control — fine-grained permissions (column-level, row-level)
3. Data catalog — Glue Data Catalog registers all your tables
4. ETL — Glue jobs to transform data

```
┌──────────────────────────────────────────┐
│           AWS Lake Formation              │
│  ┌────────────┐  ┌──────────────────┐    │
│  │ Glue Catalog│  │ Access Control   │    │
│  │ (metadata)  │  │ (permissions)    │    │
│  └─────┬──────┘  └──────────────────┘    │
│        │                                  │
│  ┌─────▼──────────────────────────────┐  │
│  │  Iceberg / Delta / Hudi tables     │  │  ← Table format lives here
│  │  (or plain Parquet/CSV)            │  │
│  └─────┬──────────────────────────────┘  │
│        │                                  │
│  ┌─────▼──────┐                          │
│  │    S3      │  ← Storage               │
│  └────────────┘                          │
└──────────────────────────────────────────┘
         │
    Query with: Athena, Redshift Spectrum, EMR Spark, Glue
```
**Key insight:** Lake Formation is the *management layer*. Iceberg/Delta/Hudi are the *table formats*. They work together. **AWS now recommends Iceberg as the default table format for Lake Formation.**

---

### Real Enterprise Architecture Examples

#### Netflix (Iceberg Creator)
```
Data Sources → Kafka → Spark ETL → Iceberg tables on S3 → Trino/Spark for analytics
                                                         → ML pipelines for recommendations
```
- 100+ PB in their data lake
- Created Iceberg because existing formats couldn't handle their scale
- Time travel lets them debug recommendation model training data

#### Uber (Hudi Creator)
```
Rider/Driver DBs → CDC via Debezium → Hudi tables on HDFS/S3 → Spark/Presto for analytics
                                                               → ML for ETA prediction, pricing
```
- Billions of upserts per day (trip updates, location changes)
- Hudi's streaming upsert was built for exactly this use case

#### Typical Modern Enterprise (2024+)
```
┌─────────────────────────────────────────────────────────┐
│  Ingestion          │  Storage           │  Consumption  │
│                     │                    │               │
│  Kafka/Kinesis  ──► │  S3               │  ◄── Spark    │
│  CDC (Debezium) ──► │  + Iceberg tables │  ◄── Trino    │
│  API pulls     ──► │  + Glue Catalog   │  ◄── dbt      │
│  File uploads  ──► │                    │  ◄── ML/AI    │
│                     │                    │  ◄── BI tools │
└─────────────────────────────────────────────────────────┘
```

---

### Why This Matters for LLM Data Engineering

1. **ACID transactions** — Cleaning pipeline crashes mid-write. Without Iceberg: corrupt partial files. With Iceberg: write fully succeeds or fully rolls back.
2. **Time travel** — Trained a model 3 months ago that performed great. What data was it trained on? `SELECT * FROM training_data VERSION AS OF '2025-11-15'`
3. **Schema evolution** — Need to add `quality_score` column to 50TB dataset. Without Iceberg: rewrite all 50TB. With Iceberg: just update metadata.
4. **Row-level deletes** — GDPR deletion request. Remove specific content from 50TB of training data without rewriting everything.
5. **Partition evolution** — Partitioned by `language` but now want `date` too. Iceberg changes partitioning without rewriting data.

---

## Session 3: Apache Iceberg — Deep Dive Internals

**Notebook:** `notebooks/ch02_s3_iceberg.ipynb`

### What Iceberg Actually Does Under the Hood

Iceberg **never modifies data files**. Instead, it keeps a **metadata tree** that tracks which Parquet files are "current":

```
Iceberg Table
│
├── metadata/
│   ├── v1.metadata.json    ← "table has columns: text, url, score"
│   ├── v2.metadata.json    ← "added column: quality_score"
│   ├── snap-001.avro       ← "snapshot 1: data files = [file1.parquet, file2.parquet]"
│   └── snap-002.avro       ← "snapshot 2: data files = [file1.parquet, file3.parquet]"
│                              (file2 was replaced by file3 — that's an "update")
│
└── data/
    ├── file1.parquet       ← never modified
    ├── file2.parquet       ← still exists on disk, but snapshot 2 doesn't reference it
    └── file3.parquet       ← new file with updated rows
```

**Key insight:**
- **Delete** = new snapshot that stops referencing old files
- **Update** = new snapshot pointing to new files with changed rows
- **Time travel** = just read an older snapshot
- **ACID** = snapshot is written atomically — readers see either old or new, never partial

### The Metadata Tree — Three Levels

```
Level 1: metadata.json          ← Table definition (ONE current file)
    │
    ▼
Level 2: snap-xxxx.avro         ← Manifest List (ONE per snapshot)
    │
    ▼
Level 3: manifest-xxxx.avro     ← Manifest Files (MANY per snapshot)
    │
    ▼
         data/xxxx.parquet      ← Actual Parquet data files
```

#### Level 1: `metadata.json` — The Entry Point

Single JSON file. Root of everything. Contains:

```json
{
  "format-version": 2,
  "table-uuid": "abc-123-...",
  "location": "s3://iceberg-warehouse/llm_data/documents",
  "current-snapshot-id": 7283946102,
  "schemas": [
    { "schema-id": 0, "fields": [{"id": 1, "name": "text", "type": "string"}, ...] },
    { "schema-id": 1, "fields": [..., {"id": 9, "name": "quality_score", "type": "float"}] }
  ],
  "current-schema-id": 1,
  "snapshots": [
    { "snapshot-id": 5839201746, "manifest-list": "s3://...snap-5839201746.avro" },
    { "snapshot-id": 7283946102, "manifest-list": "s3://...snap-7283946102.avro" }
  ],
  "snapshot-log": [
    { "timestamp-ms": 1708100000000, "snapshot-id": 5839201746 },
    { "timestamp-ms": 1708200000000, "snapshot-id": 7283946102 }
  ]
}
```

**Key point:** This file is **replaced atomically** on every commit. Not appended — a whole new `v2.metadata.json` replaces `v1.metadata.json`. The catalog (SQLite/Glue) stores a pointer to the **current** metadata.json. This is how ACID works — the catalog pointer either points to the old file or the new one. Never in between.

#### Level 2: Manifest List (`snap-xxxx.avro`) — What's In This Snapshot?

Each snapshot has exactly ONE manifest list. It lists **manifest files** and summary stats:

```
Manifest List for Snapshot 7283946102:
┌─────────────────────────────────┬────────────┬───────────────┬────────────┐
│ manifest_path                   │ added_rows │ deleted_rows  │ partitions │
├─────────────────────────────────┼────────────┼───────────────┼────────────┤
│ s3://.../manifest-001.avro      │ 1000       │ 0             │ [en]       │
│ s3://.../manifest-002.avro      │ 500        │ 0             │ [en]       │
│ s3://.../manifest-003.avro      │ 0          │ 3             │ [en]       │
└─────────────────────────────────┴────────────┴───────────────┴────────────┘
```

**Why this level exists:** If your table has 100,000 data files across 50 partitions, and you query `WHERE language = 'de'`, Iceberg reads the manifest list first, sees which manifests cover the `de` partition, and **skips all other manifests entirely**. It never even looks at files for other partitions.

#### Level 3: Manifest Files (`manifest-xxxx.avro`) — Which Data Files, Exactly?

Each manifest file lists actual Parquet files and their **per-column statistics**:

```
Manifest manifest-001.avro:
┌──────────────────────────┬──────────┬───────────┬──────────────────────────────┐
│ file_path                │ row_count│ file_size │ column_stats                 │
├──────────────────────────┼──────────┼───────────┼──────────────────────────────┤
│ s3://.../data/0001.pqt   │ 500      │ 2.3 MB    │ word_count: min=12, max=4521 │
│                          │          │           │ language_score: min=0.65, max=0.99 │
│ s3://.../data/0002.pqt   │ 500      │ 2.1 MB    │ word_count: min=8, max=3892  │
│                          │          │           │ language_score: min=0.71, max=0.98 │
└──────────────────────────┴──────────┴───────────┴──────────────────────────────┘
```

**This is where predicate pushdown happens.** Query: `WHERE word_count > 5000`. Iceberg reads the manifest, sees `0001.pqt` has `max=4521` — that file **can't possibly have matching rows**. Skipped entirely. Never downloaded from S3.

---

### How a Write Actually Happens — Step by Step

`table.append(new_data)`:

```
1. Write Parquet file(s) to S3
   s3://warehouse/data/0003.parquet  ← new file with your data

2. Create new manifest file
   s3://warehouse/metadata/manifest-004.avro
   ← lists 0003.parquet with its row count and column stats

3. Create new manifest list
   s3://warehouse/metadata/snap-9999.avro
   ← includes manifest-001, manifest-002, manifest-004
     (reuses old manifests! doesn't rewrite them)

4. Create new metadata.json
   s3://warehouse/metadata/v3.metadata.json
   ← current-snapshot-id: 9999
   ← snapshots: [..., {id: 9999, manifest-list: snap-9999.avro}]

5. ATOMIC COMMIT: Update catalog pointer
   catalog says: "llm_data.documents → v3.metadata.json"
   ← This is ONE atomic operation (SQL UPDATE in SQLite, or S3 conditional PUT)
   ← Until this succeeds, readers still see v2.metadata.json
```

**If step 1, 2, or 3 crashes:** Step 5 never happens. The catalog still points to old metadata. Readers see old version. Orphaned files from steps 1-3 are garbage to be cleaned up later.

**If step 5 crashes:** Same — the pointer wasn't updated. Old version is still current.

**This is ACID:** The commit is a single atomic pointer swap. Everything before it is just writing files that nobody references yet.

---

### How a Delete Actually Happens

`table.delete(filter="url == 'https://example.com'")` — two strategies:

#### Copy-on-Write (default):
```
1. Read data files that MIGHT contain matching rows (using manifest stats)
2. Filter out the matching rows
3. Write NEW Parquet files with the remaining rows
4. Create new manifest pointing to new files (dropping old ones)
5. Atomic commit

Old files: [file1.parquet (1000 rows), file2.parquet (500 rows)]
New files: [file1_v2.parquet (997 rows)]  ← 3 rows deleted, file2 untouched
```
- Pros: Reads are fast (no merge needed)
- Cons: Rewrites entire files even for 1-row delete

#### Merge-on-Read (Iceberg v2):
```
1. Write a small "delete file" listing WHICH rows to skip
   delete-file-001.parquet: [{file: "file1.parquet", positions: [47, 203, 891]}]
2. Atomic commit referencing both data files AND delete files

On read:
   - Read file1.parquet
   - Read delete-file-001.parquet
   - Merge: skip rows 47, 203, 891
```
- Pros: Delete is fast (tiny file, no rewrite)
- Cons: Reads slightly slower (must merge). Periodic compaction needed.

---

### How Time Travel Works

Trivially simple once you understand the tree:

```
catalog → v3.metadata.json → current-snapshot-id: 9999

Time travel to snapshot 5839201746:
  Just read v3.metadata.json → snapshots → find 5839201746 → get its manifest-list
  → read those manifests → read those data files

No different from a current read — just following a different snapshot pointer.
```

Old snapshots reference old manifest lists, which reference old data files. Those files **still exist on S3**. They're only physically deleted when you run `expire_snapshots` + `remove_orphan_files`.

---

### How Schema Evolution Works

```
v1.metadata.json:
  schemas: [
    {id: 0, fields: [{id: 1, name: "text", type: "string"}, {id: 2, name: "url", type: "string"}]}
  ]
  current-schema-id: 0

You run: table.update_schema().add_column("quality_score", FloatType())

v2.metadata.json:
  schemas: [
    {id: 0, fields: [{id: 1, name: "text"}, {id: 2, name: "url"}]},
    {id: 1, fields: [{id: 1, name: "text"}, {id: 2, name: "url"}, {id: 9, name: "quality_score"}]}
  ]
  current-schema-id: 1
```

**No data files are touched.** When reading old Parquet files that don't have `quality_score`, Iceberg just returns `null` for that column. The mapping is by **field ID** (not name), which is why renames work — field 2 used to be called "dump", now called "crawl_batch", but it's still field ID 2 in the Parquet files.

---

### Concurrent Writes — How Two Writers Don't Corrupt Each Other

What if two Spark jobs write to the same table simultaneously?

```
Writer A: reads v3.metadata.json, prepares data, writes v4-A.metadata.json
Writer B: reads v3.metadata.json, prepares data, writes v4-B.metadata.json

Both try to update the catalog pointer:
  Writer A: "set pointer to v4-A.metadata.json" → SUCCESS (atomic compare-and-swap)
  Writer B: "set pointer to v4-B.metadata.json" → FAILS (pointer already changed!)

Writer B retries:
  Re-reads catalog → now sees v4-A.metadata.json
  Checks: "are my changes compatible?" (no overlapping files)
  If yes: creates v5.metadata.json incorporating both A's and B's changes → SUCCESS
  If no: CONFLICT → abort, user must resolve
```

This is called **optimistic concurrency control** — no locks, just atomic pointer swaps with retry on conflict. Same principle as Git (you pull, rebase, push).

---

### Session 3 Hands-On Summary

| Feature | What We Did | How It Works Internally |
|---------|------------|------------------------|
| **Catalog** | SQLite catalog → MinIO | Stores pointer to current metadata.json |
| **ACID writes** | Appended data atomically | Write files → new manifest → new snapshot → atomic pointer swap |
| **Snapshots** | Saw snapshot history | Each snapshot = manifest list pointing to data files |
| **Time travel** | Read old snapshot | Follow old snapshot's manifest list instead of current |
| **Schema evolution** | Added column, renamed column | New schema in metadata.json, field IDs map to Parquet columns |
| **Row-level delete** | Deleted by URL filter | Copy-on-write: rewrite affected files. Merge-on-read: write delete file |
| **Metadata inspection** | Browsed three-level tree in MinIO | metadata.json → manifest list → manifest files → data parquet |
| **Concurrent writes** | Conceptual | Optimistic concurrency: atomic pointer swap with retry on conflict |

### Local vs Production Mapping

| Local | Production (enterprise) |
|------------|------------------------|
| SQLite catalog | AWS Glue Catalog / Hive Metastore |
| MinIO | AWS S3 / GCS / Azure Blob |
| PyIceberg | Spark + Iceberg / Trino + Iceberg |
| Manual snapshots | Automated via Spark jobs / Airflow DAGs |

**The concepts are identical. Only the scale changes.**

---

## Key Vocabulary

| Term | Meaning |
|------|---------|
| **Object storage** | Flat key-value storage (S3/MinIO), no true directories |
| **Columnar storage** | Storing same column's data contiguously (Parquet) |
| **Row group** | Horizontal chunk of rows in Parquet (~128MB) |
| **Column pruning** | Reading only needed columns, skipping the rest |
| **Partition pruning** | Organizing data into folders by column value, reading only relevant folders |
| **Predicate pushdown** | Using footer min/max stats to skip irrelevant row groups |
| **ACID** | Atomicity, Consistency, Isolation, Durability — transaction guarantees |
| **Time travel** | Querying data as it existed at a past point in time |
| **Data lineage** | Recording where data came from and how it was processed |
| **Zero-copy branching** | Creating a branch without duplicating data (LakeFS) |
| **Bucket** | Top-level container in object storage (like a drive) |
| **Key** | Full "path" to an S3 object — just a string, no real hierarchy |
| **ETag** | Hash of object content, used for cache validation and dedup |
| **Prefix** | String filter on keys to simulate "folder browsing" in S3 |
| **Delimiter** | Used with Prefix to group keys into "virtual folders" |
| **Data warehouse** | Structured, schema-on-write analytics database (Redshift, Snowflake, BigQuery) |
| **Data lake** | Raw dump of all data into cheap storage (S3/HDFS), schema-on-read |
| **Data swamp** | A data lake gone wrong — no governance, no one knows what's in it |
| **Data lakehouse** | Combines lake flexibility + warehouse reliability via table formats |
| **Table format** | Metadata layer (Iceberg/Delta/Hudi) adding ACID, time travel, schema evolution to files on object storage |
| **CDC** | Change Data Capture — streaming database changes to a data lake |
| **Schema-on-write** | Define schema before loading data (warehouse style) |
| **Schema-on-read** | Load data first, interpret schema when reading (lake style) |
| **Schema evolution** | Adding/dropping/renaming columns without rewriting data |
| **Hidden partitioning** | Iceberg feature — engine auto-optimizes partition pruning without user specifying partition columns in queries |
| **Transaction log** | File tracking every change to a table (Delta Lake's `_delta_log/`, Iceberg's metadata tree) |
| **Lake Formation** | AWS managed service for setting up data lakes — governance, access control, Glue Catalog (not a table format) |
| **Snapshot** | Point-in-time version of an Iceberg table — records which data files make up the table at that moment |
| **Manifest list** | Avro file listing all manifest files for a snapshot + summary stats (Level 2 of metadata tree) |
| **Manifest file** | Avro file listing actual Parquet data files + per-file/per-column min/max stats (Level 3 of metadata tree) |
| **Catalog** | Entry point for Iceberg — stores pointer to current metadata.json (SQLite, Glue, Hive Metastore, REST) |
| **Copy-on-write** | Delete strategy: rewrite affected Parquet files without deleted rows. Fast reads, slow deletes. |
| **Merge-on-read** | Delete strategy: write small delete file listing row positions to skip. Fast deletes, slightly slower reads. |
| **Optimistic concurrency** | Concurrent write strategy: no locks, atomic pointer swap, retry on conflict (like Git) |
| **Field ID** | Stable integer identifier for a column in Iceberg — survives renames, maps to Parquet columns |
| **expire_snapshots** | Iceberg maintenance: remove old snapshot metadata so time travel can't reach them |
| **remove_orphan_files** | Iceberg maintenance: physically delete data files no longer referenced by any snapshot |
