"""
AWS Glue ETL job script to bulk load data into SQL Server using bcp via subprocess.

Why not just use Spark's JDBC write? Because it does row-by-row INSERT under
the hood (or batched INSERT VALUES at best). bcp uses SQL Server's native
bulk copy protocol the same thing BULK INSERT uses. Way faster.

The catch is that Glue is a managed service so you can't yum install anything.
We get around that by pre-packaging bcp + its shared libs into a zip, stashing
it on S3, and unpacking to /tmp at job start.

Job parameters:
    --BCP_S3_BUCKET     bucket where bcp_package.zip lives
    --BCP_S3_KEY        key path (e.g. glue-assets/bcp_package.zip)
    --SQL_SERVER        sql server host
    --SQL_DATABASE      target database
    --SQL_USER          login
    --SQL_PASSWORD      password (use Secrets Manager for real, see note below)
    --SQL_PORT          port (usually 1433)
    --SQL_TABLE         target table
"""

import sys
import os
import subprocess
import tempfile
import zipfile
from datetime import datetime

import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# --- parse job params ---

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "BCP_S3_BUCKET",
    "BCP_S3_KEY",
    "SQL_SERVER",
    "SQL_DATABASE",
    "SQL_USER",
    "SQL_PASSWORD",
    "SQL_PORT",
    "SQL_TABLE",
])


sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

logger = glue_ctx.get_logger()


# --- bcp setup ---
# download the zip from S3, unpack to /tmp/bcp, set up env vars

BCP_DIR = "/tmp/bcp"
BCP_BIN = os.path.join(BCP_DIR, "bin", "bcp")


def _bcp_env():
    """env dict that bcp needs — lib path, odbc config, binary path."""
    env = os.environ.copy()
    env["LD_LIBRARY_PATH"] = f"{BCP_DIR}/lib:" + env.get("LD_LIBRARY_PATH", "")
    env["ODBCSYSINI"] = BCP_DIR  # points at the dir with odbcinst.ini
    env["PATH"] = f"{BCP_DIR}/bin:" + env.get("PATH", "")
    return env


def install_bcp():
    """download bcp_package.zip from S3 and unpack it."""
    if os.path.isfile(BCP_BIN):
        logger.info("bcp already unpacked, skipping")
        return

    logger.info(f"downloading bcp from s3://{args['BCP_S3_BUCKET']}/{args['BCP_S3_KEY']}")
    s3 = boto3.client("s3")
    zip_path = "/tmp/bcp_package.zip"
    s3.download_file(args["BCP_S3_BUCKET"], args["BCP_S3_KEY"], zip_path)

    os.makedirs(BCP_DIR, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(BCP_DIR)
    os.chmod(BCP_BIN, 0o755)

    # quick check the installation
    result = subprocess.run([BCP_BIN, "-v"], capture_output=True, text=True, env=_bcp_env())
    logger.info(f"bcp version: {result.stdout.strip()}")


install_bcp()


# --- bulk insert function ---

def bcp_bulk_insert(csv_path, table_name):
    """call bcp to load a CSV into SQL Server."""
    cmd = [
        BCP_BIN,
        table_name,
        "in",
        csv_path,
        "-c",                # character mode
        "-t", ",",           # comma separated
        "-F", "2",           # skip header row
        "-b", "10000",       # batch size
        "-S", f"{args['SQL_SERVER']},{args['SQL_PORT']}",
        "-d", args["SQL_DATABASE"],
        "-U", args["SQL_USER"],
        "-P", args["SQL_PASSWORD"],
        "-u",                # trust server certificate (bcp 18 defaults to encrypted)
    ]

    logger.info(f"bcp: loading {csv_path} into {table_name}")
    result = subprocess.run(cmd, capture_output=True, text=True, env=_bcp_env(), timeout=600)

    if result.returncode != 0:
        logger.error(f"bcp failed: {result.stderr}")
        raise RuntimeError(f"bcp failed (rc={result.returncode}): {result.stderr}")

    logger.info(f"bcp done: {result.stdout.strip()}")
    return result.stdout


# --- read source data ---
#
# swap this out for your actual source. could be:
#   spark.read.parquet("s3://bucket/data/")
#   glue_ctx.create_dynamic_frame.from_catalog(database="...", table_name="...")
#   spark.read.jdbc(...)
#
# using sample data here to keep it self-contained

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType
import random

schema = StructType([
    StructField("id", IntegerType()),
    StructField("value", DoubleType()),
    StructField("amount", IntegerType()),
    StructField("category", StringType()),
    StructField("description", StringType()),
    StructField("created_at", TimestampType()),
])

# 10K rows as a demo — replace with your real data source
random.seed(42)
rows = [
    (i, random.gauss(0, 1), random.randint(1, 9999),
     f"cat_{random.randint(0,19)}",
     f"item_{i}_" + "x" * random.randint(10, 80),
     datetime(2024, 1, 1))
    for i in range(1, 10001)
]
source_df = spark.createDataFrame(rows, schema)
logger.info(f"source: {source_df.count()} rows")


# --- write to CSV and bulk load ---
#
# strategy: coalesce into a few partitions, write each as CSV, bcp each file.
# bcp is single-threaded so there's no point in too many partitions, but
# splitting keeps memory usage reasonable for large datasets.

NUM_PARTITIONS = 4
source_df = source_df.coalesce(NUM_PARTITIONS)

csv_dir = tempfile.mkdtemp(prefix="bcp_staging_")
source_df.write.option("header", "true").csv(csv_dir, mode="overwrite")

# find the actual part-*.csv files spark wrote
csv_files = sorted([
    os.path.join(csv_dir, f)
    for f in os.listdir(csv_dir)
    if f.startswith("part-") and f.endswith(".csv")
])

logger.info(f"wrote {len(csv_files)} partition files")

for i, path in enumerate(csv_files):
    logger.info(f"loading partition {i+1}/{len(csv_files)}")
    bcp_bulk_insert(path, args["SQL_TABLE"])

logger.info("bulk load complete")


# --- bonus: bcp out (extract FROM sql server) ---
#
# bcp queryout lets you pull data out of SQL Server into a flat file.
# handy for migrating data to S3 or feeding downstream systems.

def bcp_queryout(query, output_path):
    """extract data FROM SQL Server using bcp queryout."""
    cmd = [
        BCP_BIN,
        query,
        "queryout",
        output_path,
        "-c",
        "-t", ",",
        "-S", f"{args['SQL_SERVER']},{args['SQL_PORT']}",
        "-d", args["SQL_DATABASE"],
        "-U", args["SQL_USER"],
        "-P", args["SQL_PASSWORD"],
        "-u",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env=_bcp_env(), timeout=600)
    if result.returncode != 0:
        raise RuntimeError(f"bcp queryout failed: {result.stderr}")
    logger.info(f"queryout done: {result.stdout.strip()}")
    return output_path


# example: extract what we just loaded and push to S3
# extract_path = "/tmp/bcp_extract.csv"
# bcp_queryout(f"SELECT TOP 1000 * FROM {args['SQL_TABLE']}", extract_path)
# boto3.client("s3").upload_file(extract_path, args["BCP_S3_BUCKET"],
#     f"extracts/{args['SQL_TABLE']}_{datetime.now():%Y%m%d_%H%M%S}.csv")


# --- NOTE for passwords use secrets manager in production ---

job.commit()
