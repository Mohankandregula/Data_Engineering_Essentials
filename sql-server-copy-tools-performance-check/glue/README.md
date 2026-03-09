# AWS Glue + bcp: Bulk Loading SQL Server from Spark

## Why this exists

Spark's JDBC write to SQL Server is slow — it generates row-by-row INSERT statements under the hood. bcp uses SQL Server's native bulk copy protocol and is dramatically faster. But Glue is a managed service so you can't install packages on the workers.

The workaround: package bcp and its shared libraries into a zip on the same OS that Glue runs, upload to S3, download and unpack to `/tmp` at job start.

## The glibc problem

This is the thing that wasted most of my time. Glue workers run a specific Linux version, and the bcp binary must be compiled against the same glibc or it'll crash:

| Glue version | OS | glibc | Build base image |
|---|---|---|---|
| 5.0 | Amazon Linux 2023 | 2.34 | `amazonlinux:2023` |
| 4.0 | Amazon Linux 2 | 2.26 | `amazonlinux:2` |

If you build on CloudShell (which is AL2023) and try to run on Glue 4.0 (AL2), you get `version GLIBC_2.28 not found`. The reverse also fails. **Match the OS exactly.**

The Dockerfile in this directory targets Glue 5.0 (AL2023). For Glue 4.0, change the `FROM` to `amazonlinux:2`, use `yum` instead of `dnf`, and use the `rhel/7` repo.

## Architecture

```
S3 (source data)          S3 (bcp_package.zip)
       \                        |
        \                  [boto3 download]
         \                 [unzip to /tmp/bcp/]
          \                     |
      AWS Glue PySpark ──── subprocess.run(["bcp", ...])
                                |
                          SQL Server
                     (RDS / on-prem / Azure SQL)
```

## Quick start (local testing)

You need Docker running.

```bash
cd glue/

# 1. build the bcp package
docker build -f Dockerfile.bcp_builder -t bcp-builder .
docker run --rm -v $(pwd)/output:/output bcp-builder

# 2. verify it works against the Glue 5.0 image
# (the build script does this automatically)
chmod +x build_and_upload.sh
./build_and_upload.sh
```

The script will:
- Build bcp_package.zip on AL2023
- Pull the Glue 5.0 Docker image
- Unpack the zip inside the Glue container and run `bcp -v` as a smoke test
- Show the glibc versions for both images so you can confirm they match

## Deploying to AWS

```bash
# upload the zip and job script to S3
aws s3 cp output/bcp_package.zip s3://your-bucket/glue-assets/bcp_package.zip
aws s3 cp glue_bcp_job.py s3://your-bucket/glue-scripts/glue_bcp_job.py

# create the Glue job
aws glue create-job \
  --name bcp-bulk-load \
  --role your-glue-role-arn \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://your-bucket/glue-scripts/glue_bcp_job.py",
    "PythonVersion": "3"
  }' \
  --glue-version "5.0" \
  --number-of-workers 2 \
  --worker-type G.1X \
  --default-arguments '{
    "--BCP_S3_BUCKET": "your-bucket",
    "--BCP_S3_KEY": "glue-assets/bcp_package.zip",
    "--SQL_SERVER": "your-sql-server-host",
    "--SQL_DATABASE": "benchmark_db",
    "--SQL_USER": "sa",
    "--SQL_PASSWORD": "your-password",
    "--SQL_PORT": "1433",
    "--SQL_TABLE": "bench_bulk"
  }'

# run it
aws glue start-job-run --job-name bcp-bulk-load
```

## Networking

Glue workers need TCP access to SQL Server on port 1433. Depending on where your SQL Server lives:

- **RDS in same VPC**: add Glue's security group to the RDS inbound rules
- **On-prem**: needs VPN or Direct Connect
- **Azure SQL**: open the firewall for your Glue NAT gateway's public IP

## Security

Don't put `SQL_PASSWORD` in plain job parameters for production. Use Secrets Manager:

```python
import json
secret = boto3.client("secretsmanager").get_secret_value(SecretId="my-sql-creds")
creds = json.loads(secret["SecretString"])
```

There's a comment in `glue_bcp_job.py` showing where to plug this in.

## Limitations

- bcp is single-threaded per invocation — can't parallelize a single file load
- `/tmp` on Glue workers has limited space (~64GB on G.1X). If your data is huge, partition it and load chunk by chunk (the job script already does this)
- bcp doesn't do TRUNCATE — if you need to clear the table first, run a TRUNCATE via pyodbc or JDBC before the bcp calls
- The ODBC config in `odbcinst.ini` hardcodes paths to `/tmp/bcp/lib/` — if you unpack somewhere else, update it
