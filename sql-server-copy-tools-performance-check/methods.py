import os
import shutil
import subprocess
import pyodbc
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table

import config
from db_setup import truncate_table


# 1. pyodbc executemany — row by row, the slow baseline
def insert_pyodbc_executemany(df):
    truncate_table()
    conn = pyodbc.connect(config.PYODBC_CONN_STR)
    cur = conn.cursor()
    rows = list(df.itertuples(index=False, name=None))
    cur.executemany(
        "INSERT INTO bench_bulk (id, value, amount, category, description, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    cur.close()
    conn.close()


# 2. pyodbc fast_executemany — sends whole rowset in one shot
def insert_pyodbc_fast_executemany(df):
    truncate_table()
    conn = pyodbc.connect(config.PYODBC_CONN_STR)
    cur = conn.cursor()
    cur.fast_executemany = True
    rows = list(df.itertuples(index=False, name=None))
    cur.executemany(
        "INSERT INTO bench_bulk (id, value, amount, category, description, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    cur.close()
    conn.close()


# 3. pymssql executemany — TDS protocol, no ODBC layer
def insert_pymssql_executemany(df):
    import pymssql

    truncate_table()
    conn = pymssql.connect(**config.PYMSSQL_ARGS)
    cur = conn.cursor()
    rows = list(df.itertuples(index=False, name=None))
    cur.executemany(
        "INSERT INTO bench_bulk (id, value, amount, category, description, created_at) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        rows,
    )
    conn.commit()
    cur.close()
    conn.close()


# 4. mssql-python — Microsoft's DDBC driver, bypasses ODBC
def insert_mssql_python_executemany(df):
    import mssql_python

    truncate_table()
    conn = mssql_python.connect(config.MSSQL_PYTHON_CONN_STR)
    cur = conn.cursor()
    rows = list(df.itertuples(index=False, name=None))
    cur.executemany(
        "INSERT INTO bench_bulk (id, value, amount, category, description, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    cur.close()
    conn.close()


# 5. pandas to_sql — default SQLAlchemy path
_engine_default = None

def _get_engine_default():
    global _engine_default
    if _engine_default is None:
        _engine_default = create_engine(config.SA_PYODBC_URL)
    return _engine_default


def insert_sa_to_sql_default(df):
    truncate_table()
    engine = _get_engine_default()
    df.to_sql(config.TABLE_NAME, engine, if_exists="append", index=False)


# 6. pandas to_sql with fast_executemany on the engine
_engine_fast = None

def _get_engine_fast():
    global _engine_fast
    if _engine_fast is None:
        _engine_fast = create_engine(config.SA_PYODBC_URL, fast_executemany=True)
    return _engine_fast


def insert_sa_to_sql_fast(df):
    truncate_table()
    engine = _get_engine_fast()
    df.to_sql(config.TABLE_NAME, engine, if_exists="append", index=False)


# 7. pandas to_sql with method='multi' — multi-row INSERT VALUES
# SQL Server caps at 2100 params, so chunksize = 2100 / 6 cols ≈ 350
def insert_sa_to_sql_multi(df):
    truncate_table()
    engine = _get_engine_default()
    df.to_sql(
        config.TABLE_NAME, engine,
        if_exists="append", index=False,
        method="multi", chunksize=300,
    )


# 8. SQLAlchemy Core insert — skip pandas overhead, go through table.insert()
_metadata = None
_bulk_table = None

def _get_bulk_table():
    global _metadata, _bulk_table
    if _bulk_table is None:
        engine = _get_engine_default()
        _metadata = MetaData()
        _bulk_table = Table(config.TABLE_NAME, _metadata, autoload_with=engine)
    return _bulk_table


def insert_sa_core_executemany(df):
    truncate_table()
    engine = _get_engine_fast()
    table = _get_bulk_table()
    records = df.to_dict("records")
    with engine.begin() as conn:
        conn.execute(table.insert(), records)


# 9. bcpandas — wraps bcp CLI, stages data as CSV then calls bcp
def insert_bcpandas(df):
    from bcpandas import SqlCreds, to_sql as bcp_to_sql

    creds = SqlCreds(
        server=f"{config.SERVER},{config.PORT}",
        database=config.DATABASE,
        username=config.USER,
        password=config.PASSWORD,
    )
    bcp_to_sql(df, config.TABLE_NAME, creds, if_exists="replace", index=False)


# 10. T-SQL BULK INSERT — server reads CSV directly from disk
def insert_bulk_insert_tsql(df):
    from test_data import save_to_csv

    truncate_table()
    _, container_path = save_to_csv(df, "bulk_data.csv")

    conn = pyodbc.connect(config.PYODBC_CONN_STR)
    cur = conn.cursor()
    cur.execute(f"""
        BULK INSERT {config.TABLE_NAME}
        FROM '{container_path}'
        WITH (
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            FIRSTROW = 2,
            TABLOCK
        )
    """)
    conn.commit()
    cur.close()
    conn.close()


# 11. bcp CLI — subprocess call to Microsoft's bulk copy utility
_has_bcp = shutil.which("bcp") is not None

def insert_bcp_cli(df):
    if not _has_bcp:
        raise FileNotFoundError("bcp not found on PATH")

    from test_data import save_to_csv

    truncate_table()
    host_path, _ = save_to_csv(df, "bcp_data.csv")

    cmd = [
        "bcp",
        f"{config.DATABASE}.dbo.{config.TABLE_NAME}",
        "in",
        host_path,
        "-c", "-t,", "-F", "2",
        "-S", f"{config.SERVER},{config.PORT}",
        "-U", config.USER,
        "-P", config.PASSWORD,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        raise RuntimeError(f"bcp failed: {result.stderr}")


# 12. TVP — pass entire dataset as a table-valued parameter to a stored proc
def insert_tvp_pyodbc(df):
    truncate_table()
    conn = pyodbc.connect(config.PYODBC_CONN_STR)
    cur = conn.cursor()

    rows = list(df.itertuples(index=False, name=None))
    cur.setinputsizes([("BulkLoadType", pyodbc.SQL_WVARCHAR, 0)])
    cur.execute("{CALL usp_insert_bulk_tvp (?)}", (rows,))
    conn.commit()
    cur.close()
    conn.close()


# 13. turbodbc — ODBC with columnar Arrow batching
def insert_turbodbc_arrow(df):
    import turbodbc
    import pyarrow as pa
    from turbodbc import connect as turbo_connect, make_options

    truncate_table()

    options = make_options(large_decimals_as_64_bit_types=True)
    conn = turbo_connect(connection_string=config.PYODBC_CONN_STR, turbodbc_options=options)
    cur = conn.cursor()

    table = pa.Table.from_pandas(df)
    columns = [table.column(i).to_pylist() for i in range(table.num_columns)]

    cur.executemanycolumns(
        "INSERT INTO bench_bulk (id, value, amount, category, description, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        columns,
    )
    conn.commit()
    cur.close()
    conn.close()


# 14. arrowsqlbcpy — Arrow -> .NET SqlBulkCopy bridge
def insert_arrowsqlbcpy(df):
    from arrowsqlbcpy import bulkcopy_from_pandas as arrow_bulk_insert

    ado_conn_str = (
        f"Server={config.SERVER},{config.PORT};"
        f"Database={config.DATABASE};"
        f"User Id={config.USER};"
        f"Password={config.PASSWORD};"
        "TrustServerCertificate=True;"
    )
    truncate_table()
    arrow_bulk_insert(df, ado_conn_str, config.TABLE_NAME)
