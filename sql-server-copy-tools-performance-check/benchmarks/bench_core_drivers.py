"""
richbench comparison: pyodbc vs pymssql on basic operations. Kept separate from the bulk copy benchmarks since these test
general driver overhead, not bulk loading specifically.

Run with:
    richbench benchmarks/ --repeat 5 --times 5
"""

import os
import sys
import pyodbc
import pymssql

# add parent dir to path so we can import config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# --- SELECT * ------
def bench_select_pyodbc():
    conn = pyodbc.connect(config.PYODBC_CONN_STR)
    cur = conn.cursor()
    cur.execute("SELECT * FROM bench_bulk")
    cur.fetchall()
    cur.close()
    conn.close()


def bench_select_pymssql():
    conn = pymssql.connect(**config.PYMSSQL_ARGS)
    cur = conn.cursor()
    cur.execute("SELECT * FROM bench_bulk")
    cur.fetchall()
    cur.close()
    conn.close()


# --- single INSERT ---

def bench_insert_pyodbc():
    conn = pyodbc.connect(config.PYODBC_CONN_STR)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO bench_bulk (id, value, amount, category, description, created_at) "
        "VALUES (1, 3.14, 100, 'test', 'benchmark row', '2024-01-01')"
    )
    conn.commit()
    # clean up so repeated runs don't pile up
    cur.execute("DELETE FROM bench_bulk WHERE id = 1 AND category = 'test'")
    conn.commit()
    cur.close()
    conn.close()


def bench_insert_pymssql():
    conn = pymssql.connect(**config.PYMSSQL_ARGS)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO bench_bulk (id, value, amount, category, description, created_at) "
        "VALUES (1, 3.14, 100, 'test', 'benchmark row', '2024-01-01')"
    )
    conn.commit()
    cur.execute("DELETE FROM bench_bulk WHERE id = 1 AND category = 'test'")
    conn.commit()
    cur.close()
    conn.close()


# --- executemany (100 rows) ---

_rows_100 = [(i, 1.5, 42, "bench", f"row_{i}", "2024-06-15") for i in range(100)]


def bench_executemany_pyodbc():
    conn = pyodbc.connect(config.PYODBC_CONN_STR)
    cur = conn.cursor()
    cur.executemany(
        "INSERT INTO bench_bulk (id, value, amount, category, description, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        _rows_100,
    )
    conn.commit()
    cur.execute("DELETE FROM bench_bulk WHERE category = 'bench'")
    conn.commit()
    cur.close()
    conn.close()


def bench_executemany_pymssql():
    conn = pymssql.connect(**config.PYMSSQL_ARGS)
    cur = conn.cursor()
    cur.executemany(
        "INSERT INTO bench_bulk (id, value, amount, category, description, created_at) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        _rows_100,
    )
    conn.commit()
    cur.execute("DELETE FROM bench_bulk WHERE category = 'bench'")
    conn.commit()
    cur.close()
    conn.close()


# --- fast_executemany (100 rows) - pyodbc exclusive feature ---

def bench_fast_executemany_pyodbc():
    conn = pyodbc.connect(config.PYODBC_CONN_STR)
    cur = conn.cursor()
    cur.fast_executemany = True
    cur.executemany(
        "INSERT INTO bench_bulk (id, value, amount, category, description, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        _rows_100,
    )
    conn.commit()
    cur.execute("DELETE FROM bench_bulk WHERE category = 'bench'")
    conn.commit()
    cur.close()
    conn.close()


def bench_fast_executemany_pymssql():
    # pymssql doesn't have fast_executemany, so this is just regular executemany.
    # including it so richbench has something to compare against.
    conn = pymssql.connect(**config.PYMSSQL_ARGS)
    cur = conn.cursor()
    cur.executemany(
        "INSERT INTO bench_bulk (id, value, amount, category, description, created_at) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        _rows_100,
    )
    conn.commit()
    cur.execute("DELETE FROM bench_bulk WHERE category = 'bench'")
    conn.commit()
    cur.close()
    conn.close()


# --- connection overhead ---

def bench_connect_pyodbc():
    conn = pyodbc.connect(config.PYODBC_CONN_STR)
    conn.close()


def bench_connect_pymssql():
    conn = pymssql.connect(**config.PYMSSQL_ARGS)
    conn.close()


__benchmarks__ = [
    (bench_select_pyodbc, bench_select_pymssql, "SELECT (pyodbc vs pymssql)"),
    (bench_insert_pyodbc, bench_insert_pymssql, "single INSERT"),
    (bench_executemany_pyodbc, bench_executemany_pymssql, "executemany 100 rows"),
    (bench_fast_executemany_pyodbc, bench_fast_executemany_pymssql, "fast_executemany vs pymssql 100 rows"),
    (bench_connect_pyodbc, bench_connect_pymssql, "connection overhead"),
]
