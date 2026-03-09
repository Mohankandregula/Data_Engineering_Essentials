"""
We will use richbench pairwise comparisons for bulk copy methods. Each pair compares a method against pyodbc fast_executemany as the baseline, since that's what most people reach for first.

Run with:
    richbench benchmarks/ --repeat 3 --times 3
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from test_data import make_test_df
from db_setup import truncate_table
from methods import (
    insert_pyodbc_executemany,
    insert_pyodbc_fast_executemany,
    insert_pymssql_executemany,
    insert_sa_to_sql_default,
    insert_sa_to_sql_fast,
    insert_sa_to_sql_multi,
    insert_sa_core_executemany,
    insert_bulk_insert_tsql,
    insert_bcp_cli,
    insert_tvp_pyodbc,
    _has_bcp,
)

# create a single test DataFrame to use across all benchmarks for consistency. 5K rows is enough to see differences without making the benchmark take too long.
DF = make_test_df(5000)


# wrap each method so richbench can call it with no args
def _baseline():
    insert_pyodbc_fast_executemany(DF)

def _pyodbc_slow():
    insert_pyodbc_executemany(DF)

def _pymssql():
    insert_pymssql_executemany(DF)

def _sa_default():
    insert_sa_to_sql_default(DF)

def _sa_fast():
    insert_sa_to_sql_fast(DF)

def _sa_multi():
    insert_sa_to_sql_multi(DF)

def _sa_core():
    insert_sa_core_executemany(DF)

def _bulk_insert():
    insert_bulk_insert_tsql(DF)

def _tvp():
    insert_tvp_pyodbc(DF)

def _bcp():
    insert_bcp_cli(DF)


__benchmarks__ = [
    (_pyodbc_slow, _baseline, "executemany vs fast_executemany (5K rows)"),
    (_baseline, _pymssql, "fast_executemany vs pymssql (5K rows)"),
    (_baseline, _sa_fast, "fast_executemany vs SA to_sql fast (5K rows)"),
    (_sa_default, _sa_fast, "SA to_sql default vs fast (5K rows)"),
    (_sa_fast, _sa_multi, "SA to_sql fast vs multi (5K rows)"),
    (_sa_fast, _sa_core, "SA to_sql fast vs Core insert (5K rows)"),
    (_baseline, _bulk_insert, "fast_executemany vs BULK INSERT (5K rows)"),
    (_baseline, _tvp, "fast_executemany vs TVP (5K rows)"),
]

if _has_bcp:
    __benchmarks__.append(
        (_baseline, _bcp, "fast_executemany vs bcp CLI (5K rows)")
    )

# optional methods if available to install — these may have additional dependencies or require more setup, so we keep them separate
try:
    from methods import insert_bcpandas, _has_bcpandas
    if _has_bcpandas:
        def _bcpandas():
            insert_bcpandas(DF)
        __benchmarks__.append(
            (_baseline, _bcpandas, "fast_executemany vs bcpandas BCP (5K rows)")
        )
except ImportError:
    pass

try:
    from methods import insert_turbodbc_arrow, _has_turbodbc
    if _has_turbodbc:
        def _turbodbc():
            insert_turbodbc_arrow(DF)
        __benchmarks__.append(
            (_baseline, _turbodbc, "fast_executemany vs turbodbc Arrow (5K rows)")
        )
except ImportError:
    pass
