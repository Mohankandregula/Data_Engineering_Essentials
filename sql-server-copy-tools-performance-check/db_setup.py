import pyodbc
import config


def get_master_conn():
    return pyodbc.connect(config.PYODBC_MASTER_CONN_STR, autocommit=True)


def get_conn():
    return pyodbc.connect(config.PYODBC_CONN_STR, autocommit=True)


def create_database():
    conn = get_master_conn()
    cur = conn.cursor()
    cur.execute(f"""
        IF DB_ID('{config.DATABASE}') IS NULL
            CREATE DATABASE [{config.DATABASE}]
    """)
    cur.close()
    conn.close()
    print(f"database '{config.DATABASE}' ready")


def create_tables():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        IF OBJECT_ID('bench_bulk', 'U') IS NOT NULL DROP TABLE bench_bulk;
        CREATE TABLE bench_bulk (
            id          INT,
            value       FLOAT,
            amount      INT,
            category    NVARCHAR(50),
            description NVARCHAR(200),
            created_at  DATETIME
        )
    """)
    cur.close()
    conn.close()
    print("table 'bench_bulk' created")


def create_tvp_type():
    conn = get_conn()
    cur = conn.cursor()
    # drop proc first since it depends on the type
    cur.execute("""
        IF OBJECT_ID('usp_insert_bulk_tvp', 'P') IS NOT NULL
            DROP PROCEDURE usp_insert_bulk_tvp
    """)
    cur.execute("""
        IF TYPE_ID('BulkLoadType') IS NOT NULL
            DROP TYPE BulkLoadType
    """)
    cur.execute("""
        CREATE TYPE BulkLoadType AS TABLE (
            id          INT,
            value       FLOAT,
            amount      INT,
            category    NVARCHAR(50),
            description NVARCHAR(200),
            created_at  DATETIME
        )
    """)
    cur.close()
    conn.close()
    print("TVP type 'BulkLoadType' created")


def create_tvp_proc():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        IF OBJECT_ID('usp_insert_bulk_tvp', 'P') IS NOT NULL
            DROP PROCEDURE usp_insert_bulk_tvp
    """)
    cur.execute("""
        CREATE PROCEDURE usp_insert_bulk_tvp
            @data BulkLoadType READONLY
        AS
        BEGIN
            SET NOCOUNT ON;
            INSERT INTO bench_bulk (id, value, amount, category, description, created_at)
            SELECT id, value, amount, category, description, created_at
            FROM @data;
        END
    """)
    cur.close()
    conn.close()
    print("stored proc 'usp_insert_bulk_tvp' created")


def truncate_table():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE {config.TABLE_NAME}")
    cur.close()
    conn.close()


def drop_all():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        IF OBJECT_ID('usp_insert_bulk_tvp', 'P') IS NOT NULL
            DROP PROCEDURE usp_insert_bulk_tvp;
        IF TYPE_ID('BulkLoadType') IS NOT NULL
            DROP TYPE BulkLoadType;
        IF OBJECT_ID('bench_bulk', 'U') IS NOT NULL
            DROP TABLE bench_bulk;
    """)
    cur.close()
    conn.close()
    print("cleaned up everything")


def setup_all():
    create_database()
    create_tables()
    create_tvp_type()
    create_tvp_proc()
    print("\nall set — ready to benchmark")


if __name__ == "__main__":
    setup_all()
