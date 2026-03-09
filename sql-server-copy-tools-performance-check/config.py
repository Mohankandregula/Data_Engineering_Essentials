import os
from dotenv import load_dotenv

load_dotenv()

SERVER = os.getenv("SQL_SERVER", "localhost")
PORT = int(os.getenv("SQL_PORT", "1433"))
DATABASE = os.getenv("SQL_DATABASE", "benchmark_db")
USER = os.getenv("SQL_USER", "sa")
PASSWORD = os.getenv("SQL_PASSWORD", "Str0ngPa55word_2024")

ODBC_DRIVER = "ODBC Driver 17 for SQL Server"

PYODBC_CONN_STR = (
    f"Driver={{{ODBC_DRIVER}}};"
    f"Server={SERVER},{PORT};"
    f"Database={DATABASE};"
    f"Uid={USER};"
    f"Pwd={PASSWORD};"
    "TrustServerCertificate=yes;"
)

PYODBC_MASTER_CONN_STR = (
    f"Driver={{{ODBC_DRIVER}}};"
    f"Server={SERVER},{PORT};"
    f"Database=master;"
    f"Uid={USER};"
    f"Pwd={PASSWORD};"
    "TrustServerCertificate=yes;"
)

PYMSSQL_ARGS = dict(
    server=SERVER,
    port=PORT,
    user=USER,
    password=PASSWORD,
    database=DATABASE,
)

MSSQL_PYTHON_CONN_STR = (
    f"Server={SERVER},{PORT};"
    f"Database={DATABASE};"
    f"Uid={USER};"
    f"Pwd={PASSWORD};"
    "TrustServerCertificate=yes;"
)

SA_PYODBC_URL = (
    f"mssql+pyodbc://{USER}:{PASSWORD}@{SERVER}:{PORT}/{DATABASE}"
    f"?driver={ODBC_DRIVER.replace(' ', '+')}&TrustServerCertificate=yes"
)

BCP_ARGS = dict(
    server=SERVER,
    database=DATABASE,
    username=USER,
    password=PASSWORD,
    port=PORT,
)

# staging directory for BULK INSERT — host side vs what SQL Server sees inside docker
STAGING_DIR_HOST = os.path.join(os.path.dirname(__file__), "staging")
STAGING_DIR_CONTAINER = "/staging"

TABLE_NAME = "bench_bulk"
