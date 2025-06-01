import pyodbc
import os
from dotenv import load_dotenv

load_dotenv()

server = os.getenv("Azure_Server")
database = os.getenv("Azure_DB")
username = os.getenv("Azure_SQL")
password = os.getenv("Azure_Pass")

def get_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)