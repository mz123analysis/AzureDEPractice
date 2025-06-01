import pyodbc
import os
from dotenv import load_dotenv

script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
parent_dir = os.path.dirname(parent_dir)
env_path = os.path.join(parent_dir,".env")
load_dotenv(env_path)

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