import pandas as pd
import pyodbc
import toml
import os

def load_secrets():
    secrets_path = "secrets.toml"
    if os.path.exists(secrets_path):
        return toml.load(secrets_path)
    else:
        raise FileNotFoundError(f"Secrets file not found: {secrets_path}")

def get_sql_driver():
    drivers = [x for x in pyodbc.drivers()]
    preferred = [
        'ODBC Driver 18 for SQL Server',
        'ODBC Driver 17 for SQL Server',
        'SQL Server Native Client 11.0',
        'SQL Server'
    ]
    for d in preferred:
        if d in drivers:
            return d
    raise RuntimeError("No compatible SQL Server driver found.")

def create_eaccount_connection():
    """
    Create and return a connection to the eAccount SQL Server database using secrets.toml.
    """
    secrets = load_secrets()
    driver = get_sql_driver()
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={secrets['eaccount_credentials']['server']},{secrets['eaccount_credentials']['port']};"
        f"DATABASE={secrets['eaccount_credentials']['database']};"
        f"UID={secrets['eaccount_credentials']['username']};"
        f"PWD={secrets['eaccount_credentials']['password']};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
        f"Connection Timeout=30;"
    )
    try:
        print("Connection string used:EACCOUNT ", conn_str)
        return pyodbc.connect(conn_str)
    except pyodbc.Error as e:
        print(f"Connection string used: {conn_str.replace(secrets['eaccount_credentials']['password'], '****')}")
        raise e

def fetch_debit_note():
    """
    Fetch debit_note table from eAccount and return as a pandas DataFrame.
    """
    conn = create_eaccount_connection()
    try:
        query = """
            SELECT *
            FROM dbo.DEBIT_Note
            WHERE [From] >= '2023-01-01' AND [From] <= GETDATE();
        """
        df = pd.read_sql(query, conn)
        print(f"Fetched {len(df)} rows from debit_note table.")
        return df
    except Exception as e:
        print(f"Error fetching debit_note: {e}")
        return None
    finally:
        conn.close()

if __name__ == "__main__":
    df = fetch_debit_note()
    if df is not None:
        print(df.head())

