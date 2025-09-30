import pandas as pd
import pyodbc
import os
import toml

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

def create_medicloud_connection():
    secrets = load_secrets()
    driver = get_sql_driver()
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={secrets['credentials']['server']},{secrets['credentials']['port']};"
        f"DATABASE={secrets['credentials']['database']};"
        f"UID={secrets['credentials']['username']};"
        f"PWD={secrets['credentials']['password']};"
        f"Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)

def get_total_pa_procedures():
    conn = create_medicloud_connection()
    query = """
        SELECT
            txn.panumber,
            txn.groupname,
            txn.divisionname,
            txn.plancode,
            txn.IID,
            txn.providerid,
            txn.requestdate,
            txn.pastatus,
            tbp.code,
            tbp.requested,
            tbp.granted
        FROM dbo.tbPATxn txn
        JOIN dbo.tbPAProcedures tbp ON txn.panumber = tbp.panumber
        WHERE txn.requestdate >= DATEADD(month, -6, GETDATE()) AND txn.requestdate <= GETDATE();
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_active_members():
    conn = create_medicloud_connection()
    query = """
        SELECT
            mc.memberid,
                m.groupid,
                m.legacycode,
                m.planid,
                mc.iscurrent,
                m.isterminated,
                m.phone1,
                m.phone2,
                m.phone3,
                m.phone4,
                m.email1,
                m.email2,
                m.email3,
                m.email4,
                m.address1,
                m.address2,
                mc.effectivedate,
                mc.terminationdate
                FROM dbo.member_coverage mc
                JOIN dbo.member m ON mc.memberid = m.memberid
                WHERE m.isterminated = 0
                AND mc.iscurrent = 1
                AND CAST(mc.terminationdate AS DATETIME) >= CAST(GETDATE() AS DATETIME)
                AND m.legacycode LIKE 'CL%';
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_all_providers():
    conn = create_medicloud_connection()
    query = """
        SELECT
            p.*,
            l.lganame,
            s.statename,
            pc.categoryname
        FROM
            dbo.provider p
            JOIN dbo.providercategory pc ON p.provcatid = pc.provcatid
        LEFT JOIN
            dbo.lgas l ON p.lgaid = l.lgaid
        LEFT JOIN
            dbo.states s ON p.stateid = s.stateid
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_group_plan():
    conn = create_medicloud_connection()
    query = """
        SELECT * FROM dbo.group_plan
        WHERE iscurrent = 1
        AND CAST(terminationdate AS DATETIME) >= CAST(GETDATE() AS DATETIME)
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_pa_issue_request():
    conn = create_medicloud_connection()
    query = """
        SELECT Providerid, RequestDate, ResolutionTime, EncounterDate, PANumber, DateAdded
        FROM dbo.PAIssueRequest
        WHERE YEAR(EncounterDate) = YEAR(GETDATE())
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

if __name__ == "__main__":
    print("Fetching total_pa_procedures...")
    pa_df = get_total_pa_procedures()
    print(f"Total PA Procedures: {len(pa_df)}")

    print("Fetching active members...")
    members_df = get_active_members()
    print(f"Active Members: {len(members_df)}")

    print("Fetching all providers...")
    providers_df = get_all_providers()
    print(f"All Providers: {len(providers_df)}")

    print("Fetching group plan...")
    group_plan_df = get_group_plan()
    print(f"Group Plans: {len(group_plan_df)}")

    print("Fetching PA issue requests...")
    pa_issue_df = get_pa_issue_request()
    print(f"PA Issue Requests: {len(pa_issue_df)}")

# Merge pa with provider to get providername
PA = pa_df.join(
    providers_df.select(['providertin', 'providername']),
    left_on='providerid',
    right_on='providertin',
    how='left'
)