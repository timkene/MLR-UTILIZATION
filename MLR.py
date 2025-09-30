import os
import pandas as pd
import streamlit as st
import polars as pl
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, date, timedelta
import pyodbc
import toml
import plotly.graph_objects as go
import io
import zipfile

# Configure the page
st.set_page_config(
    page_title="MLR & Utilization Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("MLR Analysis & Utilization Dashboard with Email Alerts")

# Email configuration
SENDER_EMAIL = "leocasey0@gmail.com"
RECIPIENTS = [
    "k.chukwuka@clearlinehmo.com",
    "k.odegbami@clearlinehmo.com"
]
THRESHOLDS = [65, 75, 85]

# Benefit group functions
@st.cache_data(ttl=7200)  # 2 hours cache for benefit codes
def load_benefit_codes():
    """Load procedure codes from benefits_cleaned.csv"""
    try:
        import csv
        benefit_codes = {}
        
        with open('benefits_cleaned.csv', 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                benefit_group = row['benefit_group']
                procedure_code = row['procedure_code']
                
                if benefit_group not in benefit_codes:
                    benefit_codes[benefit_group] = []
                benefit_codes[benefit_group].append(procedure_code)
        
        return benefit_codes
    except Exception as e:
        st.error(f"Error loading benefit codes: {str(e)}")
        return {}

def get_procedure_codes_for_benefit_group(benefit_group_name):
    """Get procedure codes for a specific benefit group"""
    benefit_codes = load_benefit_codes()
    return benefit_codes.get(benefit_group_name, [])

# Database connection functions (from dlt_sources.py)
def load_secrets():
    """Load secrets from environment variables or toml file"""
    # Try environment variables first (for production/GitHub)
    if all(os.getenv(key) for key in ['MEDICLOUD_SERVER', 'MEDICLOUD_DATABASE', 'MEDICLOUD_USERNAME', 'MEDICLOUD_PASSWORD', 'MEDICLOUD_PORT']):
        return {
            'credentials': {
                'server': os.getenv('MEDICLOUD_SERVER'),
                'database': os.getenv('MEDICLOUD_DATABASE'),
                'username': os.getenv('MEDICLOUD_USERNAME'),
                'password': os.getenv('MEDICLOUD_PASSWORD'),
                'port': os.getenv('MEDICLOUD_PORT')
            },
            'eaccount_credentials': {
                'server': os.getenv('EACOUNT_SERVER'),
                'database': os.getenv('EACOUNT_DATABASE'),
                'username': os.getenv('EACOUNT_USERNAME'),
                'password': os.getenv('EACOUNT_PASSWORD'),
                'port': os.getenv('EACOUNT_PORT')
            }
        }
    
    # Fallback to toml file (for local development)
    secrets_path = "secrets.toml"
    if os.path.exists(secrets_path):
        return toml.load(secrets_path)
    else:
        raise FileNotFoundError(f"Secrets file not found: {secrets_path}. Please set environment variables or create secrets.toml")

def get_sql_driver():
    """Get compatible SQL Server driver"""
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

@st.cache_resource
def get_database_connections():
    """Create database connections with caching"""
    try:
        secrets = load_secrets()
        driver = get_sql_driver()
        
        # MediCloud connection
        medicloud_conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={secrets['credentials']['server']},{secrets['credentials']['port']};"
            f"DATABASE={secrets['credentials']['database']};"
            f"UID={secrets['credentials']['username']};"
            f"PWD={secrets['credentials']['password']};"
            f"Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
        )
        
        # EACOUNT connection
        eacount_conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={secrets['eaccount_credentials']['server']},{secrets['eaccount_credentials']['port']};"
            f"DATABASE={secrets['eaccount_credentials']['database']};"
            f"UID={secrets['eaccount_credentials']['username']};"
            f"PWD={secrets['eaccount_credentials']['password']};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout=30;"
        )
        
        return medicloud_conn_str, eacount_conn_str, secrets
    except Exception as e:
        st.error(f"Error loading database configuration: {str(e)}")
        return None, None, None

@st.cache_data(ttl=10800)  # 3 hours = 10800 seconds
def load_data_from_sources():
    """Load data directly from source databases with caching"""
    try:
        medicloud_conn_str, eacount_conn_str, secrets = get_database_connections()
        
        if not all([medicloud_conn_str, eacount_conn_str]):
            st.error("Failed to get database connection strings")
            return None
        
        # Create a more engaging loading experience
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Connect to MediCloud database
        status_text.text("🔌 Connecting to MediCloud database...")
        progress_bar.progress(5)
        medicloud_conn = pyodbc.connect(medicloud_conn_str)
        
        # Load MediCloud tables with progress tracking
        status_text.text("📊 Loading group contracts...")
        progress_bar.progress(10)
        GROUP_CONTRACT = pd.read_sql("""
            SELECT 
            gc.groupid,
            gc.startdate,
            gc.enddate,
            g.groupname
            FROM dbo.group_contract gc
            JOIN dbo.[group] g ON gc.groupid = g.groupid
            WHERE gc.iscurrent = 1
            AND CAST(gc.enddate AS DATETIME) >= CAST(GETDATE() AS DATETIME);
        """, medicloud_conn)
        
        status_text.text("🏥 Loading claims data (this may take a moment)...")
        progress_bar.progress(20)
        CLAIMS = pd.read_sql("""
            SELECT nhislegacynumber, nhisproviderid, nhisgroupid, panumber, encounterdatefrom, 
                   datesubmitted, chargeamount, approvedamount, procedurecode, deniedamount 
            FROM dbo.claims
            WHERE datesubmitted >= '2024-07-01' AND datesubmitted <= GETDATE();
        """, medicloud_conn)
        
        status_text.text("👥 Loading groups data...")
        progress_bar.progress(30)
        GROUPS = pd.read_sql("SELECT * FROM dbo.[group]", medicloud_conn)
        
        status_text.text("📋 Loading PA procedures...")
        progress_bar.progress(40)
        PA = pd.read_sql("""
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
            WHERE txn.requestdate >= '2024-07-01' AND txn.requestdate <= GETDATE();
        """, medicloud_conn)
        
        status_text.text("👤 Loading active members...")
        progress_bar.progress(50)
        ACTIVE_ENROLLEE = pd.read_sql("""
            SELECT
            mc.memberid,
            m.groupid,
            m.legacycode,
            m.planid,
            mc.iscurrent,
            m.isterminated,
            mc.effectivedate,
            mc.terminationdate
            FROM dbo.member_coverage mc
            JOIN dbo.member m ON mc.memberid = m.memberid
            WHERE m.isterminated = 0
            AND mc.iscurrent = 1
            AND CAST(mc.terminationdate AS DATETIME) >= CAST(GETDATE() AS DATETIME)
            AND m.legacycode LIKE 'CL%';
        """, medicloud_conn)
        
        status_text.text("📋 Loading member plans...")
        progress_bar.progress(60)
        M_PLAN = pd.read_sql("SELECT * FROM dbo.member_plan", medicloud_conn)
        
        status_text.text("🏢 Loading group plans...")
        progress_bar.progress(70)
        G_PLAN = pd.read_sql("""
            SELECT * FROM dbo.group_plan
            WHERE iscurrent = 1
            AND CAST(terminationdate AS DATETIME) >= CAST(GETDATE() AS DATETIME)
        """, medicloud_conn)
        
        status_text.text("📊 Loading plans data...")
        progress_bar.progress(75)
        PLAN = pd.read_sql("SELECT * FROM dbo.plans", medicloud_conn)
        
        # Additional tables needed for utilization analysis
        status_text.text("🏥 Loading providers data...")
        progress_bar.progress(80)
        PROVIDER = pd.read_sql("""
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
        """, medicloud_conn)
        
        status_text.text("💊 Loading benefit codes...")
        progress_bar.progress(85)
        BENEFIT = pd.read_sql("SELECT * FROM dbo.benefitcode", medicloud_conn)
        
        status_text.text("🔗 Loading benefit procedure codes...")
        progress_bar.progress(90)
        BEN_CODE = pd.read_sql("""
            SELECT
            bcf.benefitcodeid,
            bcf.procedurecode,
            bc.benefitcodename,
            bc.benefitcodedesc
            FROM dbo.benefitcode_procedure bcf
            JOIN dbo.benefitcode bc ON bcf.benefitcodeid = bc.benefitcodeid
        """, medicloud_conn)
        
        medicloud_conn.close()
        
        # Connect to EACOUNT database
        status_text.text("🔌 Connecting to EACOUNT database...")
        progress_bar.progress(92)
        eacount_conn = pyodbc.connect(eacount_conn_str)
        
        status_text.text("💰 Loading debit notes...")
        progress_bar.progress(95)
        DEBIT = pd.read_sql("""
            SELECT *
            FROM dbo.DEBIT_Note
            WHERE [From] >= '2023-01-01' AND [From] <= GETDATE();
        """, eacount_conn)
        
        eacount_conn.close()
        
        # Convert to Polars DataFrames for efficient processing
        status_text.text("🔄 Converting data to Polars format...")
        progress_bar.progress(98)
        GROUP_CONTRACT = pl.from_pandas(GROUP_CONTRACT)
        CLAIMS = pl.from_pandas(CLAIMS)
        GROUPS = pl.from_pandas(GROUPS)
        DEBIT = pl.from_pandas(DEBIT)
        PA = pl.from_pandas(PA)
        ACTIVE_ENROLLEE = pl.from_pandas(ACTIVE_ENROLLEE)
        M_PLAN = pl.from_pandas(M_PLAN)
        G_PLAN = pl.from_pandas(G_PLAN)
        PLAN = pl.from_pandas(PLAN)
        PROVIDER = pl.from_pandas(PROVIDER)
        BENEFIT = pl.from_pandas(BENEFIT)
        BEN_CODE = pl.from_pandas(BEN_CODE)
        
        # Final progress update
        status_text.text("✅ Data loading completed!")
        progress_bar.progress(100)
        
        # Clear the progress indicators
        import time
        time.sleep(1)
        progress_bar.empty()
        status_text.empty()
            
        st.success("✅ Data loaded successfully from source databases!")
        return PA, GROUP_CONTRACT, CLAIMS, GROUPS, DEBIT, ACTIVE_ENROLLEE, M_PLAN, G_PLAN, PLAN, PROVIDER, BENEFIT, BEN_CODE
        
    except Exception as e:
        st.error(f"Error loading data from sources: {str(e)}")
        return None, None, None, None, None, None, None, None, None, None, None, None

def send_mlr_alert_email(groupname, mlr_value, mlr_type, threshold, sender_password):
    """Send email alert for MLR threshold breach"""
    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = SENDER_EMAIL
        msg['To'] = ", ".join(RECIPIENTS)
        msg['Subject'] = f"MLR Alert: {groupname} has reached {threshold}% threshold"
        
        # Email body
        body = f"""Dear Team,

Kindly note that "{groupname}" has hit the {threshold}% mark for {mlr_type}.

Current MLR: {mlr_value}%
Type: {mlr_type}
Date: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Kindly check the webapp for details and plan towards its renewal.

Best regards,
MLR Monitoring System
        """
        
        msg.attach(MIMEText(body, 'plain'))
        
        # Connect to Gmail SMTP server
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(SENDER_EMAIL, sender_password)
        
        # Send email
        text = msg.as_string()
        server.sendmail(SENDER_EMAIL, RECIPIENTS, text)
        server.quit()
        
        return True
    except Exception as e:
        st.error(f"Failed to send email: {str(e)}")
        return False

def load_sent_notifications():
    """Load previously sent notifications from session state"""
    if 'sent_notifications' not in st.session_state:
        st.session_state.sent_notifications = {}
    return st.session_state.sent_notifications

def save_notification(groupname, mlr_type, threshold):
    """Save notification to prevent duplicate sends"""
    if 'sent_notifications' not in st.session_state:
        st.session_state.sent_notifications = {}
    
    key = f"{groupname}_{mlr_type}_{threshold}"
    st.session_state.sent_notifications[key] = datetime.now().isoformat()

def should_send_notification(groupname, mlr_type, threshold):
    """Check if notification should be sent (avoid duplicates)"""
    sent_notifications = load_sent_notifications()
    key = f"{groupname}_{mlr_type}_{threshold}"
    return key not in sent_notifications

def check_and_send_alerts(pa_merged, claims_merged, sender_password):
    """Check MLR values and send alerts if thresholds are breached"""
    alerts_sent = []
    
    if pa_merged.height > 0:
        pa_df = pa_merged.to_pandas()
        for _, row in pa_df.iterrows():
            mlr_value = row['MLR(PA) (%)']
            groupname = row['groupname']
            
            for threshold in THRESHOLDS:
                if mlr_value >= threshold and should_send_notification(groupname, 'PA', threshold):
                    if send_mlr_alert_email(groupname, mlr_value, 'PA', threshold, sender_password):
                        save_notification(groupname, 'PA', threshold)
                        alerts_sent.append(f"{groupname} - PA: {mlr_value}% (≥{threshold}%)")
    
    if claims_merged.height > 0:
        claims_df = claims_merged.to_pandas()
        for _, row in claims_df.iterrows():
            mlr_value = row['MLR(CLAIMS) (%)']
            groupname = row['groupname']
            
            for threshold in THRESHOLDS:
                if mlr_value >= threshold and should_send_notification(groupname, 'CLAIMS', threshold):
                    if send_mlr_alert_email(groupname, mlr_value, 'CLAIMS', threshold, sender_password):
                        save_notification(groupname, 'CLAIMS', threshold)
                        alerts_sent.append(f"{groupname} - CLAIMS: {mlr_value}% (≥{threshold}%)")
    
    return alerts_sent

@st.cache_data(ttl=1800)  # 30 minutes cache for MLR calculations
def calculate_mlr(PA, GROUP_CONTRACT, CLAIMS, GROUPS, DEBIT):
    """Calculate MLR metrics"""
    try:
        # --- PA MLR ---
        PA = PA.with_columns([
            pl.col('requestdate').cast(pl.Datetime),
            pl.col('granted').cast(pl.Float64, strict=False)
        ])
        group_contract_dates = GROUP_CONTRACT.select(['groupname', 'startdate', 'enddate']).with_columns([
            pl.col('startdate').cast(pl.Datetime),
            pl.col('enddate').cast(pl.Datetime)
        ])
        pa_filtered = PA.join(group_contract_dates, on='groupname', how='inner').filter(
            (pl.col('requestdate') >= pl.col('startdate')) & (pl.col('requestdate') <= pl.col('enddate'))
        )
        PA_mlr = pa_filtered.group_by('groupname').agg(
            pl.col('granted').sum().alias('Total cost')
        )

        # --- CLAIMS MLR ---
        CLAIMS = CLAIMS.with_columns([
            pl.col('approvedamount').cast(pl.Float64),
            pl.col('encounterdatefrom').cast(pl.Datetime),
            pl.col('nhisgroupid').cast(pl.Utf8)
        ])
        GROUPS = GROUPS.with_columns(pl.col('groupid').cast(pl.Utf8))
        claims_with_group = CLAIMS.join(
            GROUPS.select(['groupid', 'groupname']),
            left_on='nhisgroupid', right_on='groupid', how='inner'
        )
        claims_with_dates = claims_with_group.join(
            group_contract_dates, on='groupname', how='inner'
        ).filter(
            (pl.col('encounterdatefrom') >= pl.col('startdate')) & (pl.col('encounterdatefrom') <= pl.col('enddate'))
        )
        claims_mlr = claims_with_dates.group_by('groupname').agg(
            pl.col('approvedamount').sum().alias('Total cost')
        ).sort('Total cost', descending=True)

        # --- DEBIT NOTE (filtered by contract dates) ---
        # Ensure DEBIT is pandas DataFrame for filtering
        if not isinstance(DEBIT, pd.DataFrame):
            DEBIT = DEBIT.to_pandas()
        
        # Convert date column and filter out rows containing "tpa" in description
        DEBIT['From'] = pd.to_datetime(DEBIT['From'])
        CURRENT_DEBIT = DEBIT[~DEBIT['Description'].str.contains('tpa', case=False, na=False)]
        
        # Change company_name to groupname for consistency
        CURRENT_DEBIT = CURRENT_DEBIT.rename(columns={'CompanyName': 'groupname'})
        
        # Convert to polars for joining with contract dates
        current_debit_pl = pl.from_pandas(CURRENT_DEBIT)
        
        # Join with contract dates and filter by contract period
        debit_with_dates = current_debit_pl.join(
            group_contract_dates, on='groupname', how='inner'
        ).filter(
            (pl.col('From') >= pl.col('startdate')) & (pl.col('From') <= pl.col('enddate'))
        )
        
        # Group by company and sum amounts within contract period
        DEBIT_BY_CLIENT = debit_with_dates.group_by('groupname').agg(
            pl.col('Amount').sum().alias('amount')
        ).sort('amount', descending=True)

        # --- Merge Results ---
        debit_df = DEBIT_BY_CLIENT.rename({'amount': 'Total cost(DEBIT_BY_CLIENT)'})
        pa_df = PA_mlr.rename({'Total cost': 'Total cost(PA)'}).with_columns(
            (pl.col('Total cost(PA)') * 1.4).round(2).alias('PA40%')
        )
        claims_df = claims_mlr.rename({'Total cost': 'Total cost(claims)'})

        # Calculate PA MLR DataFrame
        pa_merged = debit_df.join(
            pa_df.select(['groupname', 'Total cost(PA)', 'PA40%']),
            on='groupname', how='outer'
        )
        pa_merged = pa_merged.with_columns(
            (pl.col('Total cost(DEBIT_BY_CLIENT)') * 0.10).round(2).alias('commission')
        ).select([
            'groupname',
            'Total cost(DEBIT_BY_CLIENT)',
            'Total cost(PA)',
            'PA40%',
            'commission'
        ])
        pa_merged = pa_merged.with_columns([
            (
                (pl.col('PA40%').fill_null(0) +
                    pl.col('commission').fill_null(0)
                ) / pl.col('Total cost(DEBIT_BY_CLIENT)').fill_null(0) * 100
            ).round(2).alias('MLR(PA) (%)')
        ])

        # Calculate CLAIMS MLR DataFrame
        claims_merged = debit_df.join(
            claims_df.select(['groupname', 'Total cost(claims)']),
            on='groupname', how='outer'
        )
        claims_merged = claims_merged.with_columns(
            (pl.col('Total cost(DEBIT_BY_CLIENT)') * 0.10).round(2).alias('commission')
        ).select([
            'groupname',
            'Total cost(DEBIT_BY_CLIENT)',
            'Total cost(claims)',
            'commission'
        ])
        claims_merged = claims_merged.with_columns([
            (
                (
                    pl.col('Total cost(claims)').fill_null(0) +
                    pl.col('commission').fill_null(0)
                ) / pl.col('Total cost(DEBIT_BY_CLIENT)').fill_null(0) * 100
            ).round(2).alias('MLR(CLAIMS) (%)')
        ])

        # Return both DataFrames
        return pa_merged, claims_merged
        
    except Exception as e:
        st.error(f"Error calculating MLR: {str(e)}")
        return pl.DataFrame(), pl.DataFrame()

@st.cache_data(ttl=1800)  # 30 minutes cache for retail MLR calculations
def calculate_retail_mlr(PA, ACTIVE_ENROLLEE, M_PLAN, G_PLAN, GROUPS, PLAN):
    try:
        # Ensure consistent data types
        ACTIVE_ENROLLEE = ACTIVE_ENROLLEE.with_columns([
            pl.col("legacycode").cast(pl.Utf8),
            pl.col("memberid").cast(pl.Int64)
        ])

        M_PLAN = M_PLAN.with_columns([
            pl.col("memberid").cast(pl.Int64),
            pl.col("planid").cast(pl.Int64),
            pl.col("iscurrent").cast(pl.Utf8)
        ])

        G_PLAN = G_PLAN.with_columns([
            pl.col("planid").cast(pl.Int64),
            pl.col("groupid").cast(pl.Int64),
            pl.col("individualprice").cast(pl.Float64),
            pl.col("familyprice").cast(pl.Float64),
            pl.col("maxnumdependant").cast(pl.Int64)
        ])

        PA = PA.with_columns([
            pl.col("requestdate").cast(pl.Datetime),
            pl.col("IID").cast(pl.Utf8),
            pl.col("granted").cast(pl.Float64)
        ])

        GROUPS = GROUPS.with_columns([
            pl.col("groupid").cast(pl.Int64),
            pl.col("groupname").cast(pl.Utf8)
        ])

        # Filter current plans
        M_PLANN = M_PLAN.filter(pl.col("iscurrent") == "true")
        PAA = PA.with_columns(pl.col("requestdate").dt.year().alias("year"))

        # Join with group names
        G_PLANN = G_PLAN.join(
            GROUPS.select(['groupid', 'groupname']),
            on='groupid',
            how='left'
        )
        # Filter G_PLANN to only include rows where groupname is 'FAMILY SCHEME' (case-insensitive)
        G_PLANN = G_PLANN.filter(
            pl.col("groupname").str.to_lowercase() == "family scheme"
        )

        # Isolate all unique planid in G_PLANN
        unique_planids = G_PLANN.select("planid").unique()

        # Filter ACTIVE_ENROLLEE to only contain data where their planid is inside the isolated planids of G_PLANN
        # First, ensure ACTIVE_ENROLLEE has planid column (join if necessary)
        if 'planid' in ACTIVE_ENROLLEE.columns:
            ACTIVE_ENROLLEE = ACTIVE_ENROLLEE.drop('planid')
        ACTIVE_ENROLLEE = ACTIVE_ENROLLEE.join(
            M_PLANN.select(['memberid', 'planid']),
            on='memberid',
            how='left'
        )
        # Now filter ACTIVE_ENROLLEE to only those with planid in unique_planids
        ACTIVE_RETAIL = ACTIVE_ENROLLEE.join(
            unique_planids,
            on="planid",
            how="inner"
        )

        # Merge G_PLANN and PLAN to get 'planname' into G_PLANN using 'planid' as common column
        if 'planid' in G_PLANN.columns and 'planid' in PLAN.columns:
            F_GPLAN = G_PLANN.join(
                PLAN.select(['planid', 'planname']),
                on='planid',
                how='left'
            )
        else:
            F_GPLAN = G_PLANN

        # Create a new column 'premium' for each row: (individualprice * countofindividual + countoffamily * familyprice)
        FG_PLANN = F_GPLAN.with_columns(
            (pl.col("individualprice") * pl.col("countofindividual") + pl.col("countoffamily") * pl.col("familyprice")).alias("premium")
        )
        # Calculate total retail premium as the sum of the 'premium' column
        # Group by 'planname' and sum 'premium' for each planname
        total_retail_premium_by_plan = FG_PLANN.group_by("planname").agg(
            pl.col("premium").sum().alias("total_premium")
        )

        # Join with ACTIVE_ENROLLEE
        PA_M = PA.join(
            ACTIVE_ENROLLEE.select(['legacycode', 'memberid']),
            left_on='IID',
            right_on='legacycode',
            how='left'
        )

        # Join with M_PLANN
        PA_MP = PA_M.join(
            M_PLANN.select(['memberid', 'planid']),
            on='memberid',
            how='left'
        )

        # Filter PA to only include rows where groupname is 'family scheme' (case-insensitive)
        PAA = PA_MP.filter(
            pl.col("groupname").str.to_lowercase() == "family scheme"
        )

        # Join PLAN to PAA to get 'planname' into PAA using 'planid'
        if 'planid' in PAA.columns and 'planid' in PLAN.columns:
            PAA = PAA.join(
                PLAN.select(['planid', 'planname']),
                on='planid',
                how='left'
            )

        # Join PAA with ACTIVE_ENROLLEE to get effectivedate and terminationdate for each IID
        if 'IID' in PAA.columns and 'legacycode' in ACTIVE_ENROLLEE.columns:
            PAA = PAA.join(
                ACTIVE_ENROLLEE.select(['legacycode', 'effectivedate', 'terminationdate']),
                left_on='IID',
                right_on='legacycode',
                how='left'
            )

        # Filter PAA to only include claims within the customer's active period
        # This is the key step that was missing in your original code
        if all(col in PAA.columns for col in ['IID', 'planname', 'granted', 'requestdate', 'effectivedate', 'terminationdate']):
            # Filter claims to only those within the customer's active enrollment period
            filtered_PAA = PAA.filter(
                (pl.col('requestdate') >= pl.col('effectivedate')) & 
                (pl.col('requestdate') <= pl.col('terminationdate'))
            )
            
            # Now group by IID and planname, and sum the granted amounts
            grouped_PAA = filtered_PAA.group_by(['IID', 'planname']).agg(
                pl.col('granted').sum().alias('total_cost')
            )
            
            # Select final columns: IID (or legacycode), total_cost, planname
            result_df = grouped_PAA.select(['IID', 'total_cost', 'planname'])
        else:
            result_df = pl.DataFrame()

        # Group result_df by planname and sum total_cost
        if result_df.height > 0:
            total_cost_by_plan = result_df.group_by('planname').agg(
                pl.col('total_cost').sum().alias('total_cost')
            )
        else:
            total_cost_by_plan = pl.DataFrame()

        # Ensure 'planname' is string type in both DataFrames before merging
        if (total_retail_premium_by_plan.height > 0 and total_cost_by_plan.height > 0 and 
            'planname' in total_retail_premium_by_plan.columns and 'planname' in total_cost_by_plan.columns):
            total_retail_premium_by_plan = total_retail_premium_by_plan.with_columns(
                pl.col('planname').cast(pl.Utf8)
            )
            total_cost_by_plan = total_cost_by_plan.with_columns(
                pl.col('planname').cast(pl.Utf8)
            )
            merged_plan_df = total_retail_premium_by_plan.join(
                total_cost_by_plan,
                on='planname',
                how='left'
            )
        else:
            merged_plan_df = pl.DataFrame()

        return result_df, merged_plan_df

    except Exception as e:
        st.error(f"Error calculating retail MLR: {str(e)}")
        return pl.DataFrame(), pl.DataFrame()

# Main Streamlit app
if __name__ == "__main__":
    # Email configuration section
    st.sidebar.header("📧 Email Configuration")
    
    # Input for email password (use environment variable or user input)
    email_password = st.sidebar.text_input(
        "Gmail App Password", 
        type="password",
        help="Enter your Gmail App Password (not your regular password)"
    )
    
    # Enable/disable email alerts
    email_alerts_enabled = st.sidebar.checkbox("Enable Email Alerts", value=True)
    
    # Display current email settings
    with st.sidebar.expander("Email Settings"):
        st.write("**Sender:** leocasey0@gmail.com")
        st.write("**Recipients:**")
        for recipient in RECIPIENTS:
            st.write(f"- {recipient}")
        st.write("**Thresholds:** 65%, 75%, 85%")
    
    # Add database connection status
    st.sidebar.header("🔗 Database Status")
    try:
        medicloud_conn_str, eacount_conn_str, secrets = get_database_connections()
        if medicloud_conn_str and eacount_conn_str:
            st.sidebar.success("✅ Database connections ready")
        else:
            st.sidebar.error("❌ Database connection failed")
    except:
        st.sidebar.error("❌ Database configuration error")
    
    # Cache status and refresh options
    st.sidebar.header("🔄 Data Cache Status")
    
    # Check if data is cached
    cache_key = "load_data_from_sources"
    if hasattr(st.session_state, '_cache') and cache_key in st.session_state._cache:
        cache_info = st.session_state._cache[cache_key]
        st.sidebar.info(f"📊 Data cached for 3 hours")
        st.sidebar.text(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")
    else:
        st.sidebar.warning("🔄 No cached data - will load fresh")
    
    # Manual refresh button
    if st.sidebar.button("🔄 Force Refresh Data", help="Clear cache and reload all data"):
        st.cache_data.clear()
        st.rerun()
    
    # Cache TTL info
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Cache Settings:**")
    st.sidebar.markdown("• **Refresh Interval:** 3 hours")
    st.sidebar.markdown("• **Next Refresh:** Auto")
    st.sidebar.markdown("• **Manual Refresh:** Available")
    
    # Load data directly from sources with better UX
    with st.container():
        # Show a creative loading screen while data loads
        loading_placeholder = st.empty()
        
        # Create an engaging loading screen
        with loading_placeholder.container():
            st.markdown("""
            <div style='text-align: center; padding: 2rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                        border-radius: 10px; color: white; margin: 1rem 0;'>
                <h2>🚀 Loading Your MLR Dashboard</h2>
                <p style='font-size: 1.1rem; margin: 1rem 0;'>Fetching fresh data from multiple databases...</p>
                <div style='display: flex; justify-content: center; align-items: center; margin: 1rem 0;'>
                    <div style='width: 40px; height: 40px; border: 4px solid #f3f3f3; border-top: 4px solid #3498db; 
                                border-radius: 50%; animation: spin 1s linear infinite; margin-right: 1rem;'></div>
                    <span>Processing large datasets...</span>
                </div>
                <div style='background: rgba(255,255,255,0.1); padding: 1rem; border-radius: 5px; margin: 1rem 0;'>
                    <h4>💡 Did you know?</h4>
                    <p style='margin: 0.5rem 0;'>• Data is cached for 3 hours to improve performance</p>
                    <p style='margin: 0.5rem 0;'>• You can force refresh anytime using the sidebar</p>
                    <p style='margin: 0.5rem 0;'>• The dashboard processes thousands of records in real-time</p>
                </div>
            </div>
            <style>
                @keyframes spin {
                    0% { transform: rotate(0deg); }
                    100% { transform: rotate(360deg); }
                }
            </style>
            """, unsafe_allow_html=True)
        
        # Load data
        PA, GROUP_CONTRACT, CLAIMS, GROUPS, DEBIT, ACTIVE_ENROLLEE, M_PLAN, G_PLAN, PLAN, PROVIDER, BENEFIT, BEN_CODE = load_data_from_sources()
        
        # Clear loading placeholder
        loading_placeholder.empty()
    
    if all(df is not None for df in [PA, GROUP_CONTRACT, CLAIMS, GROUPS, DEBIT, ACTIVE_ENROLLEE, M_PLAN, G_PLAN, PLAN, PROVIDER, BENEFIT, BEN_CODE]):
        # Calculate MLR
        pa_merged, claims_merged = calculate_mlr(PA, GROUP_CONTRACT, CLAIMS, GROUPS, DEBIT)
        print(list(DEBIT.columns))
        
        # Check for alerts and send emails if enabled
        alerts_sent = []
        if email_alerts_enabled and email_password and (pa_merged.height > 0 or claims_merged.height > 0):
            with st.spinner("Checking for MLR alerts..."):
                alerts_sent = check_and_send_alerts(pa_merged, claims_merged, email_password)
        
        # Display alert status
        if alerts_sent:
            st.success(f"📧 {len(alerts_sent)} email alert(s) sent:")
            for alert in alerts_sent:
                st.info(f"✅ {alert}")
        elif email_alerts_enabled and email_password:
            st.info("📧 No new email alerts to send at this time.")
        
        if pa_merged.height > 0 or claims_merged.height > 0:
            st.subheader("MLR Analysis Results (PA)")
            if pa_merged.height > 0:
                # Convert to pandas for styling
                pa_df = pa_merged.to_pandas()
                
                # Create a function to highlight rows based on thresholds
                def highlight_mlr_thresholds(row):
                    mlr_value = row['MLR(PA) (%)']
                    if mlr_value >= 85:
                        return ['background-color: #ff6b6b; color: white; font-weight: bold'] * len(row)
                    elif mlr_value >= 75:
                        return ['background-color: #ffa500; color: white; font-weight: bold'] * len(row)
                    elif mlr_value >= 65:
                        return ['background-color: #ffeb3b; color: black; font-weight: bold'] * len(row)
                    return [''] * len(row)
                
                # Apply styling
                styled_pa_df = pa_df.style.apply(highlight_mlr_thresholds, axis=1)
                st.dataframe(styled_pa_df, use_container_width=True)
            else:
                st.warning("No PA MLR data available to display.")

            st.subheader("MLR Analysis Results (Claims)")
            if claims_merged.height > 0:
                # Convert to pandas for styling
                claims_df = claims_merged.to_pandas()
                
                # Create a function to highlight rows based on thresholds
                def highlight_claims_thresholds(row):
                    mlr_value = row['MLR(CLAIMS) (%)']
                    if mlr_value >= 85:
                        return ['background-color: #ff6b6b; color: white; font-weight: bold'] * len(row)
                    elif mlr_value >= 75:
                        return ['background-color: #ffa500; color: white; font-weight: bold'] * len(row)
                    elif mlr_value >= 65:
                        return ['background-color: #ffeb3b; color: black; font-weight: bold'] * len(row)
                    return [''] * len(row)
                
                # Apply styling
                styled_claims_df = claims_df.style.apply(highlight_claims_thresholds, axis=1)
                st.dataframe(styled_claims_df, use_container_width=True)
            else:
                st.warning("No Claims MLR data available to display.")
            
            

        # --- Retail MLR Section ---
        st.subheader("Retail MLR Analysis Results")
        try:
            # Call the retail MLR calculation function
            result_df, merged_plan_df = calculate_retail_mlr(
                PA, ACTIVE_ENROLLEE, M_PLAN, G_PLAN, GROUPS, PLAN
            )

            # Display result_df
            st.markdown("**Retail MLR - Individual/Plan Breakdown**")
            if result_df is not None and result_df.height > 0:
                st.dataframe(result_df.to_pandas(), use_container_width=True)
            else:
                st.info("No retail MLR (result_df) data available.")

            # Display total_retail_premium_by_plan
            st.markdown("**Total Retail Premium by Plan**")
            if merged_plan_df is not None and merged_plan_df.height > 0:
                st.dataframe(merged_plan_df.to_pandas(), use_container_width=True)
            else:
                st.info("No retail premium by plan data available.")
        except Exception as e:
            st.error(f"Error displaying retail MLR tables: {str(e)}")
            
        # --- Utilization Dashboard Section ---
        st.markdown("---")
        st.subheader("📊 Utilization Analysis Dashboard")
        
        # Initialize session state for utilization form values
        if 'selected_group' not in st.session_state:
            st.session_state.selected_group = GROUPS.select('groupname').unique().to_series().to_list()[0] if GROUPS.height > 0 else None
        if 'start_date' not in st.session_state:
            st.session_state.start_date = date.today() - timedelta(days=30)
        if 'end_date' not in st.session_state:
            st.session_state.end_date = date.today()
        if 'money_paid' not in st.session_state:
            st.session_state.money_paid = 0.0
        if 'plan_count' not in st.session_state:
            st.session_state.plan_count = 1
        if 'selected_plan_types' not in st.session_state:
            st.session_state.selected_plan_types = []
        if 'report_generated' not in st.session_state:
            st.session_state.report_generated = False
        
        # Input Parameters Section
        st.markdown("### 📝 Input Parameters")
        st.info("Please enter the basic information for your utilization report.")
        
        # Create input section
        col1, col2 = st.columns(2)
        
        with col1:
            # Group selection dropdown
            unique_groups = GROUPS.select('groupname').unique().to_series().to_list()
            selected_group = st.selectbox(
                "Select Group",
                options=unique_groups,
                index=unique_groups.index(st.session_state.selected_group) if st.session_state.selected_group in unique_groups else 0,
                help="Choose a group to analyze",
                key="group_selector",
                on_change=lambda: setattr(st.session_state, 'selected_group', st.session_state.group_selector)
            )
            
            # Date range selection
            start_date = st.date_input(
                "Report Start Date",
                value=st.session_state.start_date,
                help="Select start date for the report period",
                key="start_date_input",
                on_change=lambda: setattr(st.session_state, 'start_date', st.session_state.start_date_input)
            )
            
            end_date = st.date_input(
                "Report End Date",
                value=st.session_state.end_date,
                help="Select end date for the report period",
                key="end_date_input",
                on_change=lambda: setattr(st.session_state, 'end_date', st.session_state.end_date_input)
            )
        
        with col2:
            # Manual money paid input
            money_paid = st.number_input(
                "Total Money Paid (₦)",
                min_value=0.0,
                value=st.session_state.money_paid,
                step=1000.0,
                help="Enter the total amount paid for this group",
                key="money_paid_input",
                on_change=lambda: setattr(st.session_state, 'money_paid', st.session_state.money_paid_input)
            )
            
            # Manual plan count input
            plan_count = st.number_input(
                "Number of Plans",
                min_value=1,
                max_value=10,
                value=st.session_state.plan_count,
                step=1,
                help="Enter the number of plans for this group",
                key="plan_count_input",
                on_change=lambda: setattr(st.session_state, 'plan_count', st.session_state.plan_count_input)
            )
            
            # Plan type selection based on user input
            plan_types = ["Bronze", "Silver", "Gold", "Gold Plus", "Platinum"]
            
            if st.session_state.plan_count > 0:
                st.write(f"**Select {st.session_state.plan_count} Plan Type(s):**")
                
                # Create multiple selectboxes for each plan
                selected_plan_types = []
                for i in range(st.session_state.plan_count):
                    # Get available plan types (exclude already selected ones)
                    available_plan_types = [pt for pt in plan_types if pt not in selected_plan_types]
                    
                    if available_plan_types:
                        # Use existing selection if available
                        if i < len(st.session_state.selected_plan_types) and st.session_state.selected_plan_types[i] in available_plan_types:
                            default_index = available_plan_types.index(st.session_state.selected_plan_types[i])
                        else:
                            default_index = 0
                        
                        plan_type = st.selectbox(
                            f"Plan {i+1}",
                            options=available_plan_types,
                            index=default_index,
                            key=f"plan_type_{i}",
                            help=f"Select plan type for plan {i+1}"
                        )
                        selected_plan_types.append(plan_type)
                    else:
                        st.warning(f"No more plan types available for plan {i+1}")
                        break
                
                # Update session state for plan types
                st.session_state.selected_plan_types = selected_plan_types
                
                # Display selected plan types
                if selected_plan_types:
                    st.success(f"Selected Plans: {', '.join(selected_plan_types)}")
            else:
                st.session_state.selected_plan_types = []
        
        # Submit button
        st.markdown("---")
        submitted = st.button("Generate Utilization Report", type="primary", use_container_width=False)
        
        # Handle submission
        if submitted:
            # Validate inputs first
            if st.session_state.start_date > st.session_state.end_date:
                st.error("Start date cannot be after end date!")
            elif st.session_state.plan_count <= 0:
                st.error("Number of plans must be greater than 0!")
            else:
                # Set report generated flag
                st.session_state.report_generated = True
        
        st.markdown("---")
        
        # Report Header Section - Only show if report was generated
        if st.session_state.report_generated:
            st.subheader("📊 Utilization Report")
            
            # Get current values from session state
            current_group = st.session_state.selected_group
            current_start_date = st.session_state.start_date
            current_end_date = st.session_state.end_date
            current_money_paid = st.session_state.money_paid
            current_plan_count = st.session_state.plan_count
            current_plan_types = st.session_state.selected_plan_types
            
            # Report header with basic information
            st.markdown("---")
            
            # Streamlit-native layout for report header
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"""
                <h3 style='color: #1f77b4; margin-bottom: 10px;'>🏢 <strong>{current_group.upper()}</strong></h3>
                <p><strong>Duration:</strong> {current_start_date.strftime('%B %d, %Y')} to {current_end_date.strftime('%B %d, %Y')}</p>
                <p><strong>Report Generated:</strong> {datetime.now().strftime('%B %d, %Y at %I:%M %p')}</p>
                """, unsafe_allow_html=True)
            with col2:
                st.markdown(f"""
                <p><strong>💰 Total Money Paid:</strong> <span style='color: #27ae60;'>₦{current_money_paid:,.2f}</span></p>
                <p><strong>📋 Number of Plans:</strong> <span style='color: #8e44ad;'>{current_plan_count}</span></p>
                <p><strong>🏆 Plan Type:</strong> <span style='color: #e67e22;'>{', '.join(current_plan_types)}</span></p>
                """, unsafe_allow_html=True)
            st.markdown("---")
            
            # Summary section
            st.subheader("📊 Summary")
            
            summary_col1, summary_col2 = st.columns(2)
            
            with summary_col1:
                st.info(f"""
                **Selected Group:** {current_group}
                
                **Analysis Period:** {current_start_date} to {current_end_date}
                
                **Total Money Paid:** ₦{current_money_paid:,.2f}
                
                **Number of Plans:** {current_plan_count}
                """)
            
            with summary_col2:
                if current_plan_types:
                    st.success(f"""
                    **Selected Plan Types:** {', '.join(current_plan_types)}
                    
                    **Plan Count:** {current_plan_count}
                    
                    **Available Plan Types:** Bronze, Silver, Gold, Gold Plus, Platinum
                    """)
                else:
                    st.warning("No plan type selected")
            
            # Pie Charts Section
            st.markdown("---")
            st.subheader("📊 Utilization Analysis - Pie Charts")
            
            try:
                # 1. Filter PA data by selected group and date period
                start_datetime = pd.Timestamp(current_start_date)
                end_datetime = pd.Timestamp(current_end_date)
                
                pa_filtered = PA.filter(
                    (pl.col('groupname') == current_group) &
                    (pl.col('requestdate') >= start_datetime) &
                    (pl.col('requestdate') <= end_datetime)
                )
                
                # Calculate total PA granted amount
                total_pa_granted = pa_filtered.select(pl.col('granted').sum()).item()
                
                # 2. Prepare debit data for the selected group and date period
                debit_pandas = DEBIT.to_pandas()
                debit_pandas['From'] = pd.to_datetime(debit_pandas['From']) 
                
                debit_filtered = debit_pandas[
                    (debit_pandas['CompanyName'] == current_group) &
                    (debit_pandas['From'] >= start_datetime) &
                    (debit_pandas['From'] <= end_datetime)
                ]
                
                total_debit_amount = debit_filtered['Amount'].sum() 
                
                # 3. Prepare Claims data filtered for selected group and period (used for charts and downloads)
                claims_with_group_for_download = CLAIMS.filter(
                    (pl.col('nhisgroupid') != "") & 
                    (pl.col('nhisgroupid').is_not_null())
                ).with_columns(
                    pl.col('nhisgroupid').cast(pl.Int64)
                ).join(
                    GROUPS.select(['groupid', 'groupname']),
                    left_on='nhisgroupid',
                    right_on='groupid',
                    how='left'
                )
                claims_filtered_for_download = claims_with_group_for_download.filter(
                    (pl.col('groupname') == current_group) &
                    (pl.col('encounterdatefrom') >= start_datetime) &
                    (pl.col('encounterdatefrom') <= end_datetime)
                )

                # Create pie charts
                col1, col2 = st.columns(2)
                
                with col1:
                    # Pie Chart 1: PA Granted vs Remaining Debit Amount
                    if total_pa_granted > 0 or total_debit_amount > 0:
                        # Calculate remaining debit amount (total debit - PA granted)
                        remaining_debit = max(0, total_debit_amount - total_pa_granted)
                        
                        fig1 = go.Figure(data=[go.Pie(
                            labels=['PA Granted', 'Remaining Debit'],
                            values=[total_pa_granted, remaining_debit],
                            hole=0.3,
                            marker_colors=['#FF6B6B', '#4ECDC4']
                        )])
                        
                        # Calculate percentage
                        pa_percentage = (total_pa_granted / total_debit_amount * 100) if total_debit_amount > 0 else 0
                        
                        fig1.update_layout(
                            title=f"PA Utilization vs Debit Amount<br><sub>{current_group} | {current_start_date} to {current_end_date}</sub>",
                            height=400,
                            showlegend=True
                        )
                        
                        st.plotly_chart(fig1, use_container_width=True)
                        
                        # Display values and percentage
                        st.info(f"""
                        **PA Granted:** ₦{total_pa_granted:,.2f} ({pa_percentage:.1f}% of total debit)
                        **Total Debit:** ₦{total_debit_amount:,.2f}
                        **Remaining:** ₦{remaining_debit:,.2f}
                        """)
                    else:
                        st.warning("No data available for PA vs Debit comparison")
                
                with col2:
                    # Pie Chart 2: PA Granted vs Remaining Money Paid
                    if total_pa_granted > 0 or current_money_paid > 0:
                        # Calculate remaining money paid (total money paid - PA granted)
                        remaining_money = max(0, current_money_paid - total_pa_granted)
                        
                        fig2 = go.Figure(data=[go.Pie(
                            labels=['PA Granted', 'Remaining Money'],
                            values=[total_pa_granted, remaining_money], 
                            hole=0.3,
                            marker_colors=['#FF6B6B', '#45B7D1']  
                        )])
                        
                        # Calculate percentage
                        pa_percentage_money = (total_pa_granted / current_money_paid * 100) if current_money_paid > 0 else 0
                        
                        fig2.update_layout(
                            title=f"PA Utilization vs Money Paid<br><sub>{current_group} | {current_start_date} to {current_end_date}</sub>",
                            height=400,
                            showlegend=True
                        )
                        
                        st.plotly_chart(fig2, use_container_width=True) 
                        
                        # Display values and percentage
                        st.info(f"""
                        **PA Granted:** ₦{total_pa_granted:,.2f} ({pa_percentage_money:.1f}% of money paid)
                        **Total Money Paid:** ₦{current_money_paid:,.2f}
                        **Remaining:** ₦{remaining_money:,.2f}
                        """)
                    else:
                        st.warning("No data available for PA vs Money Paid comparison")
                
                # Claims Analysis Pie Charts
                st.markdown("---")
                st.subheader("🏥 Claims Analysis - Pie Charts")
                
                try:
                    # 3. Prepare Claims data with groupname by merging with groups
                    claims_with_group = claims_with_group_for_download
                    # Filter Claims data by selected group and date period
                    claims_filtered = claims_filtered_for_download
                    
                    # Calculate total claims approved amount
                    total_claims_amount = claims_filtered.select(pl.col('approvedamount').sum()).item()
                    
                    # Create claims pie charts
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        # Pie Chart 3: Claims Amount vs Debit Amount
                        if total_claims_amount > 0 or total_debit_amount > 0:
                            remaining_debit_claims = max(0, total_debit_amount - total_claims_amount)
                            
                            fig3 = go.Figure(data=[go.Pie(
                                labels=['Claims Amount', 'Remaining Debit'],
                                values=[total_claims_amount, remaining_debit_claims],
                                hole=0.3,
                                marker_colors=['#FF9F43', '#4ECDC4']
                            )])
                            
                            claims_percentage_debit = (total_claims_amount / total_debit_amount * 100) if total_debit_amount > 0 else 0
                            
                            fig3.update_layout(
                                title=f"Claims vs Debit Amount<br><sub>{current_group} | {current_start_date} to {current_end_date}</sub>",
                                height=400,
                                showlegend=True
                            )
                            
                            st.plotly_chart(fig3, use_container_width=True)
                            
                            st.info(f"""
                            **Claims Amount:** ₦{total_claims_amount:,.2f} ({claims_percentage_debit:.1f}% of total debit)
                            **Total Debit:** ₦{total_debit_amount:,.2f}
                            **Remaining:** ₦{remaining_debit_claims:,.2f}
                            """)
                        else:
                            st.warning("No data available for Claims vs Debit comparison")
                    
                    with col2:
                        # Pie Chart 4: Claims Amount vs Money Paid
                        if total_claims_amount > 0 or current_money_paid > 0:
                            remaining_money_claims = max(0, current_money_paid - total_claims_amount)
                            
                            fig4 = go.Figure(data=[go.Pie(
                                labels=['Claims Amount', 'Remaining Money'],
                                values=[total_claims_amount, remaining_money_claims],
                                hole=0.3,
                                marker_colors=['#FF9F43', '#45B7D1']
                            )])
                            
                            claims_percentage_money = (total_claims_amount / current_money_paid * 100) if current_money_paid > 0 else 0
                            
                            fig4.update_layout(
                                title=f"Claims vs Money Paid<br><sub>{current_group} | {current_start_date} to {current_end_date}</sub>",
                                height=400,
                                showlegend=True
                            )
                            
                            st.plotly_chart(fig4, use_container_width=True)
                            
                            st.info(f"""
                            **Claims Amount:** ₦{total_claims_amount:,.2f} ({claims_percentage_money:.1f}% of money paid)
                            **Total Money Paid:** ₦{current_money_paid:,.2f}
                            **Remaining:** ₦{remaining_money_claims:,.2f}
                            """)
                        else:
                            st.warning("No data available for Claims vs Money Paid comparison")
                    
                    with col3:
                        # Pie Chart 5: Claims Amount vs PA Amount
                        if total_claims_amount > 0 or total_pa_granted > 0:
                            if total_claims_amount >= total_pa_granted:
                                remaining_pa = max(0, total_claims_amount - total_pa_granted)
                                fig5 = go.Figure(data=[go.Pie(
                                    labels=['PA Amount', 'Remaining Claims'],
                                    values=[total_pa_granted, remaining_pa],
                                    hole=0.3,
                                    marker_colors=['#FF6B6B', '#FF9F43']
                                )])
                                pa_percentage_claims = (total_pa_granted / total_claims_amount * 100) if total_claims_amount > 0 else 0
                                title_text = f"PA vs Claims Amount<br><sub>{current_group} | {current_start_date} to {current_end_date}</sub>"
                                info_text = f"""
                                **PA Amount:** ₦{total_pa_granted:,.2f} ({pa_percentage_claims:.1f}% of claims)
                                **Total Claims:** ₦{total_claims_amount:,.2f}
                                **Remaining:** ₦{remaining_pa:,.2f}
                                """
                            else:
                                remaining_claims = max(0, total_pa_granted - total_claims_amount)
                                fig5 = go.Figure(data=[go.Pie(
                                    labels=['Claims Amount', 'Remaining PA'],
                                    values=[total_claims_amount, remaining_claims],
                                    hole=0.3,
                                    marker_colors=['#FF9F43', '#FF6B6B']
                                )])
                                claims_percentage_pa = (total_claims_amount / total_pa_granted * 100) if total_pa_granted > 0 else 0
                                title_text = f"Claims vs PA Amount<br><sub>{current_group} | {current_start_date} to {current_end_date}</sub>"
                                info_text = f"""
                                **Claims Amount:** ₦{total_claims_amount:,.2f} ({claims_percentage_pa:.1f}% of PA)
                                **Total PA:** ₦{total_pa_granted:,.2f}
                                **Remaining:** ₦{remaining_claims:,.2f}
                                """
                            
                            fig5.update_layout(
                                title=title_text,
                                height=400,
                                showlegend=True
                            )
                            
                            st.plotly_chart(fig5, use_container_width=True)
                            
                            st.info(info_text)
                        else:
                            st.warning("No data available for Claims vs PA comparison")
                
                except Exception as e:
                    st.error(f"Error creating claims pie charts: {str(e)}")
                    st.info("Please check if the claims data contains the required columns and values.")
                
            except Exception as e:
                st.error(f"Error creating pie charts: {str(e)}")
                st.info("Please check if the data contains the required columns and values.")
            
            # --- Download Raw Data Section ---
            st.markdown("---")
            st.subheader("⬇️ Download Raw Utilization Data")
            try:
                # Convert filtered datasets to pandas for export
                pa_filtered_pd = pa_filtered.to_pandas() if pa_filtered is not None else pd.DataFrame()
                claims_filtered_pd = claims_filtered_for_download.to_pandas() if 'claims_filtered_for_download' in locals() else pd.DataFrame()
                debit_filtered_pd = debit_filtered.copy() if 'debit_filtered' in locals() else pd.DataFrame()

                # Prepare filenames
                start_str = current_start_date.strftime('%Y-%m-%d')
                end_str = current_end_date.strftime('%Y-%m-%d')
                safe_group = (current_group or 'group').replace('/', '_')

                pa_name = f"PA_{safe_group}_{start_str}_to_{end_str}.csv"
                claims_name = f"CLAIMS_{safe_group}_{start_str}_to_{end_str}.csv"
                debit_name = f"DEBIT_{safe_group}_{start_str}_to_{end_str}.csv"
                zip_name = f"UTILIZATION_RAW_{safe_group}_{start_str}_to_{end_str}.zip"

                # Build ZIP in-memory
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
                    zf.writestr(pa_name, pa_filtered_pd.to_csv(index=False))
                    zf.writestr(claims_name, claims_filtered_pd.to_csv(index=False))
                    zf.writestr(debit_name, debit_filtered_pd.to_csv(index=False))
                zip_bytes = zip_buffer.getvalue()

                # Single ZIP download button
                st.download_button(
                    label="Download Raw Utilization Data (ZIP)",
                    data=zip_bytes,
                    file_name=zip_name,
                    mime='application/zip'
                )

                # Optional: individual CSV downloads
                with st.expander("Download individual datasets (CSV)"):
                    col_csv1, col_csv2, col_csv3 = st.columns(3)
                    with col_csv1:
                        st.download_button(
                            label="Download PA CSV",
                            data=pa_filtered_pd.to_csv(index=False),
                            file_name=pa_name,
                            mime='text/csv'
                        )
                    with col_csv2:
                        st.download_button(
                            label="Download Claims CSV",
                            data=claims_filtered_pd.to_csv(index=False),
                            file_name=claims_name,
                            mime='text/csv'
                        )
                    with col_csv3:
                        st.download_button(
                            label="Download Debit CSV",
                            data=debit_filtered_pd.to_csv(index=False),
                            file_name=debit_name,
                            mime='text/csv'
                        )
            except Exception as e:
                st.error(f"Error preparing raw data downloads: {str(e)}")

            # Additional Analysis Sections
            st.markdown("---")
            st.subheader("🔍 Additional Analysis Sections")
            
            tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
                "📈 Utilization Trends", 
                "🩺 Chronic Disease Management", 
                "🏥 Surgeries", 
                "🤱 Maternity", 
                "🦷 Dental", 
                "👁️ Optical"
            ])
            
            with tab1:
                # Utilization Trends - Metrics Cards
                try:
                    # Filter PA data by selected group and date period
                    pa_filtered = PA.filter(
                        (pl.col('groupname') == current_group) &
                        (pl.col('requestdate') >= start_datetime) &
                        (pl.col('requestdate') <= end_datetime)
                    )

                    # Merge pa with provider to get providername
                    pa_with_provider = pa_filtered.join(
                        PROVIDER.select(['providertin', 'providername']),
                        left_on='providerid',
                        right_on='providertin',
                        how='left'
                    )

                    # Merge benefit and ben_code to get benefitcodedesc into ben_code
                    ben_code_with_desc = BEN_CODE.join(
                        BENEFIT.select(['benefitcodeid', 'benefitcodedesc']),
                        on='benefitcodeid',
                        how='left'
                    )
                    
                    # Merge ben_code_with_desc with pa to get benefitcodedesc into pa as 'benefit'
                    pa_with_benefit = pa_with_provider.join(
                        ben_code_with_desc.select(['procedurecode', 'benefitcodedesc']),
                        left_on='code',
                        right_on='procedurecode',
                        how='left'
                    ).with_columns([
                        pl.col('benefitcodedesc').alias('benefit')
                    ])

                    # Metrics calculations
                    total_pa_granted = pa_with_benefit.select(pl.col('granted').sum()).item()
                    total_unique_customers = pa_with_benefit.select(pl.col('IID').n_unique()).item()
                    total_visits = pa_with_benefit.height
                    
                    # Benefit with highest cost
                    benefit_costs = pa_with_benefit.group_by('benefit').agg(pl.col('granted').sum().alias('total_granted'))
                    if benefit_costs.height > 0:
                        top_benefit_row = benefit_costs.sort('total_granted', descending=True).row(0)
                        top_benefit = top_benefit_row[0] if top_benefit_row[0] is not None else 'N/A'
                        top_benefit_amount = top_benefit_row[1] if top_benefit_row[1] is not None else 0
                    else:
                        top_benefit = 'N/A'
                        top_benefit_amount = 0
                    
                    # Total unique providers
                    total_unique_providers = pa_with_benefit.select(pl.col('providername').n_unique()).item()

                    # Top provider by cost
                    provider_costs = pa_with_benefit.group_by('providername').agg(pl.col('granted').sum().alias('total_granted'))
                    if provider_costs.height > 0:
                        top_provider_row = provider_costs.sort('total_granted', descending=True).row(0)
                        top_provider = top_provider_row[0] if top_provider_row[0] is not None else 'N/A'
                        top_provider_amount = top_provider_row[1] if top_provider_row[1] is not None else 0
                    else:
                        top_provider = 'N/A'
                        top_provider_amount = 0

                    # Display as cards
                    card1, card2, card3, card4, card5, card6 = st.columns(6)
                    card1.metric("Total PA Granted", f"₦{total_pa_granted:,.2f}")
                    card2.metric("Unique Customers", f"{total_unique_customers}")
                    card3.metric("Total Visits", f"{total_visits}")
                    
                    # Top Benefit card with smaller, wrapped font for benefit name
                    card4.markdown(f"""
                        <div style='display: flex; flex-direction: column; align-items: flex-start;'>
                            <span style='font-size: 0.85rem; word-break: break-word; color: #333;'><b>Top Benefit (₦)</b></span>
                            <span style='font-size: 0.85rem; word-break: break-word; color: #888;'>{top_benefit}</span>
                            <span style='font-size: 1.1rem; color: #1f77b4;'><b>₦{top_benefit_amount:,.2f}</b></span>
                        </div>
                    """, unsafe_allow_html=True)
                    
                    card5.metric("Unique Providers", f"{total_unique_providers}")
                    
                    # Top Provider card with smaller, wrapped font for provider name
                    card6.markdown(f"""
                        <div style='display: flex; flex-direction: column; align-items: flex-start;'>
                            <span style='font-size: 0.85rem; word-break: break-word; color: #333;'><b>Top Provider (₦)</b></span>
                            <span style='font-size: 0.85rem; word-break: break-word; color: #888;'>{top_provider}</span>
                            <span style='font-size: 1.1rem; color: #1f77b4;'><b>₦{top_provider_amount:,.2f}</b></span>
                        </div>
                    """, unsafe_allow_html=True)

                    # Line chart: PA granted per month
                    pa_with_benefit = pa_with_benefit.with_columns([
                        pl.col('requestdate').dt.strftime('%Y-%m').alias('month')
                    ])
                    monthly_pa = pa_with_benefit.group_by('month').agg(pl.col('granted').sum().alias('total_granted')).sort('month')
                    if monthly_pa.height > 0:
                        st.markdown("#### PA Granted per Month")
                        st.line_chart(
                            data=monthly_pa.to_pandas().set_index('month')['total_granted'],
                            use_container_width=True
                        )
                    else:
                        st.info("No PA data available for the selected period.")

                    # Tables Section
                    st.markdown("---")
                    st.markdown("### Top 10 Customers by PA Cost")
                    # Top 10 customers by cost
                    top_customers = pa_with_benefit.group_by('IID').agg([
                        pl.col('granted').sum().alias('total_cost'),
                        pl.col('panumber').n_unique().alias('num_visits')
                    ]).with_columns([
                        (pl.col('total_cost') / pl.col('num_visits')).alias('avg_cost_per_visit')
                    ]).sort('total_cost', descending=True).head(10)
                    st.dataframe(
                        top_customers.to_pandas().rename(columns={
                            'IID': 'Customer ID',
                            'total_cost': 'Total Cost',
                            'num_visits': 'Number of Visits',
                            'avg_cost_per_visit': 'Avg Cost per Visit'
                        }),
                        use_container_width=True
                    )

                    st.markdown("### Top 10 Providers by PA Cost")
                    # Top 10 providers by cost
                    top_providers = pa_with_benefit.group_by('providername').agg([
                        pl.col('granted').sum().alias('total_cost'),
                        pl.col('IID').n_unique().alias('unique_customers')
                    ]).sort('total_cost', descending=True).head(10)
                    st.dataframe(
                        top_providers.to_pandas().rename(columns={
                            'IID': 'Provider ID',
                            'providername': 'Provider Name',
                            'total_cost': 'Total Cost',
                            'unique_customers': 'Unique Customers'
                        }),
                        use_container_width=True
                    )

                    st.markdown("### Top 5 Benefits by PA Cost")
                    # Top 5 benefits by cost
                    top_benefits = pa_with_benefit.group_by('benefit').agg([
                        pl.col('granted').sum().alias('total_cost')
                    ]).sort('total_cost', descending=True).head(5)
                    st.dataframe(
                        top_benefits.to_pandas().rename(columns={
                            'benefit': 'Benefit',
                            'total_cost': 'Total Cost'
                        }),
                        use_container_width=True
                    )

                except Exception as e:
                    st.error(f"Error generating utilization trend cards: {str(e)}")
                
            with tab2:
                try:
                    # Get chronic disease procedure codes from CSV
                    chronic_codes = get_procedure_codes_for_benefit_group('chronic medication')
                    
                    if not chronic_codes:
                        st.warning("No chronic medication codes found in benefits_cleaned.csv")
                        chronic_codes = []

                    # Column to use for procedure code
                    proc_col = 'procedurecode' if 'procedurecode' in PA.columns else 'code'

                    # Filter PA by selected group, date range, and chronic codes
                    pa_cd = PA
                    if proc_col == 'code':
                        pa_cd = pa_cd.with_columns(pl.col(proc_col).str.to_lowercase())
                    else:
                        pa_cd = pa_cd.with_columns(pl.col(proc_col).str.to_lowercase())

                    pa_cd = pa_cd.filter(
                        (pl.col('groupname') == current_group) &
                        (pl.col('requestdate') >= pd.Timestamp(current_start_date)) &
                        (pl.col('requestdate') <= pd.Timestamp(current_end_date)) &
                        (pl.col(proc_col).is_in(chronic_codes))
                    )

                    # Ensure numeric type for granted
                    if 'granted' in pa_cd.columns:
                        pa_cd = pa_cd.with_columns(pl.col('granted').cast(pl.Float64, strict=False))

                    st.markdown("### 🩺 Chronic Disease Management")

                    # Metrics: unique customers and total cost
                    unique_customers = pa_cd.select(pl.col('IID').n_unique()).item() if 'IID' in pa_cd.columns else 0
                    total_cd_cost = pa_cd.select(pl.col('granted').sum()).item() if 'granted' in pa_cd.columns else 0.0

                    m1, m2 = st.columns(2)
                    m1.metric("Customers with Chronic Conditions", f"{int(unique_customers):,}")
                    m2.metric("Total Chronic Disease Cost", f"₦{(total_cd_cost or 0):,.2f}")

                    # Monthly cost line chart
                    if pa_cd.height > 0:
                        pa_cd_monthly = pa_cd.with_columns([
                            pl.col('requestdate').dt.strftime('%Y-%m').alias('month')
                        ]).group_by('month').agg(
                            pl.col('granted').sum().alias('total_cost')
                        ).sort('month')

                        st.markdown("#### Monthly Chronic Disease Cost")
                        st.line_chart(
                            data=pa_cd_monthly.to_pandas().set_index('month')['total_cost'],
                            use_container_width=True
                        )
                        
                        # Merge with provider to get providername
                        pa_cd_with_provider = pa_cd.join(
                            PROVIDER.select(['providertin', 'providername']),
                            left_on='providerid',
                            right_on='providertin',
                            how='left'
                        )
                        
                        st.markdown("---")
                        st.markdown("### Top 10 Customers by Chronic Disease Cost")
                        # Top 10 customers by cost
                        top_customers = pa_cd_with_provider.group_by('IID').agg([
                            pl.col('granted').sum().alias('total_cost'),
                            pl.col('panumber').n_unique().alias('num_visits')
                        ]).with_columns([
                            (pl.col('total_cost') / pl.col('num_visits')).alias('avg_cost_per_visit')
                        ]).sort('total_cost', descending=True).head(10)
                        st.dataframe(
                            top_customers.to_pandas().rename(columns={
                                'IID': 'Customer ID',
                                'total_cost': 'Total Cost',
                                'num_visits': 'Number of Visits',
                                'avg_cost_per_visit': 'Avg Cost per Visit'
                            }),
                            use_container_width=True
                        )

                        st.markdown("### Top 10 Providers by Chronic Disease Cost")
                        # Top 10 providers by cost
                        top_providers = pa_cd_with_provider.group_by('providername').agg([
                            pl.col('granted').sum().alias('total_cost'),
                            pl.col('IID').n_unique().alias('unique_customers')
                        ]).sort('total_cost', descending=True).head(10)
                        st.dataframe(
                            top_providers.to_pandas().rename(columns={
                                'providername': 'Provider Name',
                                'total_cost': 'Total Cost',
                                'unique_customers': 'Unique Customers'
                            }),
                            use_container_width=True
                        )
                    else:
                        st.info("No chronic disease utilization found for the selected period.")

                except Exception as e:
                    st.error(f"Error generating Chronic Disease Management section: {str(e)}")
            
            with tab3:
                try:
                    # Get surgery procedure codes from CSV
                    surgery_codes = get_procedure_codes_for_benefit_group('surgeries')
                    
                    if not surgery_codes:
                        st.warning("No surgery codes found in benefits_cleaned.csv")
                        surgery_codes = []
                    
                    # Column to use for procedure code
                    proc_col = 'procedurecode' if 'procedurecode' in PA.columns else 'code'
                    
                    # Filter PA by selected group, date range, and surgery codes
                    pa_cd = PA
                    if proc_col == 'code':
                        pa_cd = pa_cd.with_columns(pl.col(proc_col).str.to_lowercase())
                    else:
                        pa_cd = pa_cd.with_columns(pl.col(proc_col).str.to_lowercase())
                    
                    pa_surgery = pa_cd.filter(
                        (pl.col('groupname') == current_group) &
                        (pl.col('requestdate') >= start_datetime) &
                        (pl.col('requestdate') <= end_datetime) &
                        (pl.col(proc_col).is_in(surgery_codes))
                    )
                    
                    if len(pa_surgery) > 0:
                        # Calculate surgery utilization metrics
                        surgery_cost = pa_surgery['granted'].sum()
                        surgery_count = len(pa_surgery)
                        surgery_avg_cost = surgery_cost / surgery_count if surgery_count > 0 else 0
                        
                        # Display metrics
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Surgery Cost", f"₦{surgery_cost:,.2f}")
                        with col2:
                            st.metric("Surgery Count", f"{surgery_count:,}")
                        with col3:
                            st.metric("Average Surgery Cost", f"₦{surgery_avg_cost:,.2f}")
                        
                        # Top surgery procedures
                        if 'procedurecode' in pa_surgery.columns:
                            top_surgeries = pa_surgery.group_by('procedurecode').agg([
                                pl.col('granted').sum().alias('total_cost'),
                                pl.col('granted').count().alias('count')
                            ]).sort('total_cost', descending=True).head(10)
                            
                            st.subheader("Top Surgery Procedures by Cost")
                            st.dataframe(
                                top_surgeries.to_pandas().rename(columns={
                                    'procedurecode': 'Procedure Code',
                                    'total_cost': 'Total Cost',
                                    'count': 'Count'
                                }),
                                use_container_width=True
                            )
                        
                        # Monthly cost line chart
                        if pa_surgery.height > 0:
                            pa_surgery_monthly = pa_surgery.with_columns([
                                pl.col('requestdate').dt.strftime('%Y-%m').alias('month')
                            ]).group_by('month').agg(
                                pl.col('granted').sum().alias('total_cost')
                            ).sort('month')

                            st.markdown("#### Monthly Surgery Cost")
                            st.line_chart(
                                data=pa_surgery_monthly.to_pandas().set_index('month')['total_cost'],
                                use_container_width=True
                            )
                        
                        # Merge with provider to get providername
                        pa_surgery_with_provider = pa_surgery.join(
                            PROVIDER.select(['providertin', 'providername']),
                            left_on='providerid',
                            right_on='providertin',
                            how='left'
                        )
                        
                        st.markdown("---")
                        st.markdown("### Top 10 Customers by Surgery Cost")
                        # Top 10 customers by cost
                        top_customers = pa_surgery_with_provider.group_by('IID').agg([
                            pl.col('granted').sum().alias('total_cost'),
                            pl.col('panumber').n_unique().alias('num_visits')
                        ]).with_columns([
                            (pl.col('total_cost') / pl.col('num_visits')).alias('avg_cost_per_visit')
                        ]).sort('total_cost', descending=True).head(10)
                        st.dataframe(
                            top_customers.to_pandas().rename(columns={
                                'IID': 'Customer ID',
                                'total_cost': 'Total Cost',
                                'num_visits': 'Number of Visits',
                                'avg_cost_per_visit': 'Avg Cost per Visit'
                            }),
                            use_container_width=True
                        )

                        st.markdown("### Top 10 Providers by Surgery Cost")
                        # Top 10 providers by cost
                        top_providers = pa_surgery_with_provider.group_by('providername').agg([
                            pl.col('granted').sum().alias('total_cost'),
                            pl.col('IID').n_unique().alias('unique_customers')
                        ]).sort('total_cost', descending=True).head(10)
                        st.dataframe(
                            top_providers.to_pandas().rename(columns={
                                'providername': 'Provider Name',
                                'total_cost': 'Total Cost',
                                'unique_customers': 'Unique Customers'
                            }),
                            use_container_width=True
                        )
                    else:
                        st.info("No surgery utilization found for the selected period.")
                        
                except Exception as e:
                    st.error(f"Error generating Surgery section: {str(e)}")
            
            with tab4:
                try:
                    # Get maternity procedure codes from CSV
                    maternity_codes = get_procedure_codes_for_benefit_group('maternity')
                    
                    if not maternity_codes:
                        st.warning("No maternity codes found in benefits_cleaned.csv")
                        maternity_codes = []
                    
                    # Column to use for procedure code
                    proc_col = 'procedurecode' if 'procedurecode' in PA.columns else 'code'
                    
                    # Filter PA by selected group, date range, and maternity codes
                    pa_cd = PA
                    if proc_col == 'code':
                        pa_cd = pa_cd.with_columns(pl.col(proc_col).str.to_lowercase())
                    else:
                        pa_cd = pa_cd.with_columns(pl.col(proc_col).str.to_lowercase())
                    
                    pa_maternity = pa_cd.filter(
                        (pl.col('groupname') == current_group) &
                        (pl.col('requestdate') >= start_datetime) &
                        (pl.col('requestdate') <= end_datetime) &
                        (pl.col(proc_col).is_in(maternity_codes))
                    )
                    
                    if len(pa_maternity) > 0:
                        # Calculate maternity utilization metrics
                        maternity_cost = pa_maternity['granted'].sum()
                        maternity_count = len(pa_maternity)
                        maternity_avg_cost = maternity_cost / maternity_count if maternity_count > 0 else 0
                        
                        # Display metrics
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Maternity Cost", f"₦{maternity_cost:,.2f}")
                        with col2:
                            st.metric("Maternity Count", f"{maternity_count:,}")
                        with col3:
                            st.metric("Average Maternity Cost", f"₦{maternity_avg_cost:,.2f}")
                        
                        # Top maternity procedures
                        if 'procedurecode' in pa_maternity.columns:
                            top_maternity = pa_maternity.group_by('procedurecode').agg([
                                pl.col('granted').sum().alias('total_cost'),
                                pl.col('granted').count().alias('count')
                            ]).sort('total_cost', descending=True).head(10)
                            
                            st.subheader("Top Maternity Procedures by Cost")
                            st.dataframe(
                                top_maternity.to_pandas().rename(columns={
                                    'procedurecode': 'Procedure Code',
                                    'total_cost': 'Total Cost',
                                    'count': 'Count'
                                }),
                                use_container_width=True
                            )
                        
                        # Monthly cost line chart
                        if pa_maternity.height > 0:
                            pa_maternity_monthly = pa_maternity.with_columns([
                                pl.col('requestdate').dt.strftime('%Y-%m').alias('month')
                            ]).group_by('month').agg(
                                pl.col('granted').sum().alias('total_cost')
                            ).sort('month')

                            st.markdown("#### Monthly Maternity Cost")
                            st.line_chart(
                                data=pa_maternity_monthly.to_pandas().set_index('month')['total_cost'],
                                use_container_width=True
                            )
                        
                        # Merge with provider to get providername
                        pa_maternity_with_provider = pa_maternity.join(
                            PROVIDER.select(['providertin', 'providername']),
                            left_on='providerid',
                            right_on='providertin',
                            how='left'
                        )
                        
                        st.markdown("---")
                        st.markdown("### Top 10 Customers by Maternity Cost")
                        # Top 10 customers by cost
                        top_customers = pa_maternity_with_provider.group_by('IID').agg([
                            pl.col('granted').sum().alias('total_cost'),
                            pl.col('panumber').n_unique().alias('num_visits')
                        ]).with_columns([
                            (pl.col('total_cost') / pl.col('num_visits')).alias('avg_cost_per_visit')
                        ]).sort('total_cost', descending=True).head(10)
                        st.dataframe(
                            top_customers.to_pandas().rename(columns={
                                'IID': 'Customer ID',
                                'total_cost': 'Total Cost',
                                'num_visits': 'Number of Visits',
                                'avg_cost_per_visit': 'Avg Cost per Visit'
                            }),
                            use_container_width=True
                        )

                        st.markdown("### Top 10 Providers by Maternity Cost")
                        # Top 10 providers by cost
                        top_providers = pa_maternity_with_provider.group_by('providername').agg([
                            pl.col('granted').sum().alias('total_cost'),
                            pl.col('IID').n_unique().alias('unique_customers')
                        ]).sort('total_cost', descending=True).head(10)
                        st.dataframe(
                            top_providers.to_pandas().rename(columns={
                                'providername': 'Provider Name',
                                'total_cost': 'Total Cost',
                                'unique_customers': 'Unique Customers'
                            }),
                            use_container_width=True
                        )
                    else:
                        st.info("No maternity utilization found for the selected period.")
                        
                except Exception as e:
                    st.error(f"Error generating Maternity section: {str(e)}")
            
            with tab5:
                try:
                    # Get dental procedure codes from CSV
                    dental_codes = get_procedure_codes_for_benefit_group('dentistry')
                    
                    if not dental_codes:
                        st.warning("No dental codes found in benefits_cleaned.csv")
                        dental_codes = []
                    
                    # Column to use for procedure code
                    proc_col = 'procedurecode' if 'procedurecode' in PA.columns else 'code'
                    
                    # Filter PA by selected group, date range, and dental codes
                    pa_cd = PA
                    if proc_col == 'code':
                        pa_cd = pa_cd.with_columns(pl.col(proc_col).str.to_lowercase())
                    else:
                        pa_cd = pa_cd.with_columns(pl.col(proc_col).str.to_lowercase())
                    
                    pa_dental = pa_cd.filter(
                        (pl.col('groupname') == current_group) &
                        (pl.col('requestdate') >= start_datetime) &
                        (pl.col('requestdate') <= end_datetime) &
                        (pl.col(proc_col).is_in(dental_codes))
                    )
                    
                    if len(pa_dental) > 0:
                        # Calculate dental utilization metrics
                        dental_cost = pa_dental['granted'].sum()
                        dental_count = len(pa_dental)
                        dental_avg_cost = dental_cost / dental_count if dental_count > 0 else 0
                        
                        # Display metrics
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Dental Cost", f"₦{dental_cost:,.2f}")
                        with col2:
                            st.metric("Dental Count", f"{dental_count:,}")
                        with col3:
                            st.metric("Average Dental Cost", f"₦{dental_avg_cost:,.2f}")
                        
                        # Top dental procedures
                        if 'procedurecode' in pa_dental.columns:
                            top_dental = pa_dental.group_by('procedurecode').agg([
                                pl.col('granted').sum().alias('total_cost'),
                                pl.col('granted').count().alias('count')
                            ]).sort('total_cost', descending=True).head(10)
                            
                            st.subheader("Top Dental Procedures by Cost")
                            st.dataframe(
                                top_dental.to_pandas().rename(columns={
                                    'procedurecode': 'Procedure Code',
                                    'total_cost': 'Total Cost',
                                    'count': 'Count'
                                }),
                                use_container_width=True
                            )
                        
                        # Monthly cost line chart
                        if pa_dental.height > 0:
                            pa_dental_monthly = pa_dental.with_columns([
                                pl.col('requestdate').dt.strftime('%Y-%m').alias('month')
                            ]).group_by('month').agg(
                                pl.col('granted').sum().alias('total_cost')
                            ).sort('month')

                            st.markdown("#### Monthly Dental Cost")
                            st.line_chart(
                                data=pa_dental_monthly.to_pandas().set_index('month')['total_cost'],
                                use_container_width=True
                            )
                        
                        # Merge with provider to get providername
                        pa_dental_with_provider = pa_dental.join(
                            PROVIDER.select(['providertin', 'providername']),
                            left_on='providerid',
                            right_on='providertin',
                            how='left'
                        )
                        
                        st.markdown("---")
                        st.markdown("### Top 10 Customers by Dental Cost")
                        # Top 10 customers by cost
                        top_customers = pa_dental_with_provider.group_by('IID').agg([
                            pl.col('granted').sum().alias('total_cost'),
                            pl.col('panumber').n_unique().alias('num_visits')
                        ]).with_columns([
                            (pl.col('total_cost') / pl.col('num_visits')).alias('avg_cost_per_visit')
                        ]).sort('total_cost', descending=True).head(10)
                        st.dataframe(
                            top_customers.to_pandas().rename(columns={
                                'IID': 'Customer ID',
                                'total_cost': 'Total Cost',
                                'num_visits': 'Number of Visits',
                                'avg_cost_per_visit': 'Avg Cost per Visit'
                            }),
                            use_container_width=True
                        )

                        st.markdown("### Top 10 Providers by Dental Cost")
                        # Top 10 providers by cost
                        top_providers = pa_dental_with_provider.group_by('providername').agg([
                            pl.col('granted').sum().alias('total_cost'),
                            pl.col('IID').n_unique().alias('unique_customers')
                        ]).sort('total_cost', descending=True).head(10)
                        st.dataframe(
                            top_providers.to_pandas().rename(columns={
                                'providername': 'Provider Name',
                                'total_cost': 'Total Cost',
                                'unique_customers': 'Unique Customers'
                            }),
                            use_container_width=True
                        )
                    else:
                        st.info("No dental utilization found for the selected period.")
                        
                except Exception as e:
                    st.error(f"Error generating Dental section: {str(e)}")
            
            with tab6:
                try:
                    # Get optical procedure codes from CSV
                    optical_codes = get_procedure_codes_for_benefit_group('ophthalmic care')
                    
                    if not optical_codes:
                        st.warning("No optical codes found in benefits_cleaned.csv")
                        optical_codes = []
                    
                    # Column to use for procedure code
                    proc_col = 'procedurecode' if 'procedurecode' in PA.columns else 'code'
                    
                    # Filter PA by selected group, date range, and optical codes
                    pa_cd = PA
                    if proc_col == 'code':
                        pa_cd = pa_cd.with_columns(pl.col(proc_col).str.to_lowercase())
                    else:
                        pa_cd = pa_cd.with_columns(pl.col(proc_col).str.to_lowercase())
                    
                    pa_optical = pa_cd.filter(
                        (pl.col('groupname') == current_group) &
                        (pl.col('requestdate') >= start_datetime) &
                        (pl.col('requestdate') <= end_datetime) &
                        (pl.col(proc_col).is_in(optical_codes))
                    )
                    
                    if len(pa_optical) > 0:
                        # Calculate optical utilization metrics
                        optical_cost = pa_optical['granted'].sum()
                        optical_count = len(pa_optical)
                        optical_avg_cost = optical_cost / optical_count if optical_count > 0 else 0
                        
                        # Display metrics
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Optical Cost", f"₦{optical_cost:,.2f}")
                        with col2:
                            st.metric("Optical Count", f"{optical_count:,}")
                        with col3:
                            st.metric("Average Optical Cost", f"₦{optical_avg_cost:,.2f}")
                        
                        # Top optical procedures
                        if 'procedurecode' in pa_optical.columns:
                            top_optical = pa_optical.group_by('procedurecode').agg([
                                pl.col('granted').sum().alias('total_cost'),
                                pl.col('granted').count().alias('count')
                            ]).sort('total_cost', descending=True).head(10)
                            
                            st.subheader("Top Optical Procedures by Cost")
                            st.dataframe(
                                top_optical.to_pandas().rename(columns={
                                    'procedurecode': 'Procedure Code',
                                    'total_cost': 'Total Cost',
                                    'count': 'Count'
                                }),
                                use_container_width=True
                            )
                        
                        # Monthly cost line chart
                        if pa_optical.height > 0:
                            pa_optical_monthly = pa_optical.with_columns([
                                pl.col('requestdate').dt.strftime('%Y-%m').alias('month')
                            ]).group_by('month').agg(
                                pl.col('granted').sum().alias('total_cost')
                            ).sort('month')

                            st.markdown("#### Monthly Optical Cost")
                            st.line_chart(
                                data=pa_optical_monthly.to_pandas().set_index('month')['total_cost'],
                                use_container_width=True
                            )
                        
                        # Merge with provider to get providername
                        pa_optical_with_provider = pa_optical.join(
                            PROVIDER.select(['providertin', 'providername']),
                            left_on='providerid',
                            right_on='providertin',
                            how='left'
                        )
                        
                        st.markdown("---")
                        st.markdown("### Top 10 Customers by Optical Cost")
                        # Top 10 customers by cost
                        top_customers = pa_optical_with_provider.group_by('IID').agg([
                            pl.col('granted').sum().alias('total_cost'),
                            pl.col('panumber').n_unique().alias('num_visits')
                        ]).with_columns([
                            (pl.col('total_cost') / pl.col('num_visits')).alias('avg_cost_per_visit')
                        ]).sort('total_cost', descending=True).head(10)
                        st.dataframe(
                            top_customers.to_pandas().rename(columns={
                                'IID': 'Customer ID',
                                'total_cost': 'Total Cost',
                                'num_visits': 'Number of Visits',
                                'avg_cost_per_visit': 'Avg Cost per Visit'
                            }),
                            use_container_width=True
                        )

                        st.markdown("### Top 10 Providers by Optical Cost")
                        # Top 10 providers by cost
                        top_providers = pa_optical_with_provider.group_by('providername').agg([
                            pl.col('granted').sum().alias('total_cost'),
                            pl.col('IID').n_unique().alias('unique_customers')
                        ]).sort('total_cost', descending=True).head(10)
                        st.dataframe(
                            top_providers.to_pandas().rename(columns={
                                'providername': 'Provider Name',
                                'total_cost': 'Total Cost',
                                'unique_customers': 'Unique Customers'
                            }),
                            use_container_width=True
                        )
                    else:
                        st.info("No optical utilization found for the selected period.")
                        
                except Exception as e:
                    st.error(f"Error generating Optical section: {str(e)}")
        else:
            st.info("👆 Please fill in the parameters above and click 'Generate Utilization Report' to view the dashboard.")
    else:
        st.error("Failed to load required data. Please check your database connections and try again.")
    
    # Display notification history
    st.sidebar.subheader("📋 Notification History")
    if 'sent_notifications' in st.session_state and st.session_state.sent_notifications:
        for notification, timestamp in st.session_state.sent_notifications.items():
            st.sidebar.text(f"{notification}: {timestamp[:16]}")
    else:
        st.sidebar.text("No notifications sent yet")
    
    # Clear notification history button
    if st.sidebar.button("Clear Notification History"):
        st.session_state.sent_notifications = {}
        st.sidebar.success("Notification history cleared!")
        st.experimental_rerun()