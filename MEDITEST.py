
import pandas as pd
from dlt_sources import create_medicloud_connection

def total_procedures_df():
    """
    Fetch PA procedures table from MediCloud SQL Server and return as DataFrame.
    Only fetch data from 6 months ago from this month using requestdate.
    """
    try:
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
            txn.userid,
            txn.totaltariff,
            txn.benefitcode,
            txn.dependantnumber,
            tbp.requested,
            tbp.granted,
            COUNT(*) OVER (PARTITION BY txn.panumber) AS total_procedure
        FROM dbo.tbPATxn txn
        JOIN dbo.tbPAProcedures tbp ON txn.panumber = tbp.panumber
        WHERE txn.requestdate >= DATEFROMPARTS(YEAR(GETDATE()), 4, 1)
          AND txn.requestdate <= GETDATE();
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print(f"Successfully fetched {len(df)} rows from PA procedures table in MediCloud")
        return df
    except Exception as e:
        print(f"Error connecting to MediCloud or fetching data: {e}")
        return None

def all_active_member():
    """
    Fetch all active members from MediCloud SQL Server and return as DataFrame.
    This matches the logic in dlt_sources.py: select from member_coverage and member, with iscurrent and isterminated checks.
    """
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT
                mc.memberid,
                m.groupid,
                m.legacycode,
                m.planid,
                mc.iscurrent,
                m.isterminated,
                m.email1,
                m.email2,
                m.email3,
                m.email4,
                m.phone1,
                m.phone2,
                m.phone3,
                m.phone4,
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
        print(f"Fetched {len(df)} active members from member_coverage/member tables.")
        return df
    except Exception as e:
        print(f"Error fetching active members: {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass

def all_providers():
    """
    Fetch all providers from MediCloud SQL Server and return as DataFrame.
    This matches the logic in dlt_sources.py: join provider, providercategory, lgas, states.
    """
    try:
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
                LEFT JOIN dbo.lgas l ON p.lgaid = l.lgaid
                LEFT JOIN dbo.states s ON p.stateid = s.stateid
        """
        df = pd.read_sql(query, conn)
        print(f"Fetched {len(df)} providers from provider/providercategory/lgas/states tables.")
        return df
    except Exception as e:
        print(f"Error fetching providers: {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass

def all_group():
    """
    Fetch all groups from MediCloud SQL Server and return as DataFrame.
    This matches the logic in dlt_sources.py: select * from [group].
    """
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT * FROM dbo.[group]
        """
        df = pd.read_sql(query, conn)
        print(f"Fetched {len(df)} groups from group table.")
        return df
    except Exception as e:
        print(f"Error fetching groups: {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_group_plan():
    """
    Fetch group_plan table from MediCloud using the same logic as dlt_sources.py.
    """
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT * FROM dbo.group_plan
               WHERE iscurrent = 1
            AND CAST(terminationdate AS DATETIME) >= CAST(GETDATE() AS DATETIME)
        """
        df = pd.read_sql(query, conn)
        print(f"Fetched {len(df)} rows from group_plan table.")
        return df
    except Exception as e:
        print(f"Error fetching group_plan: {e}")
        return None
    finally:
        try:
            conn.close()
        except:
            pass

def fetch_pa_issue_request():
    """
    Fetch pa_issue_request table from MediCloud for records with EncounterDate in the last 6 months.
    """
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT Providerid, RequestDate, ResolutionTime, EncounterDate, PANumber, DateAdded
            FROM dbo.PAIssueRequest
            WHERE EncounterDate >= DATEFROMPARTS(YEAR(GETDATE()), 4, 1)
        """
        df = pd.read_sql(query, conn)
        print(f"Fetched {len(df)} rows from pa_issue_request table (last 6 months).")
        return df
    except Exception as e:
        print(f"Error fetching pa_issue_request: {e}")
        return None
    finally:
        try:
            conn.close()
        except:
            pass
def fetch_user_log():
    """
    Fetch user_log table from MediCloud (dbo.userlogin).
    """
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT * FROM dbo.userlogin
        """
        df = pd.read_sql(query, conn)
        print(f"Fetched {len(df)} rows from user_log (dbo.userlogin) table.")
        return df
    except Exception as e:
        print(f"Error fetching user_log: {e}")
        return None
    finally:
        try:
            conn.close()
        except:
            pass

def fetch_users():
    """
    Fetch users table from MediCloud (dbo.tbUsers).
    """
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT * FROM dbo.tbUsers
        """
        df = pd.read_sql(query, conn)
        print(f"Fetched {len(df)} rows from users (dbo.tbUsers) table.")
        return df
    except Exception as e:
        print(f"Error fetching users: {e}")
        return None
    finally:
        try:
            conn.close()
        except:
            pass



def build_enriched_total_procedures():
    """
    Return an enriched DataFrame built from:
    - total_procedures_df(): PA transactions and procedures (last 6 months)
    - all_providers(): used to attach providername via providerid -> providertin
    - fetch_users(): used to attach firstname via userid

    The function is defensive about column names and will search for reasonable
    alternatives when expected columns are missing.
    """
    # Load base dataframes
    procedures_df = total_procedures_df()
    providers_df = all_providers()
    users_df = fetch_users()

    if procedures_df is None or providers_df is None or users_df is None:
        print("One or more required DataFrames could not be loaded. Returning None.")
        return None

    enriched_df = procedures_df.copy()

    # Resolve provider join keys
    left_provider_key = None
    if "providerid" in enriched_df.columns:
        left_provider_key = "providerid"
    else:
        # Try common alternatives
        for candidate in ["provider_id", "ProviderId", "providerId"]:
            if candidate in enriched_df.columns:
                left_provider_key = candidate
                break

    right_provider_key = None
    for candidate in [
        "providertin",
        "providerid",
        "provider_id",
        "ProviderTIN",
        "ProviderId",
        "providerTin",
    ]:
        if candidate in providers_df.columns:
            right_provider_key = candidate 
            break

    # Resolve provider name column
    provider_name_col = None
    for candidate in ["providername", "provider_name", "ProviderName", "name", "Name"]:
        if candidate in providers_df.columns:
            provider_name_col = candidate
            break

    if left_provider_key and right_provider_key and provider_name_col:
        providers_slim = providers_df[[right_provider_key, provider_name_col]].drop_duplicates()
        providers_slim = providers_slim.rename(
            columns={provider_name_col: "providername"}
        )
        # Normalize join key types to string to avoid dtype mismatch
        try:
            enriched_df[left_provider_key] = enriched_df[left_provider_key].astype(str)
        except Exception:
            pass
        try:
            providers_slim[right_provider_key] = providers_slim[right_provider_key].astype(str)
        except Exception:
            pass
        enriched_df = enriched_df.merge(
            providers_slim,
            how="left",
            left_on=left_provider_key,
            right_on=right_provider_key,
        )
        # Drop the join helper column if it differs from left key
        if right_provider_key in enriched_df.columns and right_provider_key != left_provider_key:
            enriched_df = enriched_df.drop(columns=[right_provider_key])
    else:
        print(
            "Warning: Could not merge providers."
            f" left_key={left_provider_key}, right_key={right_provider_key}, provider_name_col={provider_name_col}"
        )

    # Use userid directly as firstname since it already contains usernames
    if "userid" in enriched_df.columns:
        enriched_df["firstname"] = enriched_df["userid"]
        print(f"Using userid as firstname: {enriched_df['firstname'].nunique()} unique names")
    else:
        print("Warning: No userid column found to use as firstname")
        enriched_df["firstname"] = None

    return enriched_df


def build_active_members_with_groupname():
    """
    Return active members joined with group info to include groupname.
    Joins all_active_member() with all_group() on groupid (with fallbacks).
    """
    members_df = all_active_member()
    groups_df = all_group()

    if members_df is None or groups_df is None:
        print("Could not load members or groups. Returning None.")
        return None

    left_key = None
    for candidate in ["groupid", "GroupID", "group_id", "GroupId"]:
        if candidate in members_df.columns:
            left_key = candidate
            break

    right_key = None
    for candidate in ["groupid", "GroupID", "group_id", "GroupId"]:
        if candidate in groups_df.columns:
            right_key = candidate
            break

    group_name_col = None
    for candidate in ["groupname", "GroupName", "group_name", "name", "Name"]:
        if candidate in groups_df.columns:
            group_name_col = candidate
            break

    if not (left_key and right_key and group_name_col):
        print(
            "Warning: Could not resolve join columns to attach groupname."
            f" left_key={left_key}, right_key={right_key}, group_name_col={group_name_col}"
        )
        return members_df

    groups_slim = groups_df[[right_key, group_name_col]].drop_duplicates()
    groups_slim = groups_slim.rename(columns={group_name_col: "groupname"})

    merged = members_df.merge(
        groups_slim,
        how="left",
        left_on=left_key,
        right_on=right_key,
    )
    if right_key in merged.columns and right_key != left_key:
        merged = merged.drop(columns=[right_key])

    return merged


def analyze_incomplete_contacts():
    """
    Analyze active members for incomplete phone numbers and emails, grouped by groupname.
    Returns DataFrame with IID, groupname, phone completeness status, and email columns for review.
    """
    # Get members with group info
    members_df = build_active_members_with_groupname()
    if members_df is None:
        print("Could not load members data.")
        return None
    
    # Ensure required columns exist
    phone_cols = ["phone1", "phone2", "phone3", "phone4"]
    email_cols = ["email1", "email2", "email3", "email4"]
    
    # Check if columns exist
    missing_phone_cols = [col for col in phone_cols if col not in members_df.columns]
    missing_email_cols = [col for col in email_cols if col not in members_df.columns]
    
    if missing_phone_cols:
        print(f"Warning: Missing phone columns: {missing_phone_cols}")
    if missing_email_cols:
        print(f"Warning: Missing email columns: {missing_email_cols}")
    
    # Create analysis DataFrame
    analysis_df = members_df[["memberid", "groupname"]].copy()
    
    # Analyze phone completeness
    def is_phone_complete(row):
        phone_values = []
        for col in phone_cols:
            if col in members_df.columns:
                val = str(row[col]).strip() if pd.notna(row[col]) else ""
                if val and val != "nan":
                    phone_values.append(val)
        
        # If all phone columns are empty
        if not phone_values:
            return False, "All phone columns empty"
        
        # Check if at least one phone number is valid (11 digits and doesn't start with "080000")
        for phone in phone_values:
            # Remove any non-digit characters for validation
            clean_phone = ''.join(filter(str.isdigit, phone))
            if len(clean_phone) == 11 and not clean_phone.startswith("080000"):
                return True, "At least one valid phone found"
        
        return False, "No valid phone numbers (must be 11 digits, not starting with 080000)"
    
    # Apply phone analysis
    phone_results = members_df.apply(is_phone_complete, axis=1, result_type='expand')
    analysis_df['phone_complete'] = phone_results[0]
    analysis_df['phone_status'] = phone_results[1]
    
    # Analyze email completeness
    def is_email_complete(row):
        email_values = []
        for col in email_cols:
            if col in members_df.columns:
                val = str(row[col]).strip() if pd.notna(row[col]) else ""
                if val and val != "nan":
                    email_values.append(val)
        
        # If all email columns are empty
        if not email_values:
            return False, "All email columns empty"
        
        # Check if at least one email is valid
        for email in email_values:
            email_lower = email.lower()
            
            # Check for invalid emails
            if (email_lower == "null" or 
                email_lower == "default@gmail.com" or 
                email_lower == "test@gmail.com" or
                "@" not in email):
                continue  # Skip this email, check next one
            
            # If we reach here, this email is valid
            return True, "At least one valid email found"
        
        return False, "No valid emails (empty, null, test/default emails, or missing @)"
    
    # Apply email analysis
    email_results = members_df.apply(is_email_complete, axis=1, result_type='expand')
    analysis_df['email_complete'] = email_results[0]
    analysis_df['email_status'] = email_results[1]
    
    # Add email columns for review
    for col in email_cols:
        if col in members_df.columns:
            analysis_df[col] = members_df[col]
    
    # Log summary counts but return FULL analysis_df for accurate group totals elsewhere
    incomplete_contacts = analysis_df[(~analysis_df['phone_complete']) | (~analysis_df['email_complete'])]
    print(f"Total members analyzed: {len(analysis_df)}")
    print(f"Members with incomplete phone numbers: {len(analysis_df[~analysis_df['phone_complete']])}")
    print(f"Members with incomplete email addresses: {len(analysis_df[~analysis_df['email_complete']])}")
    print(f"Members with incomplete contacts (phone OR email): {len(incomplete_contacts)}")
    print(f"Members with complete contacts: {len(analysis_df) - len(incomplete_contacts)}")
    
    return analysis_df


def show_incomplete_contacts_sample(n=20):
    """
    Show first n rows of members with incomplete contact information.
    """
    analysis_df = analyze_incomplete_contacts()
    if analysis_df is None or analysis_df.empty:
        print("No incomplete contact data found.")
        return None
    
    print(f"\nFirst {n} rows of members with incomplete contacts:")
    print("=" * 100)
    
    # Show relevant columns
    display_cols = ["memberid", "groupname", "phone_status", "email_status", "phone1", "phone2", "phone3", "phone4", 
                   "email1", "email2", "email3", "email4"]
    available_cols = [col for col in display_cols if col in analysis_df.columns]
    
    # Filter to incomplete rows only for display
    incomplete_df = analysis_df[(~analysis_df['phone_complete']) | (~analysis_df['email_complete'])]
    
    print(incomplete_df[available_cols].head(n).to_string(index=False))
    
    return incomplete_df





    

# import pandas as pd
# from datetime import datetime

# # Get the dataframes
# group_contract_df = get_group_contract_df()
# debit_note_df = get_debit_note_df()

# # Convert date columns to datetime
# group_contract_df['startdate'] = pd.to_datetime(group_contract_df['startdate'])
# debit_note_df['from'] = pd.to_datetime(debit_note_df['from'])

# # Filter contracts that start in July and August 2025
# july_august_2025_contracts = group_contract_df[
#     (group_contract_df['startdate'].dt.year == 2025) & 
#     (group_contract_df['startdate'].dt.month.isin([1, 2, 3, 4, 5 ,6, 7, 8]))
# ]

# # Filter debit notes with 'from' date in July and August 2025
# july_august_2025_debit_notes = debit_note_df[
#     (debit_note_df['from'].dt.year == 2025) & 
#     (debit_note_df['from'].dt.month.isin([1, 2, 3, 4, 5 ,6, 7, 8]))
# ]

# # Get unique company names from each dataset
# contract_companies = set(july_august_2025_contracts['groupname'].unique())
# debit_note_companies = set(july_august_2025_debit_notes['company_name'].unique())

# # Find companies in contracts but not in debit notes
# companies_without_debit_notes = contract_companies - debit_note_companies

# print("=== ANALYSIS RESULTS ===")
# print(f"Total companies with contracts starting July-August 2025: {len(contract_companies)}")
# print(f"Total companies with debit notes in July-August 2025: {len(debit_note_companies)}")
# print(f"Companies with contracts but NO debit notes: {len(companies_without_debit_notes)}")

# print("\n=== COMPANIES WITH CONTRACTS BUT NO DEBIT NOTES ===")
# if companies_without_debit_notes:
#     for i, company in enumerate(sorted(companies_without_debit_notes), 1):
#         print(f"{i}. {company}")
# else:
#     print("All companies with July-August 2025 contracts have corresponding debit notes.")

# print("\n=== DETAILED BREAKDOWN ===")
# print("Companies in July-August 2025 contracts:")
# for company in sorted(contract_companies):
#     print(f"  - {company}")

# print("\nCompanies in July-August 2025 debit notes:")
# for company in sorted(debit_note_companies):
#     print(f"  - {company}")

# # Optional: Show contract details for companies without debit notes
# if companies_without_debit_notes:
#     print("\n=== CONTRACT DETAILS FOR COMPANIES WITHOUT DEBIT NOTES ===")
#     missing_companies_contracts = july_august_2025_contracts[
#         july_august_2025_contracts['groupname'].isin(companies_without_debit_notes)
#     ]
    
#     for _, row in missing_companies_contracts.iterrows():
#         print(f"Company: {row['groupname']}")
#         print(f"  Start Date: {row['startdate'].strftime('%Y-%m-%d')}")
#         print(f"  End Date: {row['enddate']}")
#         print(f"  Group ID: {row['groupid']}")
#         print("  ---")



    


# import duckdb

# # Set MotherDuck credentials
# motherduck_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6Imxlb2Nhc2V5MEBnbWFpbC5jb20iLCJzZXNzaW9uIjoibGVvY2FzZXkwLmdtYWlsLmNvbSIsInBhdCI6IndUUEFydnRna19INlVTbDFGamlyVGFoa3ZoVUtrX2pOZ05XcmtNd0VTQXciLCJ1c2VySWQiOiJmNDAzMTg5ZS05ODIxLTQ2NzktYjRmZS0wZWMyMjY0NDQyZjgiLCJpc3MiOiJtZF9wYXQiLCJyZWFkT25seSI6ZmFsc2UsInRva2VuVHlwZSI6InJlYWRfd3JpdGUiLCJpYXQiOjE3NTIyMjMzODJ9.BjvBqQ8dpgYkbW98IpxE8QTwGJbWexsctB4qNxaxGpo"


# # Connect and list existing databases
# conn = duckdb.connect(f'md:?motherduck_token={motherduck_token}')
# result = conn.execute("SHOW DATABASES").fetchall()
# print("Available databases:")
# for db in result:
#     print(f"  - {db[0]}")
# conn.close()


