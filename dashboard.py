import pandas as pd
import streamlit as st

from MEDITEST import build_enriched_total_procedures, analyze_incomplete_contacts


@st.cache_data(show_spinner=True)
def load_data() -> pd.DataFrame:
    df = build_enriched_total_procedures()
    if df is None or df.empty:
        return pd.DataFrame()
    # Normalize expected columns
    if "requestdate" in df.columns:
        df["requestdate"] = pd.to_datetime(df["requestdate"], errors="coerce")
    else:
        # Try common variants
        for c in ["RequestDate", "request_date", "date"]:
            if c in df.columns:
                df["requestdate"] = pd.to_datetime(df[c], errors="coerce")
                break

    # Ensure key columns exist even if missing
    for col in ["granted", "panumber", "providername", "IID"]:
        if col not in df.columns:
            df[col] = None

    # Keep only rows with a valid date
    df = df[pd.notnull(df["requestdate"])].copy()
    return df


@st.cache_data(show_spinner=True)
def load_contact_analysis() -> pd.DataFrame:
    """Load contact completeness analysis data"""
    return analyze_incomplete_contacts()


def generate_contact_summary(analysis_df: pd.DataFrame) -> pd.DataFrame:
    """Generate contact completeness summary from analysis data"""
    if analysis_df is None or analysis_df.empty:
        return pd.DataFrame()
    
    # Group by groupname and calculate summary statistics
    summary = analysis_df.groupby('groupname').agg({
        'memberid': 'count',  # Total members
        'phone_complete': lambda x: (~x).sum(),  # Count of incomplete phones
        'email_complete': lambda x: (~x).sum(),  # Count of incomplete emails
    }).rename(columns={
        'memberid': 'total_members',
        'phone_complete': 'incomplete_phone_count',
        'email_complete': 'incomplete_email_count'
    })
    
    # Calculate percentages (complete instead of incomplete)
    summary['phone_complete_pct'] = ((summary['total_members'] - summary['incomplete_phone_count']) / summary['total_members'] * 100).round(2)
    summary['email_complete_pct'] = ((summary['total_members'] - summary['incomplete_email_count']) / summary['total_members'] * 100).round(2)
    
    # Sort by total members descending
    summary = summary.sort_values('total_members', ascending=False)
    
    return summary


def filter_by_month_range(df: pd.DataFrame, start_month: pd.Timestamp, end_month: pd.Timestamp) -> pd.DataFrame:
    if df.empty:
        return df 
    # Clamp dates to month boundaries
    start = pd.Timestamp(year=start_month.year, month=start_month.month, day=1)
    end = pd.Timestamp(year=end_month.year, month=end_month.month, day=1) + pd.offsets.MonthEnd(0)
    return df[(df["requestdate"] >= start) & (df["requestdate"] <= end)].copy()


def prepare_monthly_series(df: pd.DataFrame, start_month: pd.Timestamp, end_month: pd.Timestamp) -> dict:
    if df.empty:
        empty_index = pd.date_range(
            start=pd.Timestamp(year=start_month.year, month=start_month.month, day=1),
            end=pd.Timestamp(year=end_month.year, month=end_month.month, day=1),
            freq="MS",
        )
        empty = pd.DataFrame(index=empty_index)
        return {
            "granted_sum": empty,
            "unique_panumber": empty,
            "unique_providername": empty,
            "unique_IID": empty,
        }

    df = df.copy()
    df["bucket"] = df["requestdate"].values.astype("datetime64[M]") 

    granted_series = (
        df.groupby("bucket")["granted"].sum(min_count=1).rename("granted_sum").to_frame()
    )
    panumber_series = (
        df.groupby("bucket")["panumber"].nunique().rename("unique_panumber").to_frame()
    )
    providername_series = (
        df.groupby("bucket")["providername"].nunique().rename("unique_providername").to_frame()
    )
    iid_series = df.groupby("bucket")["IID"].nunique().rename("unique_IID").to_frame()

    # Build month index only for months that exist in the data
    actual_months = sorted(df["bucket"].unique())
    if len(actual_months) > 0:
        month_index = pd.DatetimeIndex(actual_months)
    else:
        month_index = pd.date_range(
            start=pd.Timestamp(year=start_month.year, month=start_month.month, day=1),
            end=pd.Timestamp(year=end_month.year, month=end_month.month, day=1),
            freq="MS",
        )
    for name, frame in {
        "granted_sum": granted_series,
        "unique_panumber": panumber_series,
        "unique_providername": providername_series,
        "unique_IID": iid_series,
    }.items():
        frame.index.name = "month"
        reindexed = frame.reindex(month_index)
        # Ensure chronological order
        reindexed = reindexed.sort_index()
        if name == "granted_sum":
            granted_series = reindexed
        elif name == "unique_panumber":
            panumber_series = reindexed
        elif name == "unique_providername":
            providername_series = reindexed
        else:
            iid_series = reindexed

    return {
        "granted_sum": granted_series,
        "unique_panumber": panumber_series,
        "unique_providername": providername_series,
        "unique_IID": iid_series,
    }


def main() -> None:
    st.set_page_config(page_title="PA Dashboard", layout="wide")
    st.title("PA Dashboard")

    # Month range filter: default April to current month
    today = pd.Timestamp.today()
    default_start = pd.Timestamp(year=today.year, month=4, day=1)
    start_month = st.date_input("Start month", value=default_start, format="YYYY-MM-DD")
    end_month = st.date_input("End month", value=today.normalize(), format="YYYY-MM-DD")

    df = load_data()
    if df.empty:
        st.warning("No data available to display. Please check your data sources.")
        return

    # Filter last 6 months
    df_filtered = filter_by_month_range(df, pd.Timestamp(start_month), pd.Timestamp(end_month))
    if df_filtered.empty:
        st.warning("No rows in the selected period.")
        return

    # Prepare series
    series = prepare_monthly_series(df_filtered, pd.Timestamp(start_month), pd.Timestamp(end_month))

    # Charts
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Sum of Granted per Month")
        st.bar_chart(series["granted_sum"])

    with col2:
        st.subheader("Unique PANumber per Month")
        st.bar_chart(series["unique_panumber"])

    col3, col4 = st.columns(2)
    with col3:
        st.subheader("Unique Provider Name per Month")
        st.bar_chart(series["unique_providername"])

    with col4:
        st.subheader("Unique IID per Month")
        st.bar_chart(series["unique_IID"])

    # Monthly breakdown by firstname
    st.subheader("Monthly Breakdown by User (Firstname)")
    
    # Prepare monthly data for each firstname
    df_monthly = df_filtered.copy()
    df_monthly["month"] = df_monthly["requestdate"].values.astype("datetime64[M]")
    
    # Get unique firstnames and months
    firstnames = df_monthly["firstname"].dropna().unique()
    months = sorted(df_monthly["month"].unique())
    
    # Only show months that exist in the data
    if len(months) == 0:
        st.info("No data available for the selected period.")
        return
    
    if len(firstnames) > 0 and len(months) > 0:
        # Create pivot table: firstname vs month with panumber count and granted sum
        pivot_data = []
        for firstname in firstnames:
            row = {"firstname": firstname}
            for month in months:
                month_data = df_monthly[
                    (df_monthly["firstname"] == firstname) & 
                    (df_monthly["month"] == month)
                ]
                # Count unique panumbers and sum granted for this firstname in this month
                unique_panumbers = month_data["panumber"].nunique()
                total_granted = month_data["granted"].sum() if "granted" in month_data.columns else 0
                
                # Format month as string for column names
                month_str = pd.Timestamp(month).strftime("%b %Y")
                row[f"{month_str}_panumber_count"] = unique_panumbers
                row[f"{month_str}_granted_sum"] = total_granted
            
            pivot_data.append(row) 
        
        # Convert to DataFrame and display
        pivot_df = pd.DataFrame(pivot_data)
        if not pivot_df.empty:
            st.dataframe(pivot_df, use_container_width=True)
        else:
            st.info("No data available for the selected period.")
    else:
        st.info("No firstname data available for the selected period.")

    # Contact Completeness Summary
    st.subheader("Contact Completeness Summary by Group")
    
    # Load contact analysis data
    contact_analysis_df = load_contact_analysis()
    if contact_analysis_df is not None and not contact_analysis_df.empty:
        summary_df = generate_contact_summary(contact_analysis_df)
        if not summary_df.empty:
            st.dataframe(summary_df, use_container_width=True)
        else:
            st.info("No contact analysis data available.")
    else:
        st.info("No contact analysis data available.")

    with st.expander("Preview Data (filtered period)"):
        st.dataframe(
            df_filtered[[
                "requestdate",
                "panumber",
                "providername",
                "IID",
                "granted",
                "firstname",
            ]].sort_values("requestdate", ascending=False).head(500)
        )


if __name__ == "__main__":
    main()


