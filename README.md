# MLR & Utilization Dashboard

A comprehensive Streamlit dashboard for Medical Loss Ratio (MLR) analysis and healthcare utilization monitoring.

## Features
- Real-time MLR calculations for PA and Claims
- Email alerts for threshold breaches (65%, 75%, 85%)
- Utilization analysis with interactive charts
- Raw data download functionality
- Support for chronic disease, surgery, maternity, dental, and optical analysis

## Setup
1. Install dependencies: `pip install -r requirements.txt`
2. Configure `secrets.toml` with your database credentials
3. Ensure `benefits_cleaned.csv` is in the same directory
4. Run: `streamlit run MLR.py`

## Configuration
- Database connections configured in `secrets.toml`
- Email settings in the sidebar
- Benefit codes loaded from `benefits_cleaned.csv`

## Requirements
- Python 3.9+
- SQL Server database access
- Gmail App Password for email alerts
