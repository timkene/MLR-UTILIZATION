

import os
import dlt
from dlt_sources import dashboard_sources

# Set DuckDB file path via environment variable
os.environ["DESTINATION__DUCKDB__CREDENTIALS__FILEPATH"] = "/Users/kenechukwuchukwuka/Downloads/DLT/my_pipeline.duckdb"

# Create pipeline
pipeline = dlt.pipeline(
    pipeline_name='my_pipeline',
    destination='duckdb',
    dataset_name='clearline_db'
)

# Run pipeline
load_info = pipeline.run(dashboard_sources())
print(load_info)







# motherduck_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6Imxlb2Nhc2V5MEBnbWFpbC5jb20iLCJzZXNzaW9uIjoibGVvY2FzZXkwLmdtYWlsLmNvbSIsInBhdCI6IndUUEFydnRna19INlVTbDFGamlyVGFoa3ZoVUtrX2pOZ05XcmtNd0VTQXciLCJ1c2VySWQiOiJmNDAzMTg5ZS05ODIxLTQ2NzktYjRmZS0wZWMyMjY0NDQyZjgiLCJpc3MiOiJtZF9wYXQiLCJyZWFkT25seSI6ZmFsc2UsInRva2VuVHlwZSI6InJlYWRfd3JpdGUiLCJpYXQiOjE3NTIyMjMzODJ9.BjvBqQ8dpgYkbW98IpxE8QTwGJbWexsctB4qNxaxGpo"

#jddr xruu xjsh akvi