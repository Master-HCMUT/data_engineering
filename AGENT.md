# Project overview
This is the data engineering project for Ecommerce domain. The goal is to build a data pipeline to collect, process, and analyze data from various sources to provide insights for business decisions.

# Data sources
- Currently, for academic purpose, we only use one. CSV files from `assignment/sql-server-samples/samples/databases/adventure-works/oltp-install-script` are uploaded to Databricks volume `/Volumes/dev/raw/oltp`, then we use them as source of our data platform.

# Data architecture
- Bronze: Ingest raw data (which I have done so far)
- Silver: Clean and transform data (which I am doing)
- Gold: Aggregate data for business intelligence 
    - Here, firsly, we have star schema, then do some aggegation to create data marts for business intelligence
    - We have one fact table and many dims table( e.g, dim date, customer,...). Decide it based on what we have in the bronze
# Analytic question
- Popular products ? 
- Monthly Revenue ?
