# Using AWS Glue to orchestrate analytics flow

This repository demonstrates an analytics engineering workflow focused on
advanced SQL transformations, data quality validation, governance, and
orchestration using AWS Glue and PostgreSQL (RDS).

It takes data in the curated layer of a s3 bucket and transforms it to business ready data to query in RDS.

The implementation aligns with the Day 12–13 hands-on objectives:
- Modular SQL transformations
- SQL-based data quality checks and tests
- Governance metadata in SQL
- Execution of SQL workflows using AWS Glue
- Version control using Git

---

## Repository Structure

```text
day12_13_analytics-platform/
│
├── glue/
│   ├── load_curated_trips_to_pg.py      # Loads curated S3 data into Postgres staging
│   └── run_sql_pipeline.py               # Executes SQL pipeline from S3 in Glue
│
├── sql/
│   ├── 01_schemas.sql                    # Schema creation
│   ├── 02_stg_tables.sql                 # Staging tables
│   ├── 03_core_fct_trips.sql             # Core fact table transformation
│   ├── 04_dq_framework.sql               # Data quality framework tables
│   ├── 05_dq_validations.sql             # DQ metric calculations
│   └── 06_dq_tests.sql                   # SQL-based DQ tests
│
└── docs/
    └── transformations.md                # Detailed transformation & DQ documentation
