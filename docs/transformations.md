```md
# SQL Transformations & Data Quality Documentation

This document describes the SQL transformation pipeline, data quality framework,
and how the workflow is executed using AWS Glue.

---

## Overall Pipeline Design

**Grain:** 1 row per taxi trip  
**Style:** Full refresh (TRUNCATE + INSERT)  
**Target Audience:** Analytics & BI consumption  

The pipeline converts curated trip data into an analytics-ready fact table,
enriched with master data and protected by SQL-based data quality tests.

---

## SQL Script Overview

### 01_schemas.sql
**Purpose**
- Creates required schemas:
  - `stg` (staging)
  - `core` (analytics)
  - `dq` (data quality)

**Why**
- Separates raw/staging, analytics, and quality concerns.

---

### 02_stg_tables.sql
**Purpose**
- Defines staging tables used to load curated S3 data into PostgreSQL.

**Notes**
- No transformations applied here.
- Staging mirrors curated data structure.

---

### 03_core_fct_trips.sql
**Purpose**
- Builds the analytics-ready fact table `core.fct_trips`.

**Key Transformations**
- Standardized column naming
- Trip duration calculation
- Removal of invalid records:
  - null timestamps
  - negative distances
  - invalid totals
- Enrichment from MDM tables:
  - `mdm.taxi_zone_master`
  - `mdm.taxi_rate_code_master`

**Governance**
- Author, owner, purpose, dependencies, and quality expectations documented
  in SQL header.

---

### 04_dq_framework.sql
**Purpose**
- Creates the data quality framework.

**Objects Created**
- `dq.test_results`

**Why**
- Central place to record test outcomes (pass/fail, metrics, thresholds).

---

### 05_dq_validations.sql
**Purpose**
- Computes data quality metrics, such as:
  - Pickup zone fill rate
  - Dropoff zone fill rate
  - Rate code enrichment rate

**Output**
- Metrics are calculated but not evaluated here.

---

### 06_dq_tests.sql
**Purpose**
- Implements SQL-based tests using validation metrics.

**Behavior**
- Inserts rows into `dq.test_results`:
  - test_name
  - passed (boolean)
  - metric_value
  - threshold
  - execution timestamp

**Example Tests**
- Zone enrichment fill rate ≥ threshold
- Referential integrity expectations

---

## Data Quality Philosophy

- No fallback or default values
- Missing enrichment is allowed but measured
- Failures are visible, not hidden
- Tests are transparent and queryable via SQL

---

## Glue Workflow Execution

A Glue Python Shell job performs the following:
1. Downloads SQL scripts from S3
2. Executes them in strict order
3. Runs against PostgreSQL using SSL
4. Produces:
   - Updated `core.fct_trips`
   - New rows in `dq.test_results`

The workflow is **manually triggered** (no schedule), which is sufficient for
this hands-on.

---

## Validation Queries

```sql
-- Latest DQ test results
SELECT *
FROM dq.test_results
ORDER BY created_at DESC;
