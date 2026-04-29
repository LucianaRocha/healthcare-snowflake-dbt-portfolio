# Medicare Part D Analytics Platform  
### CMS Medicare Data → Python → AWS S3 → Snowflake → dbt → Tableau

![Status](https://img.shields.io/badge/status-active%20development-success)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS%20S3-232F3E?logo=amazonaws&logoColor=white)
![Tableau](https://img.shields.io/badge/Tableau-E97627?logo=tableau&logoColor=white)

---

# Executive Summary

Production-style healthcare analytics platform built using **real CMS Medicare Part D public data** and a modern cloud data stack.

This project demonstrates how enterprise-scale healthcare data can be:

- Ingested with Python
- Optimized and stored in AWS S3
- Loaded into Snowflake
- Modeled using dbt
- Tested and documented
- Visualized in Tableau

Designed to showcase practical capabilities for:

- Analytics Engineer
- Data Engineer
- Senior BI Developer
- Healthcare / Pharma Analytics
- Snowflake Developer

## Production Design Patterns

This project applies real-world data engineering patterns:

- Incremental multi-year ingestion
- Partitioned data lake design (S3)
- Idempotent pipeline execution (safe re-runs)
- Separation of storage and compute (Snowflake)
- Modular transformations (dbt)
- Data validation and testing

---

# Project Highlights

- **3.61 GB** raw CMS source file
- **691 MB compressed** GZIP cloud landing file
- **26.79+ million rows** loaded to Snowflake
- **Multi-year ingestion architecture**
- **Dynamic parameterized pipeline**
- **Partitioned S3 data lake structure**
- **dbt dimensional modeling layer**
- **Interactive Tableau dashboards**
- **Real healthcare business use case**

---

# Current Status

| Layer | Status |
|---|---|
| Python Ingestion Pipeline | ✅ Complete |
| AWS S3 Landing Zone | ✅ Complete |
| Snowflake RAW Layer | ✅ Complete |
| Multi-Year Architecture | ✅ Complete |
| dbt Modeling Layer | 🔄 In Progress |
| Tableau Dashboards | ⬜ Planned |

---

# Business Problem

Healthcare organizations need reliable analytics platforms to answer questions such as:

- Which states drive the highest Medicare drug spend?
- Which specialties generate the most prescription volume?
- Which providers prescribe the highest-cost drugs?
- How is generic adoption changing over time?
- Where are provider concentrations highest?

This project simulates a real production analytics environment focused on business decision-making.

---

## Architecture

![Architecture](docs/architecture.png)

---

## Data Sources

| Dataset | Source | Format | Description |
|---|---|---|---|
| Medicare Part D Prescribers | [CMS data.gov](https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug) | CSV | Prescription drugs prescribed by providers, paid under Medicare Part D. Organized by NPI, drug name, total fills, and total cost. |

### 1. Medicare Part D Prescribers

Dataset contains:

- Drug name
- Prescriber NPI
- Provider specialty
- Brand drug name
- Generic drug name
- State
- Total claims
- Total fills
- Total beneficiaries
- Total drug cost


---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Data Source | CMS Medicare Part D Prescribers | Real public healthcare dataset for prescription drug spend analytics |
| Ingestion | Python, pandas, boto3 | Validate source files, add metadata, compress CSV files, and upload to S3 |
| Cloud Storage | AWS S3 | Partitioned raw data lake organized by report year |
| Query Layer | Amazon Athena | Optional validation and raw data exploration over S3 partitions |
| Data Warehouse | Snowflake | RAW storage, scalable compute, and analytics serving layer |
| Transformation | dbt | Staging models, dimensions, facts, marts, tests, and documentation |
| Visualization | Tableau Public | Executive dashboards and public portfolio reporting |
| Version Control | GitHub | Code, documentation, and project history |

---

## Snowflake Schema Design

```text
MEDICARE_ANALYTICS_DB

├── RAW
│   └── PARTD_PRESCRIBERS
│       • Multi-year Medicare Part D source data
│       • Loaded from AWS S3 compressed CSV files
│       • Includes REPORT_YEAR metadata column

├── STAGING
│   └── STG_PARTD_PRESCRIBERS
│       • Cleaned and standardized source model
│       • Renamed columns
│       • Data quality preparation layer

├── ANALYTICS
│   ├── DIM_PRESCRIBER
│   ├── DIM_DRUG
│   ├── DIM_GEOGRAPHY
│   ├── DIM_SPECIALTY
│   └── FCT_PARTD_PRESCRIPTIONS

└── MARTS
    ├── MART_DRUG_SPEND_BY_STATE
    ├── MART_SPECIALTY_SUMMARY
    ├── MART_TOP_PRESCRIBERS
    └── MART_BRAND_GENERIC_SUMMARY
```

---

## Repository Structure

```text
healthcare-snowflake-dbt-portfolio/

├── ingestion/
│   ├── ingest_partd.py
│   └── requirements.txt

├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   ├── analytics/
│   │   └── marts/
│   ├── tests/
│   ├── macros/
│   ├── seeds/
│   └── dbt_project.yml

├── docs/
│   ├── architecture.png
│   ├── schema.sql
│   ├── dashboard_mockups/
│   └── screenshots/

├── sql/
│   ├── snowflake_setup.sql
│   └── validation_queries.sql

├── .gitignore
├── README.md
└── requirements.txt
```



---

## How to Run the Ingestion Pipeline

### Prerequisites

- Python 3.9+
- AWS account with S3 access
- S3 bucket created in `us-west-2`
- Snowflake account
- Local CMS Part D source files downloaded
- Credentials configured in `.env`

---

### Environment Variables

```env
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=us-west-2
S3_BUCKET_NAME=medicare-partd-analytics

SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=HEALTHCARE_DB
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_ROLE=SYSADMIN

LOCAL_FILE_PATH=data
```

### Install Dependencies

```bash
pip install -r ingestion/requirements.txt
```

### Run Pipeline

Load a specific Medicare Part D report year using the `--year` parameter:

```bash
python ingestion/ingest_partd.py --year 2023
python ingestion/ingest_partd.py --year 2022
python ingestion/ingest_partd.py --year 2021
```

### What Happens Automatically
For the selected year, the pipeline will:

- Validate the local source file
- Add REPORT_YEAR metadata column
- Compress the file to .csv.gz
- Upload to partitioned AWS S3 path
- Repair Athena partitions
- Load Snowflake RAW table
- Replace existing rows for that year
- Validate row counts

---

## Ingestion Layer

The ingestion layer is a production-style Python pipeline designed to onboard **multi-year CMS Medicare Part D datasets** into Snowflake through AWS S3.

The pipeline supports parameterized yearly loads, metadata enrichment, compression, and cloud partitioning.

### Current Supported Dataset

| Dataset | Source File Pattern | RAW Target Table |
|---|---|---|
| Medicare Part D Prescribers | `partd_prescribers_<year>.csv` | `RAW.PARTD_PRESCRIBERS` |

### Example Source Files

```text
data/partd_prescribers_2021.csv
data/partd_prescribers_2022.csv
data/partd_prescribers_2023.csv
```

### Final S3 Cloud Objects
- raw/year=2021/partd_prescribers_2021.csv.gz
- raw/year=2022/partd_prescribers_2022.csv.gz
- raw/year=2023/partd_prescribers_2023.csv.gz


### Why This Layer Exists

Healthcare analytics teams often receive large yearly flat files.

This ingestion layer standardizes onboarding by automating:

- Local file validation
- Year-based file selection
- Metadata enrichment with REPORT_YEAR
- Compression to .csv.gz
- Secure upload to AWS S3
- Athena partition refresh
- Snowflake RAW loading
- Safe yearly reload logic
- Row count validation

### Dynamic Year Selection

The script uses a command-line parameter to select which Medicare Part D report year to process.

### Example

```bash
python ingestion/ingest_partd.py --year 2023
python ingestion/ingest_partd.py --year 2022
python ingestion/ingest_partd.py --year 2021
```
### This Automatically Controls
- Source filename
- Local file lookup
- REPORT_YEAR metadata column
- Temporary compressed file name
- S3 partition destination
- Snowflake stage source path
- Year-specific delete/reload logic
- Validation row counts

### This Automatically Controls
For:

```bash
python ingestion/ingest_partd.py --year 2023
```
The pipeline resolves to:

```bash
Source File: partd_prescribers_2023.csv
S3 Folder: raw/year=2023/
Cloud File: partd_prescribers_2023.csv.gz
Target Rows: REPORT_YEAR = 2023
```

### Example Execution Flags

The pipeline uses runtime flags inside `main()` to control which steps execute.

```python
RUN_ADD_REPORT_YEAR_AND_COMPRESS = True
RUN_UPLOAD_TO_S3 = True
RUN_REPAIR_ATHENA = True

RUN_CREATE_INFRASTRUCTURE = False
RUN_SNOWFLAKE_TEST = False
RUN_CREATE_STAGE = False
RUN_CREATE_RAW_TABLE = False
RUN_COPY_INTO = False
```

### Example Workflow

    CMS CSV File
       ↓
    Python Validation
       ↓
    AWS S3 Landing Zone
       ↓
    Snowflake External Stage
       ↓
    RAW Table Load (COPY INTO)
       ↓
    Validation Checks

### Why It Matters

This project demonstrates production-style ingestion patterns used in modern data engineering environments:

- Parameterized multi-year pipeline design
- Repeatable yearly data onboarding
- Large-file processing with chunked pandas workflows
- Automated metadata enrichment (`REPORT_YEAR`)
- Compressed cloud storage optimization (`.csv.gz`)
- Partitioned AWS S3 data lake architecture
- Athena integration for raw data validation
- Snowflake warehouse automation
- Safe delete-and-reload year processing
- Scalable foundation for future annual CMS data loads

---

## dbt Transformation Layer (In Progress)

The dbt layer transforms raw Medicare data into analytics-ready models following a layered architecture:

- **STAGING**
  - Column standardization
  - Data type casting
  - Null handling
  - Metadata enrichment

- **ANALYTICS**
  - Dimensional modeling (star schema)
  - Fact table for prescriptions
  - Reusable dimension tables

- **MARTS**
  - Business-focused aggregations
  - Optimized for Tableau dashboards

### Example dbt Model

```sql
SELECT
    prscrbr_npi        AS prescriber_npi,
    tot_clms::NUMBER   AS total_claims,
    tot_benes::NUMBER  AS total_beneficiaries,
    tot_drug_cst::FLOAT AS total_drug_cost,
    report_year
FROM {{ source('raw', 'partd_prescribers') }}
```

---

## Data Modeling Strategy

The project follows a dimensional modeling approach:

- **Fact Table**
  - `fct_partd_prescriptions`
  - Grain: Prescriber + Drug + Year

- **Dimensions**
  - `dim_prescriber`
  - `dim_drug`
  - `dim_geography`
  - `dim_specialty`

This structure enables:

- Efficient aggregation queries
- Flexible business analysis
- Scalable analytics modeling

Key Metrics:

- Total beneficiaries → SUM(tot_benes)
- Total providers → COUNT(DISTINCT prscrbr_npi)
---

## Example Analytics Outputs

### Financial Analytics

- Total Medicare drug spend by state
- Year-over-year spend trends
- Brand vs Generic cost comparison
- Average cost per claim by specialty
- Highest-cost provider segments

### Operational Analytics

- Top prescribers by total claims
- Top prescribers by total drug spend
- State-level provider concentration
- Specialty prescription volume rankings
- Beneficiary coverage by provider type

### Executive Reporting

- KPI scorecards
- Geographic spend dashboards
- Multi-year trend analysis
- Top 10 ranking views
- Interactive filters by year, state, specialty, and drug type

---

## Project Status

| Phase | Deliverable | Status |
|---|---|---|
| Snowflake Foundation | Badge 1 completed | ✅ |
| Python + AWS S3 Ingestion | Completed | ✅ |
| dbt Modeling Layer | In progress | 🔄 |
| Tableau Dashboard | Planned | ⬜ |
| SnowPro Core COF-C03 | Planned | ⬜ |

---

## Certifications

- ✅ Snowflake Badge 1: Data Warehousing Workshop
- ⬜ Snowflake Badge 2: Collaboration, Marketplace & Cost Estimation
- ⬜ SnowPro Core COF-C03

---

## Professional Background

Built by a healthcare/pharma analytics professional with 20+ years of industry experience including Roche and Wyeth, transitioning into cloud data engineering and modern analytics architecture.

This combination of domain expertise + technical execution creates strong value for healthcare, pharma, and analytics teams.

---

## Opportunities

Open to opportunities involving:

- Snowflake
- BI Development
- Analytics Engineering
- Data Engineering
- Healthcare Analytics

---

## Why This Project Matters

Many portfolio projects use toy datasets.

This project uses **real healthcare data**, enterprise cloud tools, modern warehousing concepts, and business reporting scenarios that reflect real analytics work.

---

*Repository under active development. Last updated: April 28, 2026.*
