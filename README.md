# Healthcare Analytics Pipeline
### CMS Medicare Data → Python/S3 → Snowflake → dbt → Tableau
 
![Status](https://img.shields.io/badge/status-in%20progress-yellow)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS%20S3-232F3E?logo=amazonaws&logoColor=white)
![Tableau](https://img.shields.io/badge/Tableau-E97627?logo=tableau&logoColor=white)
 
---
 
## Overview
 
End-to-end cloud analytics pipeline built on public CMS Medicare Part D prescriber data — the same dataset type used daily in pharma and payer analytics. This project demonstrates a full modern data stack: Python ingestion, cloud staging on AWS S3, Snowflake data warehouse, dbt transformation layers with tests and documentation, and a Tableau Public dashboard.
 
Built as a portfolio project targeting **Senior BI Developer** and **Data Engineer** roles in healthcare and pharma.
 
---
 
## Architecture
 
> Architecture diagram coming in Week 4 — will be added here.
 
```
CMS Medicare CSV
      │
      ▼
Python (pandas + boto3)
      │  ── light cleaning, snake_case columns
      ▼
AWS S3 (us-west-2)
      │  ── external stage
      ▼
Snowflake RAW layer
      │  ── COPY INTO
      ▼
dbt Staging models
      │  ── type casting, renaming, timestamps
      ▼
dbt Mart models
      │  ── business aggregations
      ▼
Tableau Public Dashboard
```
 
---
 
## Data Sources
 
| Dataset | Source | Format | Description |
|---|---|---|---|
| Medicare Part D Prescribers | [CMS data.gov](https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug) | CSV | Prescription drugs prescribed by providers, paid under Medicare Part D. Organized by NPI, drug name, total fills, and total cost. |
| Hospital General Information | [CMS Provider Data](https://data.cms.gov/provider-data/dataset/ynj2-r877) | CSV | ~6,000 US hospitals with star ratings, ownership type, readmission rates, and patient experience scores. |
 
---
 
## Tech Stack
 
| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Python (pandas, boto3) | Read CSV, clean, upload to S3 |
| Cloud Storage | AWS S3 (us-west-2) | Raw file landing zone |
| Data Warehouse | Snowflake (Enterprise) | Storage, compute, and serving layer |
| Transformation | dbt | Staging and mart model layers with tests |
| Visualization | Tableau Public | Dashboard published publicly |
| Version Control | GitHub | All code and documentation |
 
---
 
## Repository Structure
 
```
healthcare-snowflake-dbt-portfolio/
│
├── ingestion/
│   ├── ingest_partd.py          # Main ingestion script
│   └── requirements.txt         # Python dependencies
│
├── dbt/
│   ├── models/
│   │   ├── staging/             # stg_partd_prescribers.sql, stg_hospitals.sql
│   │   └── marts/               # mart_drug_spend.sql, mart_top_prescribers.sql
│   ├── tests/
│   └── dbt_project.yml
│
├── docs/
│   ├── architecture.png         # Architecture diagram (added Week 4)
│   └── schema.sql               # RAW layer data dictionary
│
└── README.md
```
 
---
 
## Snowflake Schema Design
 
```
HEALTHCARE_DB
├── RAW
│   ├── PARTD_PRESCRIBERS        # CMS Part D raw data
│   └── HOSPITALS                # CMS Hospital Quality raw data
├── STAGING
│   ├── STG_PARTD_PRESCRIBERS
│   └── STG_HOSPITALS
└── MARTS
    ├── MART_DRUG_SPEND_BY_SPECIALTY
    ├── MART_HOSPITAL_QUALITY
    └── MART_TOP_PRESCRIBERS
```
 
---
 
## How to Run the Ingestion Pipeline
 
> Full instructions coming in Week 3 — will be updated here.
 
**Prerequisites:**
- Python 3.9+
- AWS account with S3 bucket in us-west-2
- Snowflake account (Enterprise)
- Environment variables set (see below)
**Environment variables required:**
```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_WAREHOUSE=COMPUTE_WH
export SNOWFLAKE_DATABASE=HEALTHCARE_DB
export SNOWFLAKE_SCHEMA=RAW
```
 
**Run:**
```bash
pip install -r ingestion/requirements.txt
python ingestion/ingest_partd.py
```
 
---
 
## dbt Models
 
> dbt models and documentation coming in Week 5-6.
 
---
 
## Tableau Dashboard
 
> Dashboard published to Tableau Public in Week 6 — link will be added here.
 
---
 
## Project Status
 
| Week | Focus | Status |
|---|---|---|
| 1–2 | Snowflake Foundation + Badge 1 | ✅ Complete |
| 3–4 | Python ingestion + GitHub setup | 🔄 In progress |
| 5–6 | dbt + Tableau | ⬜ Pending |
| 7–8 | SnowPro Core COF-C03 exam prep | ⬜ Pending |
 
---
 
## Certifications
 
- ✅ [Snowflake Badge 1: Data Warehousing Workshop](https://achieve.snowflake.com/79651a1f-079a-424f-b714-10123ca48425#acc.REHfn6XP) — April 2026
- ⬜ Snowflake Badge 2: Collaboration, Marketplace & Cost Estimation — planned Week 7
- ⬜ SnowPro Core COF-C03 — planned Week 8
---
 
## Background
 
Built by a healthcare/pharma analytics professional with 20+ years of experience (Roche, Wyeth) transitioning into cloud data engineering. This project was designed to demonstrate hands-on proficiency with the modern data stack that the market is asking for: Snowflake + dbt + Python + cloud storage.
 
---
 
*This repository is under active development. Last updated: April 2026.*
