# Healthcare Analytics Pipeline
### CMS Medicare Data → Python → AWS S3 → Snowflake → dbt → Tableau

![Status](https://img.shields.io/badge/status-active%20development-success)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS%20S3-232F3E?logo=amazonaws&logoColor=white)
![Tableau](https://img.shields.io/badge/Tableau-E97627?logo=tableau&logoColor=white)

---

## Executive Summary

Production-style healthcare analytics pipeline built with **real CMS Medicare datasets** and a modern cloud data stack.

This portfolio project demonstrates how enterprise healthcare data can be ingested, stored, transformed, tested, modeled, and visualized using tools widely requested in today’s market:

- Snowflake
- Python
- AWS S3
- dbt
- SQL
- Tableau
- GitHub

Designed to showcase hands-on capability for:

- Senior BI Developer roles
- Analytics Engineer roles
- Data Engineer roles
- Healthcare / Pharma Data roles

---

## Project Metrics

- **3.61 GB** CMS Medicare Part D source file
- **26.79 million rows** loaded into `RAW.PARTD_PRESCRIBERS`
- **6,000+ US hospitals** loaded into `RAW.HOSPITAL_GENERAL_INFO`
- **3-layer Snowflake architecture** (`RAW`, `STAGING`, `MARTS`)
- **2 production-style ingestion pipelines** (Part D + Hospital datasets)
- **5+ integrated technologies** across the platform
- **End-to-end ELT workflow** from ingestion to dashboarding
- **Real healthcare business use case** using CMS public data

---

## Business Problem

Healthcare organizations need reliable analytics pipelines to answer questions such as:

- Which drug categories drive the highest Medicare spend?
- Which provider specialties generate the most claims?
- How is cost-per-claim trending over time?
- Which states have the highest prescriber concentration?
- How do hospital quality metrics compare geographically?

This project simulates a real analytics environment focused on business decision-making.

---

## Architecture

![Architecture](docs/architecture.png)

---

## Data Sources

| Dataset | Source | Format | Description |
|---|---|---|---|
| Medicare Part D Prescribers | [CMS data.gov](https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug) | CSV | Prescription drugs prescribed by providers, paid under Medicare Part D. Organized by NPI, drug name, total fills, and total cost. |
| Hospital General Information | [CMS Provider Data](https://data.cms.gov/provider-data/dataset/ynj2-r877) | CSV | ~6,000 US hospitals with star ratings, ownership type, readmission rates, and patient experience scores. |

### 1. Medicare Part D Prescribers

Provider-level prescription activity including:

- Drug name
- Total claims
- Total fills
- Total drug cost
- Prescriber identifiers

### 2. Hospital General Information

- Star ratings
- Ownership type
- Readmission metrics
- Patient experience indicators
- Geographic data

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Data Source | CMS Medicare Data | Real healthcare source data |
| Ingestion | Python, pandas, boto3 | Read, clean, upload files |
| Cloud Storage | AWS S3 | Raw landing zone |
| Data Warehouse | Snowflake | Storage, compute, analytics serving |
| Transformation | dbt | Models, tests, documentation |
| Visualization | Tableau Public | Dashboard publishing |
| Version Control | GitHub | Code and documentation |

---

## Snowflake Schema Design

    HEALTHCARE_DB

    ├── RAW
    │   ├── PARTD_PRESCRIBERS
    │   └── HOSPITAL_GENERAL_INFO

    ├── STAGING
    │   ├── STG_PARTD_PRESCRIBERS
    │   └── STG_HOSPITAL_GENERAL_INFO

    └── MARTS
        ├── MART_DRUG_SPEND_BY_SPECIALTY
        ├── MART_HOSPITAL_QUALITY
        └── MART_TOP_PRESCRIBERS

---

## Repository Structure

    healthcare-snowflake-dbt-portfolio/

    ├── ingestion/
    │   ├── ingest_partd.py
    │   └── requirements.txt

    ├── dbt/
    │   ├── models/
    │   │   ├── staging/
    │   │   └── marts/
    │   └── dbt_project.yml

    ├── docs/
    │   ├── architecture.png
    │   └── schema.sql

    └── README.md

---

## How to Run the Ingestion Pipeline

### Prerequisites

- Python 3.9+
- AWS account
- S3 bucket in `us-west-2`
- Snowflake account
- Access credentials configured as environment variables

### Environment Variables

    export AWS_ACCESS_KEY_ID=your_key
    export AWS_SECRET_ACCESS_KEY=your_secret
    export SNOWFLAKE_ACCOUNT=your_account
    export SNOWFLAKE_USER=your_user
    export SNOWFLAKE_PASSWORD=your_password
    export SNOWFLAKE_WAREHOUSE=COMPUTE_WH
    export SNOWFLAKE_DATABASE=HEALTHCARE_DB
    export SNOWFLAKE_SCHEMA=RAW

### Install Dependencies

    pip install -r ingestion/requirements.txt

### Run Pipeline

Edit the dataset selector inside `main()`:

    SELECTED_DATASET = "partd"

    # or

    SELECTED_DATASET = "hospital"

Then run:

    python ingestion/ingest_partd.py

---

## Ingestion Layer

The ingestion layer is a reusable Python pipeline designed to onboard multiple CMS healthcare datasets into Snowflake through AWS S3.

Current supported datasets:

| Dataset | Source File | RAW Target Table |
|---|---|---|
| Medicare Part D Prescribers | `partd_prescribers_2023.csv` | `RAW.PARTD_PRESCRIBERS` |
| Hospital General Information | `hospital_general_information.csv` | `RAW.HOSPITAL_GENERAL_INFO` |

### Why This Layer Exists

Healthcare analytics teams often receive large flat files from multiple public or internal sources.

This ingestion layer standardizes how those files are processed by automating:

- Local file validation
- Dataset selection via configuration
- Secure upload to AWS S3
- Snowflake file format creation
- Snowflake external stage creation
- RAW table creation
- COPY INTO bulk loading
- Row count validation

### Dynamic Dataset Selection

The script uses a dataset selector in `main()`:

    SELECTED_DATASET = "partd"

    # or

    SELECTED_DATASET = "hospital"

This controls:

- Source filename
- S3 folder destination
- Snowflake stage
- Snowflake file format
- RAW target table
- COPY INTO logic

### Example Execution Flags

    RUN_UPLOAD_TO_S3 = True
    RUN_CREATE_STAGE = True
    RUN_CREATE_RAW_TABLE = True
    RUN_COPY_INTO = True

These flags allow controlled re-runs without rebuilding every component.

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

This demonstrates production-style ingestion patterns used in real data engineering environments:

- Modular pipeline design
- Repeatable dataset onboarding
- Cloud storage integration
- Warehouse automation
- Scalable multi-source architecture    

---

## Example Analytics Outputs

### Financial Analytics

- Total Medicare spend by drug class
- Cost-per-claim trends
- High-cost provider segments

### Operational Analytics

- Top prescribers by specialty
- State-level provider density
- Hospital benchmarking

### Executive Reporting

- KPI scorecards
- Geographic dashboards
- Trend analysis
- Ranking views

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

*Repository under active development. Last updated: April 25, 2026.*
