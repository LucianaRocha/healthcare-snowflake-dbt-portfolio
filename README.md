# Healthcare Analytics Pipeline  
### Snowflake • Python • AWS S3 • dbt • Tableau • Real CMS Medicare Data

![Status](https://img.shields.io/badge/status-active%20development-success)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS%20S3-232F3E?logo=amazonaws&logoColor=white)
![Tableau](https://img.shields.io/badge/Tableau-E97627?logo=tableau&logoColor=white)

---

# Executive Summary

Production-style healthcare analytics pipeline built with a modern cloud data stack using **public CMS Medicare datasets**.

This project demonstrates how raw large-scale healthcare data can be ingested, staged, transformed, tested, modeled, and visualized using tools commonly requested in today's market:

- Snowflake
- Python
- AWS S3
- dbt
- Tableau
- SQL
- GitHub

Designed to showcase hands-on capability for:

- Senior BI Developer roles  
- Analytics Engineer roles  
- Data Engineer roles  
- Healthcare / Pharma Data roles

---

# Business Problem

Healthcare organizations need reliable analytics pipelines to answer questions such as:

- Which drug categories drive the highest Medicare spend?
- Which provider specialties generate the most claims?
- How does cost-per-claim trend over time?
- Which states have highest prescriber concentration?
- How do hospital quality metrics compare geographically?

This project simulates a real analytics environment solving those problems.

---

# Architecture

```text
CMS Medicare CSV Files
        │
        ▼
Python Ingestion Layer
(pandas + boto3)
        │
        ▼
AWS S3 Landing Zone
        │
        ▼
Snowflake RAW Schema
(COPY INTO)
        │
        ▼
dbt Staging Models
(cleaning / casting / standardization)
        │
        ▼
dbt Mart Models
(analytics-ready tables)
        │
        ▼
Tableau Public Dashboard
