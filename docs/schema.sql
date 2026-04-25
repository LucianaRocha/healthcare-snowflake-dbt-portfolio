-- ============================================================
-- Healthcare Analytics Pipeline
-- RAW Layer Schema Documentation
-- Database: HEALTHCARE_DB
-- Schema: RAW
--
-- Purpose:
-- This file documents the RAW Snowflake tables loaded from CMS
-- public healthcare datasets through the Python + AWS S3 ingestion
-- pipeline.
-- ============================================================


-- ============================================================
-- Table: RAW.PARTD_PRESCRIBERS
-- Source: CMS Medicare Part D Prescribers by Provider and Drug
-- Description:
-- Provider-level prescription drug activity for Medicare Part D,
-- including prescriber identifiers, drug names, claims, fills,
-- days supplied, beneficiary counts, and drug cost.
-- ============================================================

CREATE OR REPLACE TABLE RAW.PARTD_PRESCRIBERS (
    PRSCRBR_NPI STRING COMMENT 'National Provider Identifier for the prescribing provider.',
    PRSCRBR_LAST_ORG_NAME STRING COMMENT 'Prescriber last name or organization name.',
    PRSCRBR_FIRST_NAME STRING COMMENT 'Prescriber first name.',
    PRSCRBR_CITY STRING COMMENT 'City where the prescriber is located.',
    PRSCRBR_STATE_ABRVTN STRING COMMENT 'Two-character state abbreviation for the prescriber location.',
    PRSCRBR_STATE_FIPS STRING COMMENT 'Federal Information Processing Standard state code.',
    PRSCRBR_TYPE STRING COMMENT 'Provider specialty or prescriber type.',
    PRSCRBR_TYPE_SRC STRING COMMENT 'Source of the prescriber type classification.',
    BRND_NAME STRING COMMENT 'Brand name of the prescribed drug.',
    GNRC_NAME STRING COMMENT 'Generic name of the prescribed drug.',
    TOT_CLMS NUMBER COMMENT 'Total number of Medicare Part D claims.',
    TOT_30DAY_FILLS FLOAT COMMENT 'Total number of standardized 30-day fills.',
    TOT_DAY_SUPLY NUMBER COMMENT 'Total number of days supplied.',
    TOT_DRUG_CST FLOAT COMMENT 'Total drug cost paid under Medicare Part D.',
    TOT_BENES NUMBER COMMENT 'Total number of Medicare beneficiaries.',
    GE65_SPRSN_FLAG STRING COMMENT 'Suppression flag for age 65 and older claim counts.',
    GE65_TOT_CLMS NUMBER COMMENT 'Total claims for beneficiaries age 65 and older.',
    GE65_TOT_30DAY_FILLS FLOAT COMMENT 'Total standardized 30-day fills for beneficiaries age 65 and older.',
    GE65_TOT_DRUG_CST FLOAT COMMENT 'Total drug cost for beneficiaries age 65 and older.',
    GE65_TOT_DAY_SUPLY NUMBER COMMENT 'Total days supplied for beneficiaries age 65 and older.',
    GE65_BENE_SPRSN_FLAG STRING COMMENT 'Suppression flag for age 65 and older beneficiary counts.',
    GE65_TOT_BENES NUMBER COMMENT 'Total beneficiaries age 65 and older.'
);


-- ============================================================
-- Table: RAW.HOSPITAL_GENERAL_INFO
-- Source: CMS Hospital General Information / Hospital Quality
-- Description:
-- Hospital-level quality and measurement data including facility
-- identifiers, location, measure information, scores, estimates,
-- reporting dates, and comparison to national benchmarks.
-- ============================================================

CREATE OR REPLACE TABLE RAW.HOSPITAL_GENERAL_INFO (
    FACILITY_ID STRING COMMENT 'CMS facility identifier.',
    FACILITY_NAME STRING COMMENT 'Hospital or facility name.',
    ADDRESS STRING COMMENT 'Facility street address.',
    CITY_TOWN STRING COMMENT 'Facility city or town.',
    STATE STRING COMMENT 'Two-character state abbreviation.',
    ZIP_CODE STRING COMMENT 'Facility ZIP code.',
    COUNTY_PARISH STRING COMMENT 'County or parish where the facility is located.',
    TELEPHONE_NUMBER STRING COMMENT 'Facility telephone number.',
    MEASURE_ID STRING COMMENT 'CMS measure identifier.',
    MEASURE_NAME STRING COMMENT 'CMS measure name or description.',
    COMPARED_TO_NATIONAL STRING COMMENT 'Comparison of facility performance to the national benchmark.',
    DENOMINATOR NUMBER COMMENT 'Denominator used for the measure calculation, when applicable.',
    SCORE STRING COMMENT 'Reported measure score. Stored as string because CMS files may contain non-numeric values.',
    LOWER_ESTIMATE FLOAT COMMENT 'Lower estimate for the reported measure, when available.',
    HIGHER_ESTIMATE FLOAT COMMENT 'Higher estimate for the reported measure, when available.',
    FOOTNOTE STRING COMMENT 'CMS footnote or explanation for suppressed, unavailable, or special-case values.',
    START_DATE DATE COMMENT 'Start date of the measurement period.',
    END_DATE DATE COMMENT 'End date of the measurement period.'
);


-- ============================================================
-- Validation Queries
-- Use these after loading data to confirm RAW layer health.
-- ============================================================

-- Row count: Part D Prescribers
SELECT COUNT(*) AS row_count
FROM RAW.PARTD_PRESCRIBERS;

-- Basic profile: Part D Prescribers
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT PRSCRBR_NPI) AS distinct_prescribers,
    COUNT(DISTINCT BRND_NAME) AS distinct_brand_names,
    SUM(TOT_CLMS) AS total_claims,
    SUM(TOT_DRUG_CST) AS total_drug_cost
FROM RAW.PARTD_PRESCRIBERS;

-- Row count: Hospital General Information
SELECT COUNT(*) AS row_count
FROM RAW.HOSPITAL_GENERAL_INFO;

-- Basic profile: Hospital General Information
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT FACILITY_ID) AS distinct_facilities,
    COUNT(DISTINCT STATE) AS distinct_states,
    COUNT(DISTINCT MEASURE_ID) AS distinct_measures
FROM RAW.HOSPITAL_GENERAL_INFO;


-- ============================================================
-- Sample Queries
-- ============================================================

-- Preview Part D records
SELECT *
FROM RAW.PARTD_PRESCRIBERS
LIMIT 10;

-- Preview Hospital records
SELECT *
FROM RAW.HOSPITAL_GENERAL_INFO
LIMIT 10;

-- Top 10 drug brands by total drug cost
SELECT
    BRND_NAME,
    SUM(TOT_DRUG_CST) AS total_drug_cost,
    SUM(TOT_CLMS) AS total_claims
FROM RAW.PARTD_PRESCRIBERS
GROUP BY BRND_NAME
ORDER BY total_drug_cost DESC
LIMIT 10;

-- Hospital measure count by state
SELECT
    STATE,
    COUNT(*) AS measure_rows,
    COUNT(DISTINCT FACILITY_ID) AS distinct_facilities
FROM RAW.HOSPITAL_GENERAL_INFO
GROUP BY STATE
ORDER BY measure_rows DESC;
