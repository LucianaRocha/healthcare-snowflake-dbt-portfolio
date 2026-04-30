SELECT
    report_year,

    brnd_name,
    gnrc_name,

    CASE 
        WHEN brnd_name = gnrc_name THEN 'Generic'
        ELSE 'Brand'
    END AS brand_generic,

    SUM(total_drug_cost)        AS total_drug_spend,
    SUM(total_claims)           AS total_claims,
    SUM(total_beneficiaries)    AS total_beneficiaries,
    COUNT(DISTINCT prscrbr_npi) AS total_prescribers,

    SUM(total_drug_cost) / NULLIF(SUM(total_claims), 0) AS avg_cost_per_claim,
    SUM(total_drug_cost) / NULLIF(SUM(total_beneficiaries), 0) AS avg_cost_per_beneficiary

FROM {{ ref('fct_partd_prescriptions') }}

GROUP BY
    report_year,
    brnd_name,
    gnrc_name,
    CASE 
        WHEN brnd_name = gnrc_name THEN 'Generic'
        ELSE 'Brand'
    END