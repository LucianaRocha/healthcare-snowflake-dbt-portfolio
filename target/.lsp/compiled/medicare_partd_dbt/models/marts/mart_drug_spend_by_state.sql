SELECT
    f.prscrbr_state_abrvtn AS state_code,
    g.state_name,
    g.region,
    f.report_year,

    SUM(f.total_drug_cost) AS total_drug_spend,
    SUM(f.total_claims) AS total_claims,
    SUM(f.total_beneficiaries) AS total_beneficiaries,
    COUNT(DISTINCT f.prscrbr_npi) AS total_providers,

    SUM(f.total_drug_cost) / NULLIF(SUM(f.total_claims), 0) AS avg_cost_per_claim,
    SUM(f.total_drug_cost) / NULLIF(SUM(f.total_beneficiaries), 0) AS cost_per_beneficiary,
    SUM(f.total_claims) / NULLIF(COUNT(DISTINCT f.prscrbr_npi), 0) AS claims_per_provider

FROM MEDICARE_ANALYTICS_DB.dbt_LFerreiradaRocha_analytics.fct_partd_prescriptions f

LEFT JOIN MEDICARE_ANALYTICS_DB.dbt_LFerreiradaRocha_analytics.dim_geography g
    ON f.prscrbr_state_abrvtn = g.state_code

GROUP BY
    f.prscrbr_state_abrvtn,
    g.state_name,
    g.region,
    f.report_year