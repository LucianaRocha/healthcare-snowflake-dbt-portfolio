SELECT
    prscrbr_npi,
    prscrbr_last_org_name,
    prscrbr_first_name,
    prscrbr_city,
    prscrbr_state_abrvtn,

    brnd_name,
    gnrc_name,

    total_claims,
    total_beneficiaries,
    total_drug_cost,

    total_drug_cost / NULLIF(total_claims, 0) AS avg_cost_per_claim,

    report_year

FROM MEDICARE_ANALYTICS_DB.dbt_LFerreiradaRocha_staging.stg_partd_prescribers