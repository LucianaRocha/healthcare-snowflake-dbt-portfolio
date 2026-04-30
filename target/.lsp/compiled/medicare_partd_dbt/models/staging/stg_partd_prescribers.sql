SELECT
    prscrbr_npi,
    prscrbr_last_org_name,
    prscrbr_first_name,
    prscrbr_city,
    prscrbr_state_abrvtn,
    brnd_name,
    gnrc_name,
    tot_clms::NUMBER        AS total_claims,
    tot_benes::NUMBER       AS total_beneficiaries,
    tot_drug_cst::FLOAT     AS total_drug_cost,
    report_year
FROM MEDICARE_ANALYTICS_DB.RAW.PARTD_PRESCRIBERS