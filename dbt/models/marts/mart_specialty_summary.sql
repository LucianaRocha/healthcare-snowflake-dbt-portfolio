SELECT
    s.specialty,
    s.specialty_group,

    f.report_year,

    SUM(f.total_drug_cost)        AS total_drug_spend,
    SUM(f.total_claims)           AS total_claims,
    SUM(f.total_beneficiaries)    AS total_beneficiaries,
    COUNT(DISTINCT f.prscrbr_npi) AS total_prescribers,

    SUM(f.total_drug_cost) / NULLIF(SUM(f.total_claims), 0) AS avg_cost_per_claim

FROM {{ ref('fct_partd_prescriptions') }} f

LEFT JOIN {{ ref('dim_specialty') }} s
    ON f.prscrbr_type = s.specialty

GROUP BY
    s.specialty,
    s.specialty_group,
    f.report_year