SELECT
    f.prscrbr_npi,
    f.prscrbr_first_name,
    f.prscrbr_last_org_name,
    f.prscrbr_city,
    f.prscrbr_state_abrvtn,

    g.state_name,

    s.specialty,
    s.specialty_group,

    f.report_year,

    SUM(f.total_drug_cost)        AS total_drug_spend,
    SUM(f.total_claims)           AS total_claims,
    SUM(f.total_beneficiaries)    AS total_beneficiaries,

    SUM(f.total_drug_cost) / NULLIF(SUM(f.total_claims), 0) AS avg_cost_per_claim

FROM {{ ref('fct_partd_prescriptions') }} f

LEFT JOIN {{ ref('dim_geography') }} g
    ON f.prscrbr_state_abrvtn = g.state_code

LEFT JOIN {{ ref('dim_specialty') }} s
    ON f.prscrbr_type = s.specialty

GROUP BY
    f.prscrbr_npi,
    f.prscrbr_first_name,
    f.prscrbr_last_org_name,
    f.prscrbr_city,
    f.prscrbr_state_abrvtn,
    g.state_name,
    s.specialty,
    s.specialty_group,
    f.report_year