SELECT DISTINCT
    prscrbr_type AS specialty,

    CASE
        WHEN prscrbr_type IN (
            'Internal Medicine',
            'Family Medicine',
            'Family Practice',
            'General Practice',
            'Geriatric Medicine',
            'Hospitalist',
            'Pediatric Medicine',
            'Pediatrics'
        ) THEN 'Primary Care'

        WHEN prscrbr_type IN (
            'Nurse Practitioner',
            'Physician Assistant',
            'Certified Clinical Nurse Specialist',
            'Certified Nurse Midwife',
            'Certified Registered Nurse Anesthetist (CRNA)',
            'Registered Nurse',
            'Licensed Practical Nurse',
            'Licensed Vocational Nurse',
            'Midwife',
            'Midwife, Lay',
            'Anesthesiology Assistant'
        ) THEN 'Advanced Practice'

        WHEN prscrbr_type IN (
            'Psychiatry',
            'Psychiatry & Neurology',
            'Geriatric Psychiatry',
            'Neuropsychiatry',
            'Psychologist',
            'Psychologist, Clinical',
            'Clinical Neuropsychologist',
            'Licensed Clinical Social Worker',
            'Licensed Professional Counselor',
            'Marriage and Family Therapist',
            'Counselor',
            'Social Worker',
            'Behavior Analyst',
            'Community/Behavioral Health',
            'Peer Specialist',
            'Psychoanalyst',
            'Substance Abuse Rehabilitation Facility'
        ) THEN 'Behavioral Health'

        WHEN prscrbr_type IN (
            'Medical Oncology',
            'Hematology-Oncology',
            'Gynecological Oncology',
            'Radiation Oncology',
            'Surgical Oncology',
            'Hematology',
            'Hematopoietic Cell Transplantation and Cellular Therapy'
        ) THEN 'Oncology'

        WHEN prscrbr_type IN (
            'Unknown Supplier/Provider Specialty',
            'Undefined Physician type',
            'Unknown'
        ) THEN 'Unknown'

        WHEN prscrbr_type IN (
            'Ambulatory Surgical Center',
            'Clinic or Group Practice',
            'Clinic/Center',
            'General Acute Care Hospital',
            'Health Maintenance Organization',
            'Hospital',
            'Home Health',
            'Hospice and Palliative Care',
            'Independent Diagnostic Testing Facility (IDTF)',
            'Local Education Agency (LEA)',
            'Other Clinic/Center',
            'Pharmacy',
            'Preferred Provider Organization',
            'Public Health or Welfare Agency',
            'Residential Treatment Facility, Physical Disabilities',
            'Skilled Nursing Facility',
            'Program of All-Inclusive Care for the Elderly (PACE) Provider Organization',
            'Exclusive Provider Organization'
        ) THEN 'Other'

        WHEN prscrbr_type IN (
            'Dental Assistant',
            'Dental Hygienist',
            'Dental Laboratory Technician',
            'Dental Therapist',
            'Dentist',
            'Denturist',
            'Advanced Practice Dental Therapist',
            'Maxillofacial Surgery',
            'Oral & Maxillofacial Surgery',
            'Oral Medicinist',
            'Oral Surgery (Dentist only)'
        ) THEN 'Other'

        ELSE 'Specialist'
    END AS specialty_group

FROM {{ ref('fct_partd_prescriptions') }}