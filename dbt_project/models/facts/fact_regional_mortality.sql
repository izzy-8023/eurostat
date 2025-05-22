-- models/facts/fact_regional_mortality.sql
WITH ainfo_data AS (
    SELECT
        linear_index,
        time_key,
        geo_key,
        sex_key,
        age_key,
        icd10_key,
        value AS absolute_number,
        CAST(NULL AS DOUBLE PRECISION) AS crude_rate,
        source_dataset_id
    FROM {{ source('eurostat_raw', 'hlth_cd_ainfo') }}
),

ainfr_data AS (
    SELECT
        linear_index,
        time_key,
        geo_key,
        sex_key,
        age_key,
        icd10_key,
        CAST(NULL AS DOUBLE PRECISION) AS absolute_number,
        value AS crude_rate,
        source_dataset_id
    FROM {{ source('eurostat_raw', 'hlth_cd_ainfr') }}
),

combined_data AS (
    SELECT * FROM ainfo_data
    UNION ALL
    SELECT * FROM ainfr_data
)

SELECT
    combined_data.linear_index,
    t.time_id,
    g.geo_id,
    d.demographic_id,
    dis.disease_id,
    combined_data.absolute_number,
    combined_data.crude_rate,
    combined_data.source_dataset_id
FROM combined_data
LEFT JOIN {{ ref('dim_time') }} t ON combined_data.time_key = t.time_key
LEFT JOIN {{ ref('dim_geography') }} g ON combined_data.geo_key = g.geo_key
LEFT JOIN {{ ref('dim_demographics') }} d 
    ON combined_data.sex_key = d.sex_key AND combined_data.age_key = d.age_key
LEFT JOIN {{ ref('dim_disease') }} dis ON combined_data.icd10_key = dis.icd10_key