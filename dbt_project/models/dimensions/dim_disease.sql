-- models/dimensions/dim_disease.sql
WITH disease_values AS (
    SELECT DISTINCT 
        icd10_key,
        icd10_label
    FROM {{ source('eurostat_raw', 'hlth_cd_ainfo') }}
    
    UNION DISTINCT
    
    SELECT DISTINCT 
        icd10_key,
        icd10_label
    FROM {{ source('eurostat_raw', 'hlth_cd_ainfr') }}
    
    UNION DISTINCT
    
    SELECT DISTINCT 
        icd10_key,
        icd10_label
    FROM {{ source('eurostat_raw', 'hlth_cd_yinfo') }}
    
    UNION DISTINCT
    
    SELECT DISTINCT 
        icd10_key,
        icd10_label
    FROM {{ source('eurostat_raw', 'hlth_cd_yinfr') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY icd10_key) AS disease_id,
    icd10_key,
    icd10_label,
    -- Extract top-level ICD-10 category (usually first character)
    CASE 
        WHEN LENGTH(icd10_key) >= 1 THEN LEFT(icd10_key, 1)
        ELSE NULL
    END AS icd10_main_category
FROM disease_values