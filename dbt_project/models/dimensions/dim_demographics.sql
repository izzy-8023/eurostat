-- models/dimensions/dim_demographics.sql
WITH sex_values AS (
    SELECT DISTINCT 
        sex_key,
        sex_label
    FROM {{ source('eurostat_raw', 'hlth_cd_ainfo') }}
    
    UNION DISTINCT
    
    SELECT DISTINCT 
        sex_key,
        sex_label
    FROM {{ source('eurostat_raw', 'hlth_cd_ainfr') }}
    
    UNION DISTINCT
    
    SELECT DISTINCT 
        sex_key,
        sex_label
    FROM {{ source('eurostat_raw', 'hlth_cd_yinfo') }}
    
    UNION DISTINCT
    
    SELECT DISTINCT 
        sex_key,
        sex_label
    FROM {{ source('eurostat_raw', 'hlth_cd_yinfr') }}
),

age_values AS (
    SELECT DISTINCT 
        age_key,
        age_label
    FROM {{ source('eurostat_raw', 'hlth_cd_ainfo') }}
    
    UNION DISTINCT
    
    SELECT DISTINCT 
        age_key,
        age_label
    FROM {{ source('eurostat_raw', 'hlth_cd_ainfr') }}
    
    UNION DISTINCT
    
    SELECT DISTINCT 
        age_key,
        age_label
    FROM {{ source('eurostat_raw', 'hlth_cd_yinfo') }}
    
    UNION DISTINCT
    
    SELECT DISTINCT 
        age_key,
        age_label
    FROM {{ source('eurostat_raw', 'hlth_cd_yinfr') }}
),

-- Cartesian product of sex and age to get all combinations
demographics_cross AS (
    SELECT
        sex_values.sex_key,
        sex_values.sex_label,
        age_values.age_key,
        age_values.age_label
    FROM sex_values
    CROSS JOIN age_values
)

SELECT
    ROW_NUMBER() OVER (ORDER BY sex_key, age_key) AS demographic_id,
    sex_key,
    sex_label,
    age_key,
    age_label,
    -- You can add derived fields or categories here
    CASE 
        WHEN age_key = 'Y_LT5' THEN 'Under 5'
        WHEN age_key = 'Y_GE85' THEN '85 and over'
        WHEN age_key LIKE 'Y%' THEN 'Other Age Group'
        ELSE 'Unknown'
    END AS age_category
FROM demographics_cross