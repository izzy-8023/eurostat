-- models/dimensions/dim_geography.sql
WITH geo_values AS (
    SELECT DISTINCT 
        geo_key,
        geo_label
    FROM {{ source('eurostat_raw', 'hlth_cd_ainfo') }}
    
    UNION DISTINCT
    
    SELECT DISTINCT 
        geo_key,
        geo_label
    FROM {{ source('eurostat_raw', 'hlth_cd_ainfr') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY geo_key) AS geo_id,
    geo_key,
    geo_label,
    -- Extract country code (first 2 characters of NUTS code)
    CASE 
        WHEN LENGTH(geo_key) >= 2 THEN LEFT(geo_key, 2)
        ELSE NULL
    END AS country_code,
    -- Determine NUTS level
    CASE 
        WHEN LENGTH(geo_key) = 2 THEN 'NUTS 0'
        WHEN LENGTH(geo_key) = 3 THEN 'NUTS 1'
        WHEN LENGTH(geo_key) = 4 THEN 'NUTS 2'
        WHEN LENGTH(geo_key) = 5 THEN 'NUTS 3'
        ELSE 'Unknown'
    END AS nuts_level
FROM geo_values