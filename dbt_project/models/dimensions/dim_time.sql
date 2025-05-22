-- models/dimensions/dim_time.sql
WITH time_values AS (
    SELECT DISTINCT 
        time_key AS time_key,
        time_label AS time_label
    FROM {{ source('eurostat_raw', 'hlth_cd_ainfo') }}
    
    UNION DISTINCT
    
    SELECT DISTINCT 
        time_key,
        time_label
    FROM {{ source('eurostat_raw', 'hlth_cd_ainfr') }}
    
    UNION DISTINCT
    
    SELECT DISTINCT 
        time_key,
        time_label
    FROM {{ source('eurostat_raw', 'hlth_cd_yinfo') }}
    
    UNION DISTINCT
    
    SELECT DISTINCT 
        time_key,
        time_label
    FROM {{ source('eurostat_raw', 'hlth_cd_yinfr') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY time_key) AS time_id,
    time_key,
    time_label,
    -- You can add additional time-related fields here
    CAST(time_key AS INTEGER) AS year,
    CASE 
        WHEN CAST(time_key AS INTEGER) % 4 = 0 THEN 'Leap Year'
        ELSE 'Regular Year'
    END AS year_type
FROM time_values