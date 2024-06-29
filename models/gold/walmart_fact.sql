{{ config(
    materialized='incremental',
    unique_key=['Store', 'Date']
) }}

WITH new_data AS (
    SELECT
        Store_id,
        fact_Date,
        Temperature,
        Fuel_Price,
        MarkDown1,
        MarkDown2,
        MarkDown3,
        MarkDown4,
        MarkDown5,
        CPI,
        Unemployment,
        IsHoliday
    FROM {{ source('chinna_walmart_project', 'wal_fact') }} f
    JOIN
        {{ source('chinna_walmart_project_silver', 'walmart_dept') }} dept
        ON f.Store_id = dept.store_id
    JOIN 
        {{ source('chinna_walmart_project_silver', 'walmart_stores') }} stores
        ON f.store_id = srores.store_id

)

SELECT * FROM new_data

{% if is_incremental() %}
    ON CONFLICT (Store, Date) DO UPDATE
    SET
        Temperature = EXCLUDED.Temperature,
        Fuel_Price = EXCLUDED.Fuel_Price,
        MarkDown1 = EXCLUDED.MarkDown1,
        MarkDown2 = EXCLUDED.MarkDown2,
        MarkDown3 = EXCLUDED.MarkDown3,
        MarkDown4 = EXCLUDED.MarkDown4,
        MarkDown5 = EXCLUDED.MarkDown5,
        CPI = EXCLUDED.CPI,
        Unemployment = EXCLUDED.Unemployment,
        IsHoliday = EXCLUDED.IsHoliday
        vrsn_end_date = CURRENT_TIMESTAMP

{% endif %}
