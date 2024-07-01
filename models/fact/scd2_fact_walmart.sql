{{ config(
    materialized='incremental',
    unique_key=['store_id', 'dept_id', 'Vrsn_start_date']
) }}

WITH latest_stores AS (
    SELECT
        store_id,
        type,
        size,
        insert_date,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY insert_date DESC) AS rn
    FROM {{ ref('stg_wal_stores') }}
),
latest_depts AS (
    SELECT
        store_id,
        dept_id,
        dept_date,
        weekly_sales,
        is_holiday,
        insert_date,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY insert_date DESC) AS rn
    FROM {{ ref('stg_wal_dept') }}
),
scd2_data AS (
    SELECT
        d.store_id,
        d.dept_id,
        d.dept_date,
        CURRENT_TIMESTAMP() AS fact_date,
        NULL AS temperature,  
        NULL AS fuel_price,    
        NULL AS markdown1,
        NULL AS markdown2,
        NULL AS markdown3,
        NULL AS markdown4,
        NULL AS markdown5,
        NULL AS cpi,
        NULL AS un_employment,
        d.is_holiday,
        d.insert_date AS insert_date,
        CURRENT_TIMESTAMP() AS update_date,
        CURRENT_TIMESTAMP() AS Vrsn_start_date,
        NULL AS Vrsn_end_date
    FROM latest_depts d
    JOIN latest_stores s ON d.store_id = s.store_id
    WHERE d.rn = 1 AND s.rn = 1
)

 --Inserting new records and updating end dates of old records if necessary
SELECT
    scd.store_id,
    scd.dept_id,
    scd.dept_date,
    scd.fact_date,
    scd.temperature,
    scd.fuel_price,
    scd.markdown1,
    scd.markdown2,
    scd.markdown3,
    scd.markdown4,
    scd.markdown5,
    scd.cpi,
    scd.un_employment,
    scd.is_holiday,
    scd.insert_date,
    scd.update_date,
    scd.Vrsn_start_date,
    scd.Vrsn_end_date
FROM scd2_data scd

{% if is_incremental() %}
UNION ALL
SELECT
    f.store_id,
    f.dept_id,
    f.dept_date,
    f.fact_date,
    f.temperature,
    f.fuel_price,
    f.markdown1,
    f.markdown2,
    f.markdown3,
    f.markdown4,
    f.markdown5,
    f.cpi,
    f.un_employment,
    f.is_holiday,
    f.insert_date,
    f.update_date,
    f.Vrsn_start_date,
    f.Vrsn_end_date
FROM {{ this }} f
WHERE f.Vrsn_end_date IS NULL
  AND NOT EXISTS (
      SELECT 1
      FROM scd2_data scd
      WHERE f.store_id = scd.store_id
        AND f.dept_id = scd.dept_id
        AND f.Vrsn_start_date = scd.Vrsn_start_date
  )
{% endif %}
