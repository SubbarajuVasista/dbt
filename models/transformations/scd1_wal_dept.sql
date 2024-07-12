{{ config(
    materialized='incremental',
    unique_key=['store_id', 'dept_id'],
    schema='SILVER',
    alias= 'WALMART_DEPT'
) }}

WITH latest_data AS (
    SELECT
        store_id,
        dept_id,
        dept_date,
        weekly_sales,
        is_holiday,
        insert_date,
        ROW_NUMBER() OVER (PARTITION BY store_id, dept_id ORDER BY insert_date DESC) AS rn
    FROM {{ ref('stg_wal_dept') }}
)

-- Selecting the latest records for each store_id and dept_id
SELECT
    store_id,
    dept_id,
    dept_date,
    weekly_sales,
    is_holiday,
    insert_date
FROM latest_data
WHERE rn = 1

{% if is_incremental() %}
    UNION ALL
    SELECT
        store_id,
        dept_id,
        dept_date,
        weekly_sales,
        is_holiday,
        insert_date
    FROM {{ this }}
    WHERE (store_id, dept_id) NOT IN (
        SELECT store_id, dept_id 
        FROM latest_data 
        WHERE rn = 1
    )
{% endif %}
