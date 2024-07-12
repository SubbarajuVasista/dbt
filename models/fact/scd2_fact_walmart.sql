{{ config(
    materialized='incremental',
    unique_key=['store_id', 'dept_id', 'Vrsn_start_date'],
    merge_update_columns = ['Vrsn_end_date', 'is_current']
) }}

WITH latest_stores AS (
    SELECT
        store_id,
        type,
        size,
        insert_date,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY insert_date DESC) AS rn
    FROM {{ ref('scd1_wal_stores') }}
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
    FROM {{ ref('scd1_wal_dept') }}
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
        NULL AS Vrsn_end_date,
        TRUE AS is_current
    FROM latest_depts d
    JOIN latest_stores s ON d.store_id = s.store_id
    WHERE d.rn = 1 AND s.rn = 1
)

{% if is_incremental() %}
, updates AS (
    SELECT
        t.store_id,
        t.dept_id,
        t.Vrsn_start_date,
        CASE
            WHEN s.store_id IS NOT NULL AND (
                t.is_holiday != s.is_holiday
                -- Add more conditions here for other columns that might change
            ) THEN FALSE
            ELSE TRUE
        END AS is_still_current
    FROM {{ this }} t
    LEFT JOIN scd2_data s
        ON t.store_id = s.store_id
        AND t.dept_id = s.dept_id
    WHERE t.is_current = TRUE
)

, final_updates AS (
    SELECT
        store_id,
        dept_id,
        Vrsn_start_date,
        CASE WHEN is_still_current = FALSE THEN CURRENT_TIMESTAMP() ELSE NULL END AS Vrsn_end_date,
        is_still_current AS is_current
    FROM updates
)

, merged AS (
    SELECT
        t.*,
        COALESCE(u.Vrsn_end_date, t.Vrsn_end_date) AS new_Vrsn_end_date,
        COALESCE(u.is_current, t.is_current) AS new_is_current
    FROM {{ this }} t
    LEFT JOIN final_updates u
        ON t.store_id = u.store_id
        AND t.dept_id = u.dept_id
        AND t.Vrsn_start_date = u.Vrsn_start_date
)
{% endif %}

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
    scd.Vrsn_end_date,
    scd.is_current
FROM scd2_data scd

{% if is_incremental() %}
UNION ALL
SELECT
    store_id,
    dept_id,
    dept_date,
    fact_date,
    temperature,
    fuel_price,
    markdown1,
    markdown2,
    markdown3,
    markdown4,
    markdown5,
    cpi,
    un_employment,
    is_holiday,
    insert_date,
    update_date,
    Vrsn_start_date,
    new_Vrsn_end_date AS Vrsn_end_date,
    new_is_current AS is_current
FROM merged
WHERE (store_id, dept_id, Vrsn_start_date) NOT IN (
    SELECT store_id, dept_id, Vrsn_start_date FROM scd2_data
)
{% endif %}