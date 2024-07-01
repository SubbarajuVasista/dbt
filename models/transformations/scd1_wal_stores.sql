{{ config(
    materialized='incremental',
    unique_key='store_id'
) }}

WITH latest_data AS (
    SELECT
        store_id,
        type,
        size,
        insert_date,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY insert_date DESC) AS rn
    FROM {{ ref('stg_wal_stores') }}
)

-- Selecting the latest records for each store_id
SELECT
    store_id,
    type,
    size,
    insert_date
FROM latest_data
WHERE rn = 1

{% if is_incremental() %}
    UNION ALL
    SELECT
        store_id,
        type,
        size,
        insert_date
    FROM {{ this }}
    WHERE store_id NOT IN (SELECT DISTINCT store_id FROM latest_data WHERE rn = 1)
{% endif %}
