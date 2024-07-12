{{ config(
    materialized='incremental',
    unique_key=['dept_id', 'store_id']
) }}

WITH source AS (
    SELECT * FROM {{ source('walmart', 'wal_dept') }}
)

SELECT
    dept_seq_id,
    store_id,
    dept_id,
    dept_date,
    weekly_sales,
    is_holiday,
    insert_date
FROM source

{% if is_incremental() %}
    WHERE insert_date > (SELECT MAX(insert_date) FROM {{ this }})
{% endif %}
 