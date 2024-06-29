{{ config(
    materialized='incremental',
    unique_key=['store_id', 'dept_id', 'dept_date'],
    merge_update_columns=['weekly_sales', 'is_holiday', 'update_date']
) }}

SELECT
    store_id,
    dept_id,
    dept_date,
    weekly_sales,
    is_holiday,
    CURRENT_TIMESTAMP as insert_date,
    CURRENT_TIMESTAMP as update_date
FROM {{ source('chinna_walmart_project', 'wal_dept') }}

{% if is_incremental() %}
    WHERE insert_date > (SELECT MAX(insert_date) FROM {{ this }})
{% endif %}