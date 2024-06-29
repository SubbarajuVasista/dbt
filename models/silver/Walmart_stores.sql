{{ config(
    materialized='incremental',
    unique_key=['store_id'],
    merge_update_columns=['type','size']
) }}

SELECT
    store_id,
    type,
    size,
    CURRENT_TIMESTAMP as insert_date
FROM {{ source('chinna_walmart_project', 'wal_stores') }}

{% if is_incremental() %}
    WHERE insert_date > (SELECT MAX(insert_date) FROM {{ this }})
{% endif %}


