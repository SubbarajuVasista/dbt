{{ config(
    materialized            = 'incremental',
    unique_key              = 'store_id',
    merge_update_columns    = ['store_id', 'type'],
    schema                  = 'BRONZE'
) }}

WITH source AS (
    SELECT * FROM {{ source('walmart', 'wal_stores') }}
)

SELECT
    store_seq_id,
    store_id,
    type,
    size,
    insert_date
FROM source

{% if is_incremental() %}
    WHERE insert_date > (SELECT MAX(insert_date) FROM {{ this }})
{% endif %}
