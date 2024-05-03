{% set walmart_stores_mappings = '"Store", "Type", "Size"' %}

{{ config({
    "materialized": 'table',
    "transient": true,
    "alias": 'Walmart',
    "pre_hook": dbt_macro('copy_csv_into_walmart_stores', csv_file='stores.csv', column_mappings=walmart_stores_mappings),
    "schema": 'SILVER'
}) }}

WITH transform_stores AS(
SELECT 
    Store AS Store
    ,Type AS Type
    ,Size AS Size
    ,INSERT_DTS AS INSERT_DTS
    ,UPDATE_DTS AS UPDATE_DTS
FROM {{source('source','Walmart_Stores')}}
)

SELECT *
FROM transform_stores