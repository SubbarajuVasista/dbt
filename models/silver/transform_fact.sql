{% set walmart_fact_mappings = '"Store", "Date_Id", "Temperature", "Fuel_Price", "MarkDown1", "MarkDown2", "MarkDown3", "MarkDown4", "MarkDown5", "Cpi", "Un_Employment", "Is_Holiday"' %}

{{ config({
    "materialized": 'table',
    "transient": true,
    "alias": 'Walmart',
    "pre_hook": dbt_macro('copy_csv_into_walmart_fact', csv_file='fact.csv', column_mappings=walmart_fact_mappings),
    "schema": 'SILVER'
}) }}

WITH transform_fact AS(
SELECT 
    Srore AS Srore,
    Date_Id AS Date_Id,
    Temperature AS Temperature,
    Fuel_Price AS Fuel_Price,
    MarkDown1 AS MarkDown1,
    MarkDown2 AS MarkDown2,
    MarkDown3 AS MarkDown3,
    MarkDown4 AS MarkDown4,
    MarkDown5 AS MarkDown5,
    Cpi  AS Cpi,
    Un_Employment AS Un_Employment,
    Is_Holiday AS Is_Holiday,
    ,INSERT_DTS AS INSERT_DTS
    ,UPDATE_DTS AS UPDATE_DTS
FROM {{source('source','Walmart_fact')}}
)

SELECT *
FROM transform_fact