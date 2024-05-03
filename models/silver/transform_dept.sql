{% set walmart_dept_mappings = '"Store", "Dept", "Weekly_Sales", "Is_Holiday"' %}

{{ config({
    "materialized": "table",
    "transient": true,
    "alias": "Walmart",
    "pre_hook": ref('copy_csv_into_walmart_dept', csv_file='department.csv', column_mappings=walmart_dept_mappings),
    "schema": "SILVER"
}) }}

WITH transform_dept AS(
SELECT 
    Store AS Store
    ,Dept AS Type
    ,Weekly_Sales AS Weekly_Sales
    ,Is_Holiday AS Is_Holiday
    ,INSERT_DTS AS INSERT_DTS
    ,UPDATE_DTS AS UPDATE_DTS
FROM {{source('source','Walmart_Dept')}}
)

SELECT *
FROM transform_dept