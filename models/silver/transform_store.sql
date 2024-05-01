{{ config({ "materialized":'table',
 "transient":true,
 "alias":'Walmart',
 "pre_hook": macros_copy_csv('Walmart_Stores'),
 "schema": 'SILVER'
})}}

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