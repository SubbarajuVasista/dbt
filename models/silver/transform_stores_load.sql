{{ config({
 "materialized":'table',
 "alias":'Walmart_Stores',
 "schema": 'Silver'
})}}

select
   store_id,
   type,
   size,
   CURRENT_TIMESTAMP AS insert_date,
   CURRENT_TIMESTAMP AS update_date
  FROM {{ source('chinna_walmart_project', 'wal_stores') }}

