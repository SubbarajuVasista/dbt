{{ config({
 "materialized":'table',
 "alias":'Walmart_Dept',
 "schema": 'Silver'
})}}

select
   store_id,
   dept_id,
   dept_date,
   weekly_sales,
   is_holiday,
   CURRENT_TIMESTAMP AS insert_date,
   CURRENT_TIMESTAMP AS update_date
  FROM {{ source('chinna_walmart_project', 'wal_dept') }}