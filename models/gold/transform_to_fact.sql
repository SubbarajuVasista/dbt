{{ config({
 "materialized":'table',
 "alias":'Walmart_fact',
 "schema": 'Gold'
})}}

select
    s.store_id,
    d.dept_id,
    d.dept_date dept_date,
    f.Fact_Date fact_date,
    f.Temperature ,
    f.Fuel_Price,
    f.MarkDown1 ,
    f.MarkDown2 ,
    f.MarkDown3 ,
    f.MarkDown4 ,
    f.MarkDown5 ,
    f.Cpi ,
    f.Un_Employment ,
    f.Is_Holiday,
   CURRENT_TIMESTAMP AS insert_date,
   CURRENT_TIMESTAMP AS update_date,
   NULL AS vrsn_start_date,
   NULL AS vrsn_end_date 
  FROM {{ source('chinna_walmart_project', 'wal_fact') }} f
  inner join {{ source('chinna_walmart_project', 'wal_stores') }} s on f.Store_id = s.store_id
  inner join {{ source('chinna_walmart_project', 'wal_dept') }} d   on f.dept_id  = d.dept_id
