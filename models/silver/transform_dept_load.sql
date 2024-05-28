{{ config({ "materialized":'table',
 "transient":true,
 "alias":'Walmart_Dept',
 "pre_hook": macros_copy_csv('Wal_Dept','s3://snowflake-data-bucket01/data/department.csv'),
 "schema": 'BRONZE'
})}}

select
    cast(null as string) as Store,
    cast(null as string) as Dept,
    cast(null as Date) as Date1,
    cast(null as number) as Weekly_Sales,
    cast(null as Bool) as IsHoliday
where false
