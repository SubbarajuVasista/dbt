{{ config({ "materialized":'table',
 "transient":true,
 "alias":'Walmart_fact',
 "pre_hook": macros_copy_csv('Wal_fact','s3://snowflake-data-bucket01/data/fact.csv'),
 "schema": 'BRONZE'
})}}

select
    cast(null as string) as Store,
    cast(null as Date) as Date1,
    cast(null as number) as Temperature,
    cast(null as number) as Fuel_Price,
    cast(null as number) as Mark_Down1,
    cast(null as number) as Mark_Down2,
    cast(null as number) as Mark_Down3,
    cast(null as number) as Mark_Down4,
    cast(null as number) as CPI,
    cast(null as number) as Unemployment,
    cast(null as Bool) as IsHoliday
where false
