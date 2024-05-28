{{ config({ "materialized":'table',
 "transient":true,
 "alias":'Walmart_Stores',
 "pre_hook": macros_copy_csv('Wal_Stores','s3://snowflake-data-bucket01/data/stores.csv'),
 "schema": 'BRONZE'
})}}

select
    cast(null as string) as Store,
    cast(null as string) as Type,
    cast(null as number) as Size
where false
