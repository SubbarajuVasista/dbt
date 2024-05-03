{% set walmart_stores_mappings = '"Store", "Type", "Size"' %}

{{
    config(
        {
            "materialized": "table",
            "transient": true,
            "alias": "Walmart",
            "pre_hook": {{ref(
                "copy_csv_into_walmart_stores",
                csv_file="stores.csv",
                column_mappings=walmart_stores_mappings,
            )}},
            "schema": "SILVER",
        }
    )
}}

with
    transform_stores as (
        select
            store as store,
            type as type,
            size as size,
            insert_dts as insert_dts,
            update_dts as update_dts
        from {{ source("source", "Walmart_Stores") }}
    )

select *
from transform_stores
