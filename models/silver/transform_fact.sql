{% set walmart_fact_mappings = '"Store", "Date_Id", "Temperature", "Fuel_Price", "MarkDown1", "MarkDown2", "MarkDown3", "MarkDown4", "MarkDown5", "Cpi", "Un_Employment", "Is_Holiday"' %}

{{
    config(
        {
            "materialized": "table",
            "transient": true,
            "alias": "Walmart",
            "pre_hook": ref(
                "copy_csv_into_walmart_fact",
                csv_file="fact.csv",
                column_mappings=walmart_fact_mappings,
            ),
            "schema": "SILVER",
        }
    )
}}

with
    transform_fact as (
        select
            srore as srore,
            date_id as date_id,
            temperature as temperature,
            fuel_price as fuel_price,
            markdown1 as markdown1,
            markdown2 as markdown2,
            markdown3 as markdown3,
            markdown4 as markdown4,
            markdown5 as markdown5,
            cpi as cpi,
            un_employment as un_employment,
            is_holiday as is_holiday,,
            insert_dts as insert_dts,
            update_dts as update_dts
        from {{ source("source", "Walmart_fact") }}
    )

select *
from transform_fact
