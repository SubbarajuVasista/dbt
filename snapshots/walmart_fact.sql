{% snapshot walmart_fact%}

{{
    config(
        target_schema='GOLD',
        unique_key=['Store_id, Dept_id'],
        strategy='timestamp',
        updated_at='INSERT_DATE'
    )
}}

SELECT
    f.fact_seq_id AS Date_id,
    s.Store_id,
    d.Dept_id,
    d.Weekly_Sales AS Store_Weekly_sales,
    f.Fuel_Price AS Fuel_price,
    f.Temperature AS Store_temperature,
    f.Un_Employment AS Unemployment,
    f.Cpi AS CPI,
    f.MarkDown1,
    f.MarkDown2,
    f.MarkDown3,
    f.MarkDown4,
    f.MarkDown5,
    f.INSERT_DATE AS Insert_date,
    CURRENT_TIMESTAMP() AS Update_date,
    CURRENT_TIMESTAMP() AS Vrsn_start_date,
    NULL AS Vrsn_end_date
FROM {{ source('chinna_walmart_project', 'wal_fact') }} f
JOIN {{ source('chinna_walmart_project_silver', 'walmart_stores') }} s ON f.Store_id = s.Store_id
JOIN {{ source('chinna_walmart_project_silver', 'walmart_dept') }} d ON f.Store_id = d.Store_id AND f.Is_Holiday = d.Is_Holiday
WHERE d.Dept_Date = f.Fact_Date

{% endsnapshot %}
