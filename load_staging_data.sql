{% macro load_staging_data() %}
    copy into {{ ref('staging_data_load_model') }}
    from @WALMART.BRONZE.S3_WALMART/s3://snowflake-data-bucket01/data/stores.csv
    file_format = (type = 'CSV' field_optionally_enclosed_by='"' skip_header = 1);
{% endmacro %}