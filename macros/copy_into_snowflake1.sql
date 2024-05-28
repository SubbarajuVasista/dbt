{% macro macros_copy_csv(table_nm, s3_file_path) %}
    -- Deleting existing data from the table
    --delete from {{ var('rawhist_db') }}.{{ var('wrk_schema') }}.{{ table_nm }};
    -- Copying data from S3 bucket into the table
    COPY INTO {{ var('rawhist_db') }}.{{ var('wrk_schema') }}.{{ table_nm }} 
    FROM '{{ s3_file_path }}'
    FILE_FORMAT = (type = 'CSV', field_optionally_enclosed_by = '"', skip_header = 1)
    PURGE = {{ var('purge_status', 'FALSE') }};
{% endmacro %}