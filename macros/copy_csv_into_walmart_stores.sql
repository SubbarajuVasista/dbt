{% macro copy_csv_into_walmart_stores(csv_file, column_mappings) %}
DELETE FROM {{ var('rawhist_db') }}.{{ var('wrk_schema') }}.Walmart_Stores;
COPY INTO {{ var('rawhist_db') }}.{{ var('wrk_schema') }}.Walmart_Stores 
FROM 
(
  SELECT
      {{ column_mappings }},
      CURRENT_TIMESTAMP() AS INSERT_DTS,
      CURRENT_TIMESTAMP() AS UPDATE_DTS,
      METADATA$FILENAME AS SOURCE_FILE_NAME,
      METADATA$FILE_ROW_NUMBER AS SOURCE_FILE_ROW_NUMBER
  FROM @{{ var('stage_name') }}/{{ csv_file }}
)
FILE_FORMAT = {{ var('file_format_csv') }}
PURGE={{ var('purge_status') }};
{% endmacro %}
