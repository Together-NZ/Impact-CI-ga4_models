{% macro ga4_goal_a(source_name, table_name) %}
DELETE FROM {{ source(source_name, table_name) }} WHERE
    WHERE (
     date between DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) and CURRENT_DATE()
     AND  ABS(DATE_DIFF(DATE(report_end_date), CURRENT_DATE(), DAY)) >=2
  )
{% endmacro %}