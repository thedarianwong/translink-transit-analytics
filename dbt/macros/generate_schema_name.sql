{#
  generate_schema_name.sql

  Overrides dbt's default schema naming behaviour. By default, dbt prefixes
  the custom schema with the target schema, producing names like
  STAGING_STAGING or STAGING_ANALYTICS. This macro uses the custom schema
  name directly so models land exactly where dbt_project.yml says:
    staging models  → STAGING
    mart models     → ANALYTICS

  If no custom schema is set, falls back to the target schema from profiles.yml.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
