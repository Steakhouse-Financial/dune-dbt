{% macro source(source_name, table_name) %}
  {% set team_name = env_var('DUNE_TEAM_NAME', 'steakhouse') %}
  {% set rel = builtins.source(source_name, table_name) %}

  {# Route Materialized Views to the 'dune' catalog #}
  {% if source_name == team_name %}
    {% do return(rel.replace_path(database="dune")) %}
  {# Route all raw decoded blockchain data to the 'delta_prod' catalog #}
  {% else %}
    {% do return(rel.replace_path(database="delta_prod")) %}
  {% endif %}
{% endmacro %}
