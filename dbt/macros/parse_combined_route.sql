{#
  parse_combined_route(col)

  Normalises a raw route number field that may contain combined route entries
  like "005/006" or "049/050". Takes only the first route number and strips
  leading zeros so "005" → "5", matching TransLink's public-facing route IDs.

  Examples:
    "005"      → "5"
    "005/006"  → "5"    (secondary route discarded; flag is_combined_route = TRUE)
    "049/050"  → "49"
    "R4"       → "R4"   (RapidBus — prefix preserved, no leading-zero strip)
    "N19"      → "N19"  (NightBus — prefix preserved)

  Usage in a SELECT:
    {{ parse_combined_route('Lineno_renamed') }} as route_number
#}

{% macro parse_combined_route(col) %}
    case
        -- RapidBus (R prefix) and NightBus (N prefix) — keep as-is
        when {{ col }} ilike 'R%' or {{ col }} ilike 'N%'
            then trim({{ col }})
        -- Combined routes (e.g. "005/006") — take the part before "/"
        when {{ col }} like '%/%'
            then trim(to_varchar(try_to_number(split_part({{ col }}, '/', 1))))
        -- Standard numeric route — strip leading zeros via numeric cast
        else
            trim(to_varchar(try_to_number({{ col }})))
    end
{% endmacro %}
