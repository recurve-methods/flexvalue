{% if create_clause -%}
{{ create_clause }}
{% endif %}
WITH project_costs AS (
    SELECT
        project_info.*,
        project_info.admin_cost + (((1 - project_info.ntg) * project_info.incentive_cost) + (project_info.ntg * project_info.measure_cost)) / (1 + (project_info.discount_rate / 4.0)) as trc_costs,
        project_info.admin_cost + (project_info.incentive_cost / (1 + (project_info.discount_rate / 4.0))) as pac_costs
    FROM
    {{ project_info_table }} project_info
)
, project_costs_with_discounted_gas_av AS (
    SELECT
        project_costs.*
        , gas_av_costs.year
        , gas_av_costs.month
        , gas_av_costs.quarter
        , gas_av_costs.total, gas_av_costs.market, gas_av_costs.t_d
        , gas_av_costs.environment, gas_av_costs.btm_methane, gas_av_costs.upstream_methane, gas_av_costs.marginal_ghg
        , 1.0 / POW(1.0 + (project_costs.discount_rate / 4.0), ((gas_av_costs.year - project_costs.start_year) * 4) + gas_av_costs.quarter - project_costs.start_quarter) AS discount
        , ((gas_av_costs.year - project_costs.start_year) * 4) + gas_av_costs.quarter - project_costs.start_quarter + 1 as eul_quarter
        , gas_av_costs.datetime
    FROM project_costs
    JOIN 
      {{ gac_table }} gas_av_costs
        ON gas_av_costs.utility = project_costs.utility
            {% if use_value_curve_name_for_join -%}
            AND gas_av_costs.value_curve_name = project_costs.value_curve_name
            {% endif -%}
            {% if database_type == "postgresql" %}
            AND gas_av_costs.datetime >= make_timestamp(project_costs.start_year, (project_costs.start_quarter - 1) * 3 + 1, 1, 0, 0, 0)
            AND gas_av_costs.datetime < make_timestamp(project_costs.start_year, (project_costs.start_quarter - 1) * 3 + 1, 1, 0, 0, 0) + make_interval(project_costs.eul)
            {% else %}
            AND gas_av_costs.datetime >= CAST(DATE(project_costs.start_year, (project_costs.start_quarter - 1) * 3 + 1, 1) AS DATETIME)
            AND gas_av_costs.datetime < CAST(DATE(CAST(project_costs.start_year + project_costs.eul AS INT), (project_costs.start_quarter - 1) * 3 + 1, 1) AS DATETIME)
            {% endif %}
),
gas_calculations AS (
    SELECT pcwdga.id
    , pcwdga.total
    {% for column in gas_aggregation_columns -%}
    , pcwdga.{{ column }}
    {% endfor -%}
    , SUM(pcwdga.units * pcwdga.ntg * pcwdga.therms_savings * therms_profile.value * pcwdga.discount * pcwdga.total) as gas_benefits
    , SUM((pcwdga.units * pcwdga.therms_savings * pcwdga.ntg * therms_profile.value) / CAST(pcwdga.eul AS {{ float_type }}) ) as annual_net_therms_savings
    , SUM(pcwdga.units * pcwdga.therms_savings * pcwdga.ntg * therms_profile.value) as lifecycle_net_therms_savings
    , SUM(pcwdga.units * pcwdga.therms_savings * pcwdga.ntg * therms_profile.value * pcwdga.marginal_ghg) as lifecycle_gas_ghg_savings
    , therms_profile.value as therms_profile_value
    , MAX(pcwdga.trc_costs) AS trc_costs
    , MAX(pcwdga.pac_costs) AS pac_costs
    {% for component in gas_components -%}
    {% if component == 'marginal_ghg' %}
    , SUM(pcwdga.units * pcwdga.ntg * pcwdga.therms_savings * therms_profile.value * pcwdga.{{component}}) as {{component}}
    {% else %}
    , SUM(pcwdga.units * pcwdga.ntg * pcwdga.therms_savings * therms_profile.value * pcwdga.discount * pcwdga.{{component}}) as {{component}}
    {% endif %}
    {% endfor -%}
    {% for field in gas_addl_fields -%}
    , pcwdga.{{ field }}
    {% endfor -%}
    FROM project_costs_with_discounted_gas_av pcwdga
    JOIN {{ therms_profile_table }} therms_profile
        ON UPPER(pcwdga.therms_profile) = UPPER(therms_profile.profile_name)
            AND therms_profile.utility = pcwdga.utility
            AND therms_profile.month = pcwdga.month
    GROUP BY pcwdga.id, pcwdga.eul, pcwdga.total, therms_profile.value
    {% for field in gas_addl_fields -%}
    , pcwdga.{{ field }}
    {% endfor -%}
    {%- for column in gas_aggregation_columns %}, pcwdga.{{ column }}{% endfor %}
)

SELECT
gas_calculations.id
, SUM(gas_calculations.total) as total
{% if database_type == "postgresql" -%}
, CASE
    WHEN MAX(gas_calculations.trc_costs) = 0 AND SUM(gas_calculations.gas_benefits) > 0 then FLOAT 'inf'
    WHEN MAX(gas_calculations.trc_costs) = 0 AND SUM(gas_calculations.gas_benefits) < 0 then FLOAT '-inf'
    WHEN MAX(gas_calculations.trc_costs) = 0 AND SUM(gas_calculations.gas_benefits) = 0 then 0.0
    ELSE SUM(gas_calculations.gas_benefits) / MAX(gas_calculations.trc_costs)
  END as trc_ratio
, CASE
    WHEN MAX(gas_calculations.pac_costs) = 0 AND SUM(gas_calculations.gas_benefits) > 0 then FLOAT 'inf'
    WHEN MAX(gas_calculations.pac_costs) = 0 AND SUM(gas_calculations.gas_benefits) < 0 then FLOAT '-inf'
    WHEN MAX(gas_calculations.pac_costs) = 0 AND SUM(gas_calculations.gas_benefits) = 0 then 0.0
    ELSE SUM(gas_calculations.gas_benefits) / MAX(gas_calculations.pac_costs)
  END as pac_ratio
{% else -%}
, IF (MAX(gas_calculations.trc_costs) = 0, IF(SUM(gas_calculations.gas_benefits) > 0, cast("inf" as {{ float_type }}), cast("-inf" as {{ float_type }})), SUM(gas_calculations.gas_benefits) / MAX(gas_calculations.trc_costs)) as trc_ratio
, IF (MAX(gas_calculations.pac_costs) = 0, IF(SUM(gas_calculations.gas_benefits) > 0, cast("inf" as {{ float_type }}), cast("-inf" as {{ float_type }})), SUM(gas_calculations.gas_benefits) / MAX(gas_calculations.pac_costs)) as pac_ratio
{% endif -%}
, SUM(gas_calculations.gas_benefits) as gas_benefits
, MAX(gas_calculations.trc_costs) as trc_costs
, MAX(gas_calculations.pac_costs) as pac_costs
, SUM(gas_calculations.annual_net_therms_savings) as annual_net_therms_savings
, SUM(gas_calculations.lifecycle_net_therms_savings) as lifecycle_net_therms_savings
, MAX(gas_calculations.therms_profile_value) as therms_profile_value
, SUM(gas_calculations.lifecycle_gas_ghg_savings) as lifecycle_gas_ghg_savings
{% for field in gas_addl_fields -%}
, gas_calculations.{{ field }}
{% endfor -%}
{% for column in gas_aggregation_columns -%}
, gas_calculations.{{ column }}
{% endfor -%}
{% for component in gas_components -%}
, SUM(gas_calculations.{{ component }}) as {{ component }}
{% endfor -%}
FROM
  gas_calculations
GROUP BY
  gas_calculations.id
{% for column in gas_aggregation_columns -%}
, gas_calculations.{{ column }}
{% endfor -%}
{% for field in gas_addl_fields -%}
, gas_calculations.{{ field }}
{% endfor -%}
{% if create_clause -%}
)
{% endif %}
