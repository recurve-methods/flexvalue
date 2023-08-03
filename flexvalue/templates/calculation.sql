{% if create_clause -%}
{{ create_clause }}
{% endif -%}
WITH project_costs AS (
    SELECT
        project_info.*,
        project_info.admin_cost + (((1 - project_info.ntg) * project_info.incentive_cost) + (project_info.ntg * project_info.measure_cost)) / (1 + (project_info.discount_rate / 4.0)) as trc_costs,
        project_info.admin_cost + (project_info.incentive_cost / (1 + (project_info.discount_rate / 4.0))) as pac_costs
    FROM
    {{ project_info_table }} project_info
),
project_costs_with_discounted_elec_av AS (
    SELECT
        project_costs.*,
        elec_av_costs.hour_of_year,
        elec_av_costs.year,
        elec_av_costs.month,
        elec_av_costs.hour_of_day,
        elec_av_costs.datetime,
        elec_av_costs.quarter,
        elec_av_costs.energy, elec_av_costs.losses, elec_av_costs.ancillary_services,
        elec_av_costs.capacity, elec_av_costs.transmission, elec_av_costs.distribution,
        elec_av_costs.cap_and_trade, elec_av_costs.ghg_adder, elec_av_costs.ghg_rebalancing,
        elec_av_costs.methane_leakage, elec_av_costs.total, elec_av_costs.marginal_ghg,
        elec_av_costs.ghg_adder_rebalancing,
        1.0 / POW(1.0 + (project_costs.discount_rate / 4.0), ((elec_av_costs.year - project_costs.start_year) * 4) + elec_av_costs.quarter - project_costs.start_quarter) AS discount
    FROM project_costs
    JOIN
        {{ eac_table }} elec_av_costs
        ON elec_av_costs.utility = project_costs.utility
            AND elec_av_costs.region = project_costs.region
            {% if use_value_curve_name_for_join -%}
            AND elec_av_costs.value_curve_name = project_costs.value_curve_name
            {% endif -%}
            {% if database_type == "postgresql" -%}
            AND elec_av_costs.datetime >= make_timestamp(project_costs.start_year, (project_costs.start_quarter - 1) * 3 + 1, 1, 0, 0, 0)
            AND elec_av_costs.datetime < make_timestamp(project_costs.start_year, (project_costs.start_quarter - 1) * 3 + 1, 1, 0, 0, 0) + make_interval(project_costs.eul)
            {% else -%}
            AND elec_av_costs.datetime >= CAST(DATE(project_costs.start_year, (project_costs.start_quarter - 1) * 3 + 1, 1) AS DATETIME)
            AND elec_av_costs.datetime < CAST(DATE(CAST(project_costs.start_year + project_costs.eul AS INT), (project_costs.start_quarter - 1) * 3 + 1, 1) AS DATETIME)
            {% endif -%}
),
elec_calculations AS (
    SELECT
    pcwdea.id
    , elec_load_shape.load_shape_name
    {% for column in elec_aggregation_columns -%}
    , pcwdea.{{ column }}
    {% endfor -%}
    , pcwdea.datetime
    , SUM(pcwdea.units * pcwdea.ntg * pcwdea.mwh_savings * elec_load_shape.value * pcwdea.discount * pcwdea.total) AS electric_benefits
    {% for component in elec_components -%}
    {% if component == 'marginal_ghg' -%}
    , SUM(pcwdea.units * pcwdea.ntg * pcwdea.mwh_savings * elec_load_shape.value * pcwdea.{{component}}) AS {{component}}
    {% else -%}
    , SUM(pcwdea.units * pcwdea.ntg * pcwdea.mwh_savings * elec_load_shape.value * pcwdea.discount * pcwdea.{{component}}) AS {{component}}
    {% endif -%}
    {% endfor -%}
    , SUM(pcwdea.units * pcwdea.ntg * pcwdea.mwh_savings * elec_load_shape.value) / CAST(pcwdea.eul AS {{ float_type }}) as annual_net_mwh_savings
    , MAX(pcwdea.trc_costs) AS trc_costs
    , MAX(pcwdea.pac_costs) AS pac_costs
    , SUM(pcwdea.units * pcwdea.ntg * pcwdea.mwh_savings * elec_load_shape.value) as lifecycle_net_mwh_savings
    , SUM(pcwdea.units * pcwdea.ntg * pcwdea.mwh_savings * elec_load_shape.value * pcwdea.marginal_ghg) as lifecycle_elec_ghg_savings
    {% for field in elec_addl_fields if not field == "datetime" -%}
    , pcwdea.{{ field }}
    {% endfor -%}
    FROM project_costs_with_discounted_elec_av pcwdea
    JOIN {{ els_table }} elec_load_shape
        ON UPPER(elec_load_shape.load_shape_name) = UPPER(pcwdea.load_shape)
            AND elec_load_shape.utility = pcwdea.utility
            AND elec_load_shape.hour_of_year = pcwdea.hour_of_year
    GROUP BY pcwdea.id, pcwdea.eul, pcwdea.datetime, elec_load_shape.load_shape_name
    {% for field in elec_addl_fields if not field == "datetime" -%}
    , pcwdea.{{ field }}
    {% endfor -%}
    {%- for column in elec_aggregation_columns -%}, pcwdea.{{ column }}{% endfor -%}
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
        , gas_av_costs.datetime
    FROM project_costs
    JOIN 
      {{ gac_table }} gas_av_costs
        ON gas_av_costs.utility = project_costs.utility
            {% if use_value_curve_name_for_join -%}
            AND gas_av_costs.value_curve_name = project_costs.value_curve_name
            {% endif -%}
            {% if database_type == "postgresql" -%}
            AND gas_av_costs.datetime >= make_timestamp(project_costs.start_year, (project_costs.start_quarter - 1) * 3 + 1, 1, 0, 0, 0)
            AND gas_av_costs.datetime < make_timestamp(project_costs.start_year, (project_costs.start_quarter - 1) * 3 + 1, 1, 0, 0, 0) + make_interval(project_costs.eul)
            {% else -%}
            AND gas_av_costs.datetime >= CAST(DATE(project_costs.start_year, (project_costs.start_quarter - 1) * 3 + 1, 1) AS DATETIME)
            AND gas_av_costs.datetime < CAST(DATE(CAST(project_costs.start_year + project_costs.eul AS INT), (project_costs.start_quarter - 1) * 3 + 1, 1) AS DATETIME)
            {% endif -%}
),
gas_calculations AS (
    SELECT pcwdga.id
    , therms_profile.profile_name
    , MAX(pcwdga.trc_costs) as trc_costs
    , MAX(pcwdga.pac_costs) as pac_costs
    {% for column in gas_aggregation_columns -%}
    , pcwdga.{{ column }}
    {% endfor -%}
    , SUM(pcwdga.units * pcwdga.ntg * pcwdga.therms_savings * therms_profile.value * pcwdga.discount * pcwdga.total) as gas_benefits
    , SUM((pcwdga.units * pcwdga.therms_savings * pcwdga.ntg * therms_profile.value) / CAST(pcwdga.eul AS {{ float_type }}) ) as annual_net_therms_savings
    , SUM(pcwdga.units * pcwdga.therms_savings * pcwdga.ntg * therms_profile.value) as lifecycle_net_therms_savings
    , SUM(pcwdga.units * pcwdga.therms_savings * pcwdga.ntg * therms_profile.value * pcwdga.marginal_ghg) as lifecycle_gas_ghg_savings
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
    , pcwdga.datetime
    FROM project_costs_with_discounted_gas_av pcwdga
    JOIN {{ therms_profile_table }} therms_profile
        ON UPPER(pcwdga.therms_profile) = UPPER(therms_profile.profile_name)
            AND therms_profile.utility = pcwdga.utility
            AND therms_profile.month = pcwdga.month
    GROUP BY pcwdga.id, pcwdga.eul, pcwdga.datetime, therms_profile.profile_name
    {% for field in gas_addl_fields %}, pcwdga.{{field}} {% endfor %}
    {%- for column in gas_aggregation_columns %}, pcwdga.{{ column }}{% endfor -%}
 )

SELECT
{% if database_type == "postgresql" -%}
CASE
    WHEN elec_calculations.load_shape_name is NULL
        THEN gas_calculations.id
    ELSE elec_calculations.id
END AS id
, CASE
    WHEN MAX(COALESCE(elec_calculations.trc_costs, gas_calculations.trc_costs)) = 0 
    AND SUM(COALESCE(elec_calculations.electric_benefits, gas_calculations.gas_benefits)) > 0 
        THEN FLOAT 'inf'
    WHEN MAX(COALESCE(elec_calculations.trc_costs, gas_calculations.trc_costs)) = 0 
    AND SUM(COALESCE(elec_calculations.electric_benefits, gas_calculations.gas_benefits)) < 0 
        THEN FLOAT '-inf'
    WHEN MAX(COALESCE(elec_calculations.trc_costs, gas_calculations.trc_costs)) = 0 
    AND SUM(COALESCE(elec_calculations.electric_benefits, gas_calculations.gas_benefits)) = 0 
        THEN 0.0
    ELSE SUM(COALESCE(elec_calculations.electric_benefits, gas_calculations.gas_benefits)) / MAX(COALESCE(elec_calculations.trc_costs, gas_calculations.trc_costs))
  END as trc_ratio
, CASE
    WHEN MAX(COALESCE(elec_calculations.pac_costs, gas_calculations.pac_costs)) = 0 
    AND SUM(COALESCE(elec_calculations.electric_benefits, gas_calculations.gas_benefits)) > 0 
        THEN FLOAT 'inf'
    WHEN MAX(COALESCE(elec_calculations.pac_costs, gas_calculations.pac_costs)) = 0 
    AND SUM(COALESCE(elec_calculations.electric_benefits, gas_calculations.gas_benefits)) < 0 
        THEN FLOAT '-inf'
    WHEN MAX(COALESCE(elec_calculations.pac_costs, gas_calculations.pac_costs)) = 0 
    AND SUM(COALESCE(elec_calculations.electric_benefits, gas_calculations.gas_benefits)) = 0 
        THEN 0.0
    ELSE SUM(COALESCE(elec_calculations.electric_benefits, gas_calculations.gas_benefits)) / MAX(COALESCE(elec_calculations.pac_costs, gas_calculations.pac_costs))
  END as pac_ratio
{% else -%}
if(
    elec_calculations.load_shape_name is NULL,
    gas_calculations.id,
    elec_calculations.id
) as id
, IF(
    MAX(COALESCE(elec_calculations.trc_costs, gas_calculations.trc_costs)) = 0, 
    IF(
        SUM(COALESCE(elec_calculations.electric_benefits, gas_calculations.gas_benefits)) > 0, 
        cast("inf" as FLOAT64), 
        cast("-inf" as FLOAT64)
    ), 
    SUM(COALESCE(elec_calculations.electric_benefits, gas_calculations.gas_benefits)) / MAX(COALESCE(elec_calculations.trc_costs, gas_calculations.trc_costs))
  ) as trc_ratio
, IF(
    MAX(COALESCE(elec_calculations.pac_costs, gas_calculations.pac_costs)) = 0, 
    IF(
        SUM(COALESCE(elec_calculations.electric_benefits, gas_calculations.gas_benefits)) > 0, 
        cast("inf" as FLOAT64), 
        cast("-inf" as FLOAT64)), 
    SUM(COALESCE(elec_calculations.electric_benefits, gas_calculations.gas_benefits)) / MAX(COALESCE(elec_calculations.pac_costs, gas_calculations.pac_costs))
  ) as pac_ratio
{% endif -%}
, COALESCE(SUM(elec_calculations.electric_benefits), 0) as electric_benefits
, COALESCE(SUM(gas_calculations.gas_benefits), 0) as gas_benefits
, SUM(COALESCE(elec_calculations.electric_benefits, 0)) + SUM(COALESCE(gas_calculations.gas_benefits, 0)) as total_benefits
, MAX(COALESCE(elec_calculations.trc_costs, gas_calculations.trc_costs)) as trc_costs
, MAX(COALESCE(elec_calculations.pac_costs, gas_calculations.pac_costs)) as pac_costs
, COALESCE(SUM(elec_calculations.annual_net_mwh_savings), 0) as annual_net_mwh_savings
, COALESCE(SUM(elec_calculations.lifecycle_net_mwh_savings), 0) as lifecycle_net_mwh_savings
, COALESCE(SUM(gas_calculations.annual_net_therms_savings), 0) as annual_net_therms_savings
, COALESCE(SUM(gas_calculations.lifecycle_net_therms_savings), 0) as lifecycle_net_therms_savings
, COALESCE(SUM(elec_calculations.lifecycle_elec_ghg_savings), 0) as lifecycle_elec_ghg_savings
, COALESCE(SUM(gas_calculations.lifecycle_gas_ghg_savings), 0) as lifecycle_gas_ghg_savings
, SUM(COALESCE(elec_calculations.lifecycle_elec_ghg_savings, 0)) + SUM(COALESCE(gas_calculations.lifecycle_gas_ghg_savings, 0)) as lifecycle_total_ghg_savings
{% for column in elec_aggregation_columns -%}
, elec_calculations.{{ column }} as elec_{{ column }}
{% endfor -%}
{% for column in gas_aggregation_columns -%}
, gas_calculations.{{ column }} as gas_{{ column }}
{% endfor -%}
{% for field in elec_addl_fields -%}
, elec_calculations.{{ field }}
{% endfor -%}
{% for field in gas_addl_fields -%}
, gas_calculations.{{ field }}
{% endfor -%}
{% for comp in elec_components -%}
, COALESCE(SUM(elec_calculations.{{comp}}), 0) as {{ comp }}
{% endfor -%}
{% for comp in gas_components -%}
, COALESCE(SUM(gas_calculations.{{comp}}), 0) as {{ comp }}
{% endfor -%}
FROM
    elec_calculations
FULL JOIN 
    gas_calculations 
    ON elec_calculations.id = gas_calculations.id 
    AND elec_calculations.datetime = gas_calculations.datetime
GROUP BY 

{% if database_type == "bigquery" -%}
id
{% else -%}
CASE
    WHEN elec_calculations.load_shape_name is NULL
        THEN gas_calculations.id
    ELSE elec_calculations.id
END
{% endif -%}
{% for column in elec_aggregation_columns -%}
  , elec_calculations.{{ column }}
{% endfor -%}
{% for column in gas_aggregation_columns -%}
  , gas_calculations.{{ column }}
{% endfor -%}
{% for field in elec_addl_fields -%}
  , elec_calculations.{{ field }}
{% endfor -%}
{% for field in gas_addl_fields -%}
  , gas_calculations.{{ field }}
{% endfor -%}
{% if create_clause -%}
)
{% endif %}
