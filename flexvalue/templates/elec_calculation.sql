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
        , ((elec_av_costs.year - project_costs.start_year) * 4) + elec_av_costs.quarter - project_costs.start_quarter + 1 as eul_quarter
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
            {% else %}
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
    , SUM(pcwdea.units * pcwdea.ntg * pcwdea.mwh_savings * elec_load_shape.value) as lifecycle_net_mwh_savings
    , MAX(pcwdea.trc_costs) AS trc_costs
    , MAX(pcwdea.pac_costs) AS pac_costs
    , SUM(pcwdea.units * pcwdea.ntg * pcwdea.mwh_savings * elec_load_shape.value * pcwdea.marginal_ghg) as lifecycle_elec_ghg_savings
    {% for field in elec_addl_fields if not field == "datetime" -%}
    , pcwdea.{{ field }}
    {% endfor -%}
    FROM project_costs_with_discounted_elec_av pcwdea
    JOIN {{ els_table}} elec_load_shape
        ON UPPER(elec_load_shape.load_shape_name) = UPPER(pcwdea.load_shape)
            AND elec_load_shape.utility = pcwdea.utility
            AND elec_load_shape.hour_of_year = pcwdea.hour_of_year
    GROUP BY pcwdea.id, pcwdea.eul, pcwdea.datetime, elec_load_shape.load_shape_name
    {% for field in elec_addl_fields if not field == "datetime" -%}
    , pcwdea.{{field}}
    {% endfor -%}
    {%- for column in elec_aggregation_columns %}, pcwdea.{{ column }}{% endfor %}
)

SELECT
elec_calculations.id
{% if database_type == "postgresql" -%}
, CASE
    WHEN MAX(elec_calculations.trc_costs) = 0 AND SUM(elec_calculations.electric_benefits) > 0 then FLOAT 'inf'
    WHEN MAX(elec_calculations.trc_costs) = 0 AND SUM(elec_calculations.electric_benefits) < 0 then FLOAT '-inf'
    WHEN MAX(elec_calculations.trc_costs) = 0 AND SUM(elec_calculations.electric_benefits) = 0 then 0.0
    ELSE SUM(elec_calculations.electric_benefits) / MAX(elec_calculations.trc_costs)
  END as trc_ratio
, CASE
    WHEN MAX(elec_calculations.pac_costs) = 0 AND SUM(elec_calculations.electric_benefits) > 0 then FLOAT 'inf'
    WHEN MAX(elec_calculations.pac_costs) = 0 AND SUM(elec_calculations.electric_benefits) < 0 then FLOAT '-inf'
    WHEN MAX(elec_calculations.pac_costs) = 0 AND SUM(elec_calculations.electric_benefits) = 0 then 0.0
    ELSE SUM(elec_calculations.electric_benefits) / MAX(elec_calculations.pac_costs)
  END as pac_ratio
{% else -%}
, IF (MAX(elec_calculations.trc_costs) = 0, IF(SUM(elec_calculations.electric_benefits) > 0, cast("inf" as {{ float_type }}), cast("-inf" as {{ float_type }})), SUM(elec_calculations.electric_benefits) / MAX(elec_calculations.trc_costs)) as trc_ratio
, IF (MAX(elec_calculations.pac_costs) = 0, IF(SUM(elec_calculations.electric_benefits) > 0, cast("inf" as {{ float_type }}), cast("-inf" as {{ float_type }})), SUM(elec_calculations.electric_benefits) / MAX(elec_calculations.pac_costs)) as pac_ratio
{% endif -%}
, SUM(elec_calculations.electric_benefits) as electric_benefits
, MAX(elec_calculations.trc_costs) as trc_costs
, MAX(elec_calculations.pac_costs) as pac_costs
, SUM(elec_calculations.annual_net_mwh_savings) as annual_net_mwh_savings
, SUM(elec_calculations.lifecycle_net_mwh_savings) as lifecycle_net_mwh_savings
, SUM(elec_calculations.lifecycle_elec_ghg_savings) as lifecycle_elec_ghg_savings
{% for field in elec_addl_fields -%}
, elec_calculations.{{field}}
{% endfor -%}
{% for column in elec_aggregation_columns -%}
, elec_calculations.{{ column }}
{% endfor -%}
{% for comp in elec_components -%}
, SUM(elec_calculations.{{comp}}) as {{ comp }}
{% endfor -%}
FROM
elec_calculations
GROUP BY elec_calculations.id
{% for column in elec_aggregation_columns -%}
, elec_calculations.{{ column }}
{% endfor -%}
{% for field in elec_addl_fields -%}
, elec_calculations.{{ field }}
{% endfor -%}
{% if create_clause -%}
)
{% endif %}
