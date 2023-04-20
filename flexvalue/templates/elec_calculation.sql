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
    FROM project_costs
    JOIN {{ eac_table }} elec_av_costs
        ON elec_av_costs.utility = project_costs.utility
            AND elec_av_costs.region = project_costs.region
            {% if database_type == "postgresql" -%}
            AND elec_av_costs.datetime >= make_timestamptz(project_costs.start_year, (project_costs.start_quarter - 1) * 3 + 1, 1, 0, 0, 0, 'UTC')
            AND elec_av_costs.datetime < make_timestamptz(project_costs.start_year, (project_costs.start_quarter - 1) * 3 + 1, 1, 0, 0, 0, 'UTC') + make_interval(project_costs.eul)
            {% else %}
            AND elec_av_costs.datetime >= CAST(DATE(project_costs.start_year, (project_costs.start_quarter - 1) * 3 + 1, 1) AS TIMESTAMP)
            AND elec_av_costs.datetime < CAST(DATE(project_costs.start_year + project_costs.eul, (project_costs.start_quarter - 1) * 3 + 1, 1) AS TIMESTAMP)
            {% endif -%}
),
elec_calculations AS (
    SELECT
    pcwdea.project_id
    {% if elec_aggregation_columns %}, {{ elec_aggregation_columns }} {% endif %}
    , pcwdea.datetime
    , SUM(pcwdea.units * pcwdea.ntg * pcwdea.mwh_savings * elec_load_shape.value * pcwdea.discount) AS electric_savings
    , SUM(pcwdea.units * pcwdea.ntg * pcwdea.mwh_savings * elec_load_shape.value * pcwdea.discount * pcwdea.total) AS electric_benefits
    , SUM(pcwdea.units * pcwdea.ntg * pcwdea.mwh_savings * elec_load_shape.value * pcwdea.discount * pcwdea.energy) AS energy
    , SUM(pcwdea.units * pcwdea.ntg * pcwdea.mwh_savings * elec_load_shape.value * pcwdea.discount * pcwdea.losses) AS losses
    , SUM(pcwdea.units * pcwdea.ntg * pcwdea.mwh_savings * elec_load_shape.value * pcwdea.discount * pcwdea.ancillary_services) AS ancillary_services
    , SUM(pcwdea.units * pcwdea.ntg * pcwdea.mwh_savings * elec_load_shape.value * pcwdea.discount * pcwdea.capacity) AS capacity
    , SUM(pcwdea.units * pcwdea.ntg * pcwdea.mwh_savings * elec_load_shape.value * pcwdea.discount * pcwdea.transmission) AS transmission
    , SUM(pcwdea.units * pcwdea.ntg * pcwdea.mwh_savings * elec_load_shape.value * pcwdea.discount * pcwdea.distribution) AS distribution
    , SUM(pcwdea.units * pcwdea.ntg * pcwdea.mwh_savings * elec_load_shape.value * pcwdea.discount * pcwdea.cap_and_trade) AS cap_and_trade
    , SUM(pcwdea.units * pcwdea.ntg * pcwdea.mwh_savings * elec_load_shape.value * pcwdea.discount * pcwdea.ghg_adder_rebalancing) AS ghg_adder_rebalancing
    , SUM(pcwdea.units * pcwdea.ntg * pcwdea.mwh_savings * elec_load_shape.value * pcwdea.discount * pcwdea.ghg_adder) AS ghg_adder
    , SUM(pcwdea.units * pcwdea.ntg * pcwdea.mwh_savings * elec_load_shape.value * pcwdea.discount * pcwdea.ghg_rebalancing) AS ghg_rebalancing
    , SUM(pcwdea.units * pcwdea.ntg * pcwdea.mwh_savings * elec_load_shape.value * pcwdea.discount * pcwdea.methane_leakage) AS methane_leakage
    , SUM(pcwdea.units * pcwdea.ntg * pcwdea.mwh_savings * elec_load_shape.value * pcwdea.discount * pcwdea.marginal_ghg) AS marginal_ghg
    , SUM(pcwdea.units * pcwdea.mwh_savings * pcwdea.ntg * elec_load_shape.value) / CAST(pcwdea.eul AS {{ float_type }}) as first_year_net_mwh_savings
    , SUM(pcwdea.units * pcwdea.mwh_savings * pcwdea.ntg * elec_load_shape.value) as project_lifecycle_mwh_savings
    , SUM(pcwdea.trc_costs) AS trc_costs
    , SUM(pcwdea.pac_costs) AS pac_costs
    , SUM(pcwdea.marginal_ghg * pcwdea.units * pcwdea.ntg * pcwdea.mwh_savings * elec_load_shape.value) as elec_avoided_ghg
    {% if include_addl_fields -%}
    , pcwdea.utility
    , pcwdea.region
    , pcwdea.month
    , pcwdea.quarter
    , pcwdea.hour_of_day
    , pcwdea.total
    , pcwdea.discount
    {% endif -%}
    FROM project_costs_with_discounted_elec_av pcwdea
    JOIN {{ els_table}} elec_load_shape
        ON UPPER(elec_load_shape.load_shape_name) = UPPER(pcwdea.elec_load_shape)
            AND elec_load_shape.utility = pcwdea.utility
            AND elec_load_shape.hour_of_year = pcwdea.hour_of_year
    GROUP BY pcwdea.project_id, pcwdea.eul, pcwdea.datetime
    {% if include_addl_fields -%}
    , pcwdea.utility, pcwdea.region, pcwdea.month, pcwdea.quarter, pcwdea.hour_of_day, pcwdea.total, pcwdea.discount
    {% endif -%}
     {%- if elec_aggregation_columns %}, {{ elec_aggregation_columns }}{% endif %}
)

SELECT
elec_calculations.project_id
, elec_calculations.electric_benefits / elec_calculations.trc_costs as trc_ratio
, elec_calculations.electric_benefits / elec_calculations.pac_costs as pac_ratio
, elec_calculations.electric_benefits
, elec_calculations.trc_costs
, elec_calculations.pac_costs
, elec_calculations.first_year_net_mwh_savings
, elec_calculations.project_lifecycle_mwh_savings
, elec_calculations.elec_avoided_ghg
{% if include_addl_fields -%}
, elec_calculations.hour_of_year
, elec_calculations.utility
, elec_calculations.region
, elec_calculations.year
, elec_calculations.month
, elec_calculations.quarter
, elec_calculations.hour_of_day
, elec_calculations.datetime
, elec_calculations.total as elec_total
, elec_calculations.discount
, elec_calculations.total * elec_calculations.discount as av_csts_levelized
{% endif -%}
{% if show_elec_components -%}
, elec_calculations.electric_savings
, elec_calculations.energy
, elec_calculations.losses
, elec_calculations.ancillary_services
, elec_calculations.capacity
, elec_calculations.transmission
, elec_calculations.distribution
, elec_calculations.cap_and_trade
, elec_calculations.ghg_adder_rebalancing
, elec_calculations.ghg_adder
, elec_calculations.ghg_rebalancing
, elec_calculations.methane_leakage
, elec_calculations.marginal_ghg
{% endif -%}
FROM
elec_calculations
{% if create_clause -%}
)
{% endif %}
