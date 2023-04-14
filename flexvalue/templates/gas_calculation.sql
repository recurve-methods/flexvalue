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
        , gas_av_costs.environment, gas_av_costs.btm_methane, gas_av_costs.upstream_methane
        , gas_av_costs.marginal_ghg
        , 1.0 / POW(1.0 + (project_costs.discount_rate / 4.0), ((gas_av_costs.year - project_costs.start_year) * 4) + gas_av_costs.quarter - project_costs.start_quarter) AS discount
        , gas_av_costs.timestamp
    FROM project_costs
    JOIN {{ gac_table }} gas_av_costs
        ON gas_av_costs.utility = project_costs.utility
            {% if database_type == "postgresql" %}
            AND gas_av_costs.timestamp >= make_timestamptz(project_costs.start_year, (project_costs.start_quarter - 1) * 3 + 1, 1, 0, 0, 0, 'UTC')
            AND gas_av_costs.timestamp < make_timestamptz(project_costs.start_year, (project_costs.start_quarter - 1) * 3 + 1, 1, 0, 0, 0, 'UTC') + make_interval(project_costs.eul)
            {% else %}
            AND gas_av_costs.timestamp >= CAST(DATE(project_costs.start_year, (project_costs.start_quarter - 1) * 3 + 1, 1) AS TIMESTAMP)
            AND gas_av_costs.timestamp < CAST(DATE(project_costs.start_year + project_costs.eul, (project_costs.start_quarter - 1) * 3 + 1, 1) AS TIMESTAMP)
            {% endif %}
),
gas_calculations AS (
    SELECT pcwdga.project_id
    {% if gas_aggregation_columns %}, {{ gas_aggregation_columns }} {% endif %}
    , SUM(pcwdga.units * pcwdga.ntg * pcwdga.therms_savings * therms_profile.value * pcwdga.discount) as gas_savings
    , SUM(pcwdga.units * pcwdga.ntg * pcwdga.therms_savings * therms_profile.value * pcwdga.discount * pcwdga.total) as gas_benefits
    , SUM((pcwdga.units * pcwdga.therms_savings * pcwdga.ntg * therms_profile.value) / CAST(pcwdga.eul AS {{ float_type }}) ) as first_year_net_therms_savings
    , SUM(pcwdga.units * pcwdga.therms_savings * pcwdga.ntg * therms_profile.value) as lifecyle_net_therms_savings
    , SUM(pcwdga.units * pcwdga.therms_savings * pcwdga.ntg * therms_profile.value * 0.006) as lifecycle_gas_ghg_savings
    , SUM(pcwdga.units * pcwdga.therms_savings * pcwdga.ntg * therms_profile.value)
    {% if include_addl_fields -%}
    , pcwdga.total
    , pcwdga.discount
    , pcwdga.therms_savings
    , pcwdga.total * pcwdga.discount as av_csts_levelized
    , pcwdga.market
    , pcwdga.btm_methane
    {% endif %}
    FROM project_costs_with_discounted_gas_av pcwdga
    JOIN {{ therms_profile_table }} therms_profile
        ON UPPER(pcwdga.therms_profile) = UPPER(therms_profile.profile_name)
            AND therms_profile.utility = pcwdga.utility
            AND therms_profile.month = pcwdga.month
    GROUP BY pcwdga.project_id, pcwdga.eul
    {% if include_addl_fields %}, pcwdga.total {% endif %}
    {%- if gas_aggregation_columns %}, {{ gas_aggregation_columns }}{% endif %}
 )

SELECT
gas_calculations.project_id
, gas_calculations.year
, gas_calculations.month
, gas_calculations.quarter
, gas_calculations.total
, gas_calculations.t_d
, gas_calculations.environment
, gas_calculations.upstream_methane
{% if include_addl_fields %}
, gas_calculations.total
, gas_calculations.discount
, gas_calculations.therms_savings
, gas_calculations.market
, gas_calculations.av_csts_levelized
, gas_calculations.btm_methane
{% endif %}
, gas_calculations.btm_methane
, (elec_calculations.electric_benefits + gas_calculations.gas_benefits) / elec_calculations.trc_costs as trc_ratio
, (elec_calculations.electric_benefits + gas_calculations.gas_benefits) / elec_calculations.pac_costs as pac_ratio
, elec_calculations.electric_benefits
, gas_calculations.gas_benefits
, elec_calculations.electric_benefits + gas_calculations.gas_benefits as total_benefits
, elec_calculations.trc_costs
, elec_calculations.pac_costs
, elec_calculations.first_year_net_mwh_savings
, elec_calculations.project_lifecycle_mwh_savings
, gas_calculations.first_year_net_therms_savings
, gas_calculations.lifecyle_net_therms_savings
, elec_calculations.elec_avoided_ghg
, gas_calculations.lifecycle_gas_ghg_savings
, elec_calculations.elec_avoided_ghg + gas_calculations.lifecycle_gas_ghg_savings as lifecycle_total_ghg_savings
{% if include_addl_fields -%}
, elec_calculations.utility
, elec_calculations.region
, elec_calculations.total * elec_calculations.discount as av_csts_levelized
{% endif %}
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
{% endif %}
{% if show_gas_components -%}
, gas_calculations.gas_savings
{% endif %}
FROM
elec_calculations
LEFT JOIN gas_calculations ON elec_calculations.project_id = gas_calculations.project_id AND elec_calculations.timestamp = gas_calculations.timestamp
{% if create_clause %}
)
{% endif %}
