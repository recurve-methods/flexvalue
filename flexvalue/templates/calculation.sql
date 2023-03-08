--WITH elec_cte as (
    SELECT project_info.project_id,
    project_info.units
    * project_info.ntg
    * project_info.mwh_savings
    * SUM(elec_av_costs.total * elec_load_shape.value * discount.discount) AS electric_benefits
    , project_info.units * project_info.mwh_savings * project_info.ntg * sum(elec_load_shape.value) as first_year_net_mwh_savings
    , project_info.units * project_info.mwh_savings * project_info.ntg * sum(elec_load_shape.value) * project_info.eul as project_lifecycle_elec_savings
    , (project_info.admin_cos   t + (1 - project_info.ntg) * project_info.incentive_cost + project_info.ntg * project_info.measure_cost) / 1 + (project_info.discount_rate / 4) as trc_costs
    , (project_info.admin_cost + project_info.incentive_cost) / (1 + (project_info.discount_rate / 4)) as pac_costs
    , (project_info.units * project_info.ntg * sum(elec_av_costs.marginal_ghg * elec_load_shape.value)) as elec_avoided_ghg
    FROM project_info
--    JOIN elec_load_shape ON project_info.elec_load_shape = elec_load_shape.load_shape_name AND project_info.utility = elec_load_shape.utility
    JOIN elec_load_shape ON project_info.util_load_shape = elec_load_shape.util_load_shape
    JOIN elec_av_costs ON elec_load_shape.timestamp = elec_av_costs.timestamp AND project_info.region = elec_av_costs.region AND project_info.utility = elec_av_costs.utility
    JOIN discount ON elec_av_costs.timestamp::date = discount.date AND discount.project_id = project_info.project_id
    GROUP BY project_info.project_id
    ;
--)
-- , gas_cte as (

-- )
-- SELECT project_info.project_id
-- , elec_cte.electric_benefits
-- , elec_cte.first_year_net_mwh_savings
-- , elec_cte.project_lifecycle_elec_savings
-- , elec_cte.trc_costs
-- , elec_cte.pac_costs
-- , elec_cte.elec_avoided_ghg
-- FROM project_info
-- JOIN elec_cte ON project_info.project_id = elec_cte.project_id
-- ;
