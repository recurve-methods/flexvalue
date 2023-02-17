SELECT p.project_id
, (p.admin_cost + (1 - p.ntg) * p.incentive_cost + p.ntg * p.measure_cost) / 1 + (p.discount_rate / 4) as trc_costs
, (p.admin_cost + p.incentive_cost) / (1 + (p.discount_rate / 4)) as pac_costs
, p.units * p.mwh_savings * p.ntg * sum(els.value) as elec_savings
--, p.units * p.mwh_savings * p.ntg * sum(eac.)
--, (p.units * p.ntg * the electric_av_costs.total for the (utility, region, years, hour of year) (all of which are in or derived from project_info) * the savings for the hour (from the load shape, and the project_info.mwh_savings) * the discount for the quarter (from the discount table) as elec_benefits
FROM project_info p
-- TODO make region join dynamic for when the load shape data has region?
JOIN elec_load_shape els ON p.elec_load_shape = els.load_shape_name AND p.state = els.state AND p.utility = els.utility
--JOIN elec_av_costs eac ON p.state = eac.state AND p.
--JOIN discount d on p.project_id = d.project_id;

-- TODO make GROUP BY dynamic (aggregation columns)
GROUP BY p.project_id
;