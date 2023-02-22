-- TODO make JOIN ON region dynamic for when the eac data has region? Or will it always?
-- TODO make JOIN ON region dynamic for when the load shape data has region?
-- TODO make JOIN ON region dynamic for when the therms profiles have region?
-- TODO make GROUP BY dynamic (aggregation columns)
SELECT p.project_id as project_id
, (p.units * p.ntg * sum(eac.total * els.value) * d.discount) AS elec_benefits
, (p.units * p.ntg * sum(gac.total * tp.value) * d.discount) as gas_benefits
, ((p.units * p.ntg * sum(eac.total * els.value) * d.discount) + (p.units * p.ntg * sum(gac.total * tp.value) * d.discount)) as total_benefits
, p.units * p.mwh_savings * p.ntg * sum(els.value) as first_year_net_mwh_savings
, p.units * p.mwh_savings * p.ntg * sum(els.value) * p.eul as project_lifecycle_elec_savings
, p.units * p.therms_savings * ntg * sum(tp.value) as first_year_net_therms_savings
, p.units * p.therms_savings * ntg * sum(tp.value) * p.eul as project_lifecycle_therms_savings
, (p.admin_cost + (1 - p.ntg) * p.incentive_cost + p.ntg * p.measure_cost) / 1 + (p.discount_rate / 4) as trc_costs
, (p.admin_cost + p.incentive_cost) / (1 + (p.discount_rate / 4)) as pac_costs
, ((p.units * p.ntg * sum(eac.total * els.value) * d.discount) + (p.units * p.ntg * sum(gac.total * tp.value) * d.discount)) / (p.admin_cost + (1 - p.ntg) * p.incentive_cost + p.ntg * p.measure_cost) / 1 + (p.discount_rate / 4) AS trc_ratio
, ((p.units * p.ntg * sum(eac.total * els.value) * d.discount) + (p.units * p.ntg * sum(gac.total * tp.value) * d.discount)) / (p.admin_cost + p.incentive_cost) / (1 + (p.discount_rate / 4)) AS pac_ratio
, (p.units * p.ntg * sum(eac.marginal_ghg * els.value)) as elec_avoided_ghg
, (p.units * p.ntg * sum(gac.marginal_ghg * tp.value)) as gas_avoided_ghg
, (p.units * p.ntg * sum(eac.marginal_ghg * els.value)) + (p.units * p.ntg * sum(gac.marginal_ghg * tp.value)) as total_avoided_ghg
FROM project_info p
JOIN discount d on p.project_id = d.project_id
JOIN gas_av_costs gac on p.state = gac.state AND p.utility = gac.utility
JOIN therms_profile tp on p.state = tp.state AND p.utility = tp.utility AND tp.profile_name = p.therms_profile
JOIN elec_av_costs eac ON p.state = eac.state AND p.utility = eac.utility AND p.region = eac.region
JOIN elec_load_shape els ON p.elec_load_shape = els.load_shape_name AND p.state = els.state AND p.utility = els.utility
WHERE ((eac.year = p.start_year and eac.quarter >= p.start_quarter) OR (eac.year between p.start_year + 1 AND p.start_year + p.eul - 1) OR (eac.year = p.start_year + p.eul and eac.quarter < p.start_quarter))
AND eac.hour_of_year = els.hour_of_year AND eac.state = els.state AND eac.utility = els.utility
AND els.state = p.state AND els.utility = p.utility
AND d.year = eac.year and d.quarter = eac.quarter
AND ((gac.year = p.start_year and gac.quarter >= p.start_quarter) OR (gac.year between p.start_year +1 AND p.start_year + p.eul - 1) OR (gac.year = p.start_year + p.eul AND gac.quarter < p.start_quarter))
AND tp.state = p.state AND tp.utility = p.utility
AND gac.month = tp.month
AND d.year = gac.year and d.quarter = gac.quarter
GROUP BY p.project_id
;
