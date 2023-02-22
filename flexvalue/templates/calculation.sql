SELECT p.project_id as project_id
, (p.admin_cost + (1 - p.ntg) * p.incentive_cost + p.ntg * p.measure_cost) / 1 + (p.discount_rate / 4) as trc_costs
, (p.admin_cost + p.incentive_cost) / (1 + (p.discount_rate / 4)) as pac_costs
, (p.units * p.ntg * sum(eac.total) * sum(els.value) * d.discount) as elec_benefits
, (p.units * p.ntg * sum(gac.total) * sum(tp.value) * d.discount) as gas_benefits
, p.units * p.mwh_savings * p.ntg * sum(els.value) as first_year_net_mwh_savings
FROM project_info p
-- TODO make JOIN ON region dynamic for when the eac data has region? Or will it always?
JOIN elec_av_costs eac ON p.state = eac.state AND p.utility = eac.utility AND p.region = eac.region
-- TODO make JOIN ON region dynamic for when the load shape data has region?
JOIN elec_load_shape els ON p.elec_load_shape = els.load_shape_name AND p.state = els.state AND p.utility = els.utility
-- TODO make JOIN ON region dynamic for when the gas avoided costs have region?
JOIN gas_av_costs gac on p.state = gac.state AND p.utility = gac.utility
-- TODO make JOIN ON region dynamic for when the therms profiles have region?
JOIN therms_profile tp on p.state = tp.state AND p.utility = tp.utility AND tp.profile_name = p.therms_profile
JOIN discount d on p.project_id = d.project_id
WHERE ((eac.year = p.start_year and eac.quarter >= p.start_quarter) OR (eac.year between p.start_year + 1 AND p.start_year + p.eul - 1) OR (eac.year = p.start_year + p.eul and eac.quarter < p.start_quarter))
AND eac.hour_of_year = els.hour_of_year
AND els.state = p.state AND els.utility = p.utility
AND d.year = eac.year and d.quarter = eac.quarter
AND ((gac.year = p.start_year and gac.quarter >= p.start_quarter) OR (gac.year between p.start_year +1 AND p.start_year + p.eul - 1) OR (gac.year = p.start_year + p.eul AND gac.quarter < p.start_quarter))
AND tp.state = p.state AND tp.utility = p.utility
AND gac.month = tp.month
AND d.year = gac.year and d.quarter = gac.quarter
-- TODO make GROUP BY dynamic (aggregation columns)
GROUP BY p.project_id
;