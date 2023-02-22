-- TODO make JOIN ON region dynamic for when the eac data has region? Or will it always?
-- TODO make JOIN ON region dynamic for when the load shape data has region?
-- TODO make JOIN ON region dynamic for when the therms profiles have region?
-- TODO make GROUP BY dynamic (aggregation columns)

WITH elec_benefits AS (
    SELECT
    p.project_id
    , (p.units * p.ntg * sum(eac.total * els.value) * d.discount) AS benefits
    FROM project_info p
    JOIN elec_av_costs eac ON p.state = eac.state AND p.utility = eac.utility AND p.region = eac.region
    JOIN elec_load_shape els ON p.elec_load_shape = els.load_shape_name AND p.state = els.state AND p.utility = els.utility
    JOIN discount d on p.project_id = d.project_id
    WHERE ((eac.year = p.start_year and eac.quarter >= p.start_quarter) OR (eac.year between p.start_year + 1 AND p.start_year + p.eul - 1) OR (eac.year = p.start_year + p.eul and eac.quarter < p.start_quarter))
    AND eac.hour_of_year = els.hour_of_year AND eac.state = els.state AND eac.utility = els.utility
    AND els.state = p.state AND els.utility = p.utility
    AND d.year = eac.year and d.quarter = eac.quarter
    GROUP BY p.project_id
)
, gas_benefits AS (
    SELECT p.project_id as project_id
    , (p.units * p.ntg * sum(gac.total * tp.value) * d.discount) as benefits
    FROM project_info p
    JOIN gas_av_costs gac on p.state = gac.state AND p.utility = gac.utility
    JOIN therms_profile tp on p.state = tp.state AND p.utility = tp.utility AND tp.profile_name = p.therms_profile
    JOIN discount d on p.project_id = d.project_id
    WHERE ((gac.year = p.start_year and gac.quarter >= p.start_quarter) OR (gac.year between p.start_year +1 AND p.start_year + p.eul - 1) OR (gac.year = p.start_year + p.eul AND gac.quarter < p.start_quarter))
    AND tp.state = p.state AND tp.utility = p.utility
    AND gac.month = tp.month
    AND d.year = gac.year and d.quarter = gac.quarter
    GROUP BY p.project_id
)
SELECT p.project_id as project_id
, (p.admin_cost + (1 - p.ntg) * p.incentive_cost + p.ntg * p.measure_cost) / 1 + (p.discount_rate / 4) as trc_costs
, (p.admin_cost + p.incentive_cost) / (1 + (p.discount_rate / 4)) as pac_costs
, eb.benefits as "Electric Benefits"
, gb.benefits as "Gas Benefits"
, (eb.benefits + gb.benefits) as total_benefits
, p.units * p.mwh_savings * p.ntg * sum(els.value) as first_year_net_mwh_savings
FROM project_info p
JOIN elec_benefits eb ON p.project_id = eb.project_id
JOIN gas_benefits gb on p.project_id = gb.project_id
JOIN elec_av_costs eac ON p.state = eac.state AND p.utility = eac.utility AND p.region = eac.region
JOIN elec_load_shape els ON p.elec_load_shape = els.load_shape_name AND p.state = els.state AND p.utility = els.utility
JOIN gas_av_costs gac on p.state = gac.state AND p.utility = gac.utility
JOIN therms_profile tp on p.state = tp.state AND p.utility = tp.utility AND tp.profile_name = p.therms_profile
JOIN discount d on p.project_id = d.project_id
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
