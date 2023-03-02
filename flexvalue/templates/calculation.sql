WITH elec_cte AS MATERIALIZED (
    SELECT p.project_id as project_id,
        (
            p.units * p.ntg * sum(eac.total * els.value * d.discount)
        ) AS elec_benefits,
        p.units * p.mwh_savings * p.ntg * sum(els.value) as first_year_net_mwh_savings,
        p.units * p.mwh_savings * p.ntg * sum(els.value) * p.eul as project_lifecycle_elec_savings,
        (
            p.admin_cost + (1 - p.ntg) * p.incentive_cost + p.ntg * p.measure_cost
        ) / 1 + (p.discount_rate / 4) as trc_costs,
        (p.admin_cost + p.incentive_cost) / (1 + (p.discount_rate / 4)) as pac_costs,
        (
            p.units * p.ntg * sum(eac.marginal_ghg * els.value)
        ) as elec_avoided_ghg
    FROM project_info p
        JOIN discount d ON p.project_id = d.project_id
		JOIN elec_load_shape els ON p.elec_load_shape = els.load_shape_name
        JOIN elec_av_costs eac ON p.region = eac.region
        AND p.utility = eac.utility
        AND eac.date_time >= p.start_date
        AND eac.date_time < p.end_date
		AND d.year = eac.year
        AND d.quarter = eac.quarter
        AND eac.hoy_util_st = els.hoy_util_st
    GROUP BY p.project_id
),
gas_cte as MATERIALIZED (SELECT p.project_id as project_id
, (p.units * p.ntg * sum(gac.total * tp.value * d.discount)) as gas_benefits
, p.units * p.therms_savings * ntg * sum(tp.value) as first_year_net_therms_savings
, p.units * p.therms_savings * ntg * sum(tp.value) * p.eul as project_lifecycle_therms_savings
, (p.units * p.ntg * sum(gac.marginal_ghg * tp.value)) as gas_avoided_ghg
FROM project_info p
JOIN discount d
  on p.project_id = d.project_id

JOIN therms_profile tp
  on p.state = tp.state
 AND p.utility = tp.utility
 AND tp.profile_name = p.therms_profile
 AND d.quarter = tp.quarter

JOIN gas_av_costs gac
  on p.state = gac.state
 AND p.utility = gac.utility
 AND d.year = gac.year
 AND d.quarter = gac.quarter
 AND (
	(gac.year = p.start_year AND gac.quarter >= p.start_quarter)
	OR (gac.year between p.start_year +1 AND p.start_year + p.eul - 1)
	OR (gac.year = p.start_year + p.eul AND gac.quarter < p.start_quarter)
	)
 AND gac.month = tp.month

GROUP BY p.project_id
 )

SELECT p.project_id,
    c.elec_benefits,
    c.project_lifecycle_elec_savings,
    c.trc_costs,
    c.pac_costs,
    c.elec_avoided_ghg,
	gas_benefits,
	first_year_net_therms_savings,
	project_lifecycle_therms_savings,
	gas_avoided_ghg
FROM project_info p
    JOIN elec_cte c ON p.project_id = c.project_id
	JOIN gas_cte g on p.project_id = g.project_id
;