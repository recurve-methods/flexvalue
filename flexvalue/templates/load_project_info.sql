INSERT INTO project_info (
    project_id,
    elec_load_shape,
    start_year,
    start_quarter,
    utility,
    region,
    units,
    eul,
    ntg,
    discount_rate,
    admin_cost,
    measure_cost,
    incentive_cost,
    therms_profile,
    therms_savings,
    mwh_savings
)
VALUES 
{% for item in projects %}
("{{ item.project_id}}",
"{{item.elec_load_shape}}",
{{item.start_year}},
{{item.start_quarter}},
"{{item.utility}}",
"{{item.region}}",
{{item.units}},
{{item.eul}},
{{item.ntg}},
{{item.discount_rate}},
{{item.admin_cost}},
{{item.measure_cost}},
{{item.incentive_cost}},
"{{item.therms_profile}}",
{{item.therms_savings}},
{{item.mwh_savings}})
{% if not loop.last %},{% endif %}
{% endfor %}
;