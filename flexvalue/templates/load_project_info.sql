INSERT INTO project_info (
    project_id,
    mwh_savings,
    therms_savings,
    elec_load_shape,
    therms_profile,
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
    incentive_cost
)
VALUES 
{% for item in projects %}
("{{ item.project_id}}",
{{item.mwh_savings}},
{{item.therms_savings}},
"{{item.elec_load_shape}}",
"{{item.therms_profile}}",
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
{{item.incentive_cost}})
{% if not loop.last %},{% endif %}
{% endfor %}
;