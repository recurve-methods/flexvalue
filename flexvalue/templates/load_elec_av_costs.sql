INSERT INTO elec_av_costs (
    utility,
    region,
    year,
    hour_of_year,
    total,
    marginal_ghg
)
VALUES 
{% for cost in costs %}
("{{ cost.utility }}",
"{{ cost.region }}",
{{cost.year}},
{{cost.hour_of_year}},
{{cost.total}},
{{cost.marginal_ghg}}
) {% if not loop.last %},{% endif %};
