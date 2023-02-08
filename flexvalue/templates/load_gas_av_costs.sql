INSERT INTO gas_av_costs (
    utility,
    year,
    month,
    total,
)
VALUES 
{% for cost in av_costs %}
("{{ cost.utility }}",
{{cost.year}},
{{cost.month}},
{{cost.total}},
) {% if not loop.last %},{% endif %};
