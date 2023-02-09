INSERT INTO discount (
    project_id,
    quarter,
    discount
)
VALUES 
{% for disc in discounts %}
("{{ disc.project_id }}",
{{ disc.quarter }},
{{ disc.discount }}
){% if not loop.last %},{% endif %}{% endfor %};
