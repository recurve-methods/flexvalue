INSERT INTO elec_load_shape (
    utility,
    load_shape_name,
    hour_of_year,
    value
)
VALUES 
{% for load_shape in load_shapes %}
("{{ load_shape.utility }}",
"{{ load_shape.load_shape_name }}",
{{load_shape.hour_of_year}},
{{load_shape.value}}
) {% if not loop.last %},{% endif %}{% endfor %};
