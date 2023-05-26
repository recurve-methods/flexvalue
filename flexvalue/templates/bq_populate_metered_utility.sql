UPDATE {{ dataset}}.elec_load_shape els
SET els.utility = proj.utility
FROM {{ dataset }}.project_info proj
WHERE load_shape_name IN ({{ load_shape_names }});