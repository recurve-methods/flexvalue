FOR load_shape IN (select column_name from `{{ project }}.{{ dataset }}`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name="{{ metered_load_shape_table }}"
AND column_name NOT IN ("state", "utility", "region", "quarter", "month", "hour_of_day", "hour_of_year"))
DO
  EXECUTE IMMEDIATE format(
  """INSERT INTO {{ dataset }}.elec_load_shape (utility, hour_of_year, load_shape_name, value)
  SELECT UPPER("{{ utility }}"), hour_of_year, UPPER("%s"), %s
  FROM {{project}}.{{dataset}}.{{metered_load_shape_table}};""", load_shape.column_name, load_shape.column_name);
END FOR;
