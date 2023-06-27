FOR row in (
  select distinct load_shape, utility
  FROM `{{ project_info_table }}`
  where upper(load_shape) in (
    select upper(column_name) from `{{ source_dataset }}`.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name="{{ metered_load_shape_table_only_name }}"
    AND column_name NOT IN ("state", "utility", "region", "quarter", "month", "hour_of_day", "hour_of_year")
  )
)

DO
  EXECUTE IMMEDIATE format(
  """INSERT INTO {{ target_dataset }}.elec_load_shape (utility, hour_of_year, load_shape_name, value)
  SELECT UPPER("%s"), hour_of_year, UPPER("%s"), %s
  FROM {{ metered_load_shape_table }};""", row.utility, row.load_shape, row.load_shape);
END FOR;
