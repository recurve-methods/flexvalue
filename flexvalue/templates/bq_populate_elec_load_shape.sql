FOR load_shape IN (select column_name from `{{ source_dataset }}`.INFORMATION_SCHEMA.COLUMNS
  WHERE table_name="{{ elec_load_shape_table_name_only }}"
  AND column_name NOT IN ("state", "utility", "region", "quarter", "month", "hour_of_day", "hour_of_year"))
DO
  EXECUTE IMMEDIATE format(
    """INSERT INTO {{ target_dataset }}.elec_load_shape (state, utility, quarter, month, hour_of_year, load_shape_name, value)
    SELECT UPPER(state), UPPER(utility), quarter, month, hour_of_year, UPPER("%s"), %s
    FROM {{elec_load_shape_table}};""", load_shape.column_name, load_shape.column_name);

END FOR;