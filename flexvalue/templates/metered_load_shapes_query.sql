select column_name from `{{ project }}.{{ dataset }}`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name="{{ metered_load_shape_table }}"
AND column_name NOT IN ("state", "utility", "region", "quarter", "month", "hour_of_day", "hour_of_year")