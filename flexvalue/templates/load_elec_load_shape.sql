INSERT INTO elec_load_shape (
    timestamp,
    state,
    utility,
    region,
    quarter,
    month,
    hour_of_day,
    hour_of_year,
    load_shape_name,
    value
)
VALUES (:timestamp, :state, :utility, :region, :quarter, :month, :hour_of_day, :hour_of_year, :load_shape_name, :value)
