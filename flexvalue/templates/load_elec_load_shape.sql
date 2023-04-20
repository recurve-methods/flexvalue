INSERT INTO elec_load_shape (
    datetime,
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
VALUES (:datetime, :state, :utility, :region, :quarter, :month, :hour_of_day, :hour_of_year, :load_shape_name, :value)
