CREATE TABLE {{ dataset }}.elec_load_shape (
    state STRING,
    utility STRING,
    util_load_shape STRING,
    region STRING,
    quarter INTEGER,
    month INTEGER,
    hour_of_day INTEGER,
    hour_of_year INTEGER,
    load_shape_name STRING,
    value FLOAT64
);
