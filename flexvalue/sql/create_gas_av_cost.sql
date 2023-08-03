CREATE TABLE gas_av_costs (
    state TEXT,
    utility TEXT,
    region TEXT,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    datetime TIMESTAMP,
    market FLOAT,
    t_d FLOAT,
    environment FLOAT,
    btm_methane FLOAT,
    total FLOAT,
    upstream_methane FLOAT,
    marginal_ghg FLOAT,
    value_curve_name TEXT
);
