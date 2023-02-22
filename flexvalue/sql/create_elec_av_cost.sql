CREATE TABLE elec_av_costs (
    state TEXT,
    utility TEXT,
    region TEXT,
    date_time TEXT, -- TODO figure this out for the various dbs
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    hour_of_day INTEGER,
    hour_of_year INTEGER,
    energy FLOAT,
    losses FLOAT,
    ancillary_services FLOAT,
    capacity FLOAT,
    transmission FLOAT,
    distribution FLOAT,
    cap_and_trade FLOAT,
    ghg_adder FLOAT,
    ghg_rebalancing FLOAT,
    methane_leakage FLOAT,
    total FLOAT,
    marginal_ghg FLOAT,
    ghg_adder_rebalancing FLOAT
);
