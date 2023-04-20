CREATE TABLE {{ dataset }}.therms_profile (
    state STRING,
    utility STRING,
    region STRING,
    quarter INTEGER,
    month INTEGER,
    profile_name STRING,
    value FLOAT64
);