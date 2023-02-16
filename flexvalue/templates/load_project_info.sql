INSERT INTO project_info (
    project_id,
    mwh_savings,
    therms_savings,
    elec_load_shape,
    therms_profile,
    start_year,
    start_quarter,
    utility,
    region,
    units,
    eul,
    ntg,
    discount_rate,
    admin_cost,
    measure_cost,
    incentive_cost
)
VALUES (:project_id, :mwh_savings, :therms_savings, :elec_load_shape, :therms_profile, :start_year, :start_quarter, :utility, :region, :units, :eul, :ntg, :discount_rate, :admin_cost, :measure_cost, :incentive_cost)
