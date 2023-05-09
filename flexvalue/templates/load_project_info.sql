INSERT INTO project_info (
    project_id,
    state,
    utility,
    region,
    mwh_savings,
    therms_savings,
    load_shape,
    therms_profile,
    start_year,
    start_quarter,
    start_date,
    end_date,
    units,
    eul,
    ntg,
    discount_rate,
    admin_cost,
    measure_cost,
    incentive_cost
)
VALUES (
    :project_id, :state, :utility, :region, :mwh_savings, :therms_savings,
    :load_shape, :therms_profile, :start_year, :start_quarter,
    :start_date, :end_date, :units, :eul, :ntg, :discount_rate, :admin_cost,
    :measure_cost, :incentive_cost
)
