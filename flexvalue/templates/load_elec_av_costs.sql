-- sqlite only - will need to be updated
INSERT INTO elec_av_costs (
	state,
    utility,
    region,
    date_time,
    year,
    quarter,
    month,
    date_str,
    hour_of_day,
    hour_of_year,
    energy,
    losses,
    ancillary_services,
    capacity,
    transmission,
    distribution,
    cap_and_trade,
    ghg_adder,
    ghg_rebalancing,
    methane_leakage,
    total,
    marginal_ghg,
    ghg_adder_rebalancing,
    value_curve_name
) VALUES ( :state, :utility, :region, :datetime, :year, :quarter, :month, :date_str, :hour_of_day, :hour_of_year, :energy, :losses, :ancillary_services, :capacity, :transmission, :distribution, :cap_and_trade, :ghg_adder, :ghg_rebalancing, :methane_leakage, :total, :marginal_ghg, :ghg_adder_rebalancing, :value_curve_name
)