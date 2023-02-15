INSERT INTO gas_av_costs (
state,
utility,
region,
year,
quarter,
month,
market,
t_d,
environment,
btm_methane,
total,
upstream_methane,
marginal_ghg
)
VALUES (:state, :utility, :region, :year, :quarter, :month, :market, :t_d, :environment, :btm_methane, :total, :upstream_methane, :marginal_ghg)