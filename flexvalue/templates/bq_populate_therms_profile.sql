INSERT `oeem-avdcosts-platform.flexvalue_refactor_tables.therms_profile` (
  state, utility, region, quarter, month, profile_name, value
)
SELECT
  state, utility, region, quarter, month, "winter", winter
 FROM `oeem-avdcosts-platform.flexvalue_refactor_tables.ca_monthly_therms_load_profiles`
UNION ALL
SELECT
  state, utility, region, quarter, month, "summer", summer
 FROM `oeem-avdcosts-platform.flexvalue_refactor_tables.ca_monthly_therms_load_profiles`
UNION ALL
SELECT
  state, utility, region, quarter, month, "annual", annual
 FROM `oeem-avdcosts-platform.flexvalue_refactor_tables.ca_monthly_therms_load_profiles`
