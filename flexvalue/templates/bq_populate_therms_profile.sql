FOR therms_profile IN (select column_name from `{{ source_dataset }}`.INFORMATION_SCHEMA.COLUMNS
  WHERE table_name="{{ therms_profiles_table_only_name }}"
  AND column_name not in ("state", "utility", "region", "quarter", "month"))
DO
  EXECUTE IMMEDIATE format(
    """INSERT INTO {{ target_dataset }}.therms_profile (state, utility, region, quarter, month, profile_name, value)
    SELECT UPPER(state), UPPER(utility), UPPER(region), quarter, month, UPPER("%s"), %s FROM {{therms_profiles_table}};""", therms_profile.column_name, therms_profile.column_name);

END FOR;