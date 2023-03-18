FOR therms_profile IN (select column_name from `{{ project }}.{{ dataset }}`.INFORMATION_SCHEMA.COLUMNS
  WHERE table_name="{{ therms_profiles_table }}"
  AND column_name not in ("state", "utility", "region", "quarter", "month"))
DO
  EXECUTE IMMEDIATE format(
    """INSERT INTO {{ dataset }}.therms_profile (state, utility, region, quarter, month, profile_name, value)
    SELECT UPPER(state), UPPER(utility), UPPER(region), quarter, month, UPPER("%s"), %s FROM {{project}}.{{dataset}}.{{therms_profiles_table}};""", therms_profile.column_name, therms_profile.column_name);

END FOR;