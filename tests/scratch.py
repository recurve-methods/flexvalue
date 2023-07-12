import math
import pytest
from flexvalue.db import DBManager
from flexvalue.flexvalue import FlexValueRun


def troubleshoot():
    real_data_calculations_time_series = FlexValueRun(
        database_type="bigquery",
        project="oeem-avdcosts-platform",
        elec_av_costs_table="oeem-avdcosts-platform.avoided_costs_platform_use.full_ca_avoided_costs_2020acc",
        elec_load_shape_table="flexvalue_refactor_tables.ca_hourly_electric_load_shapes_horizontal_copy",
        therms_profiles_table="flexvalue_refactor_tables.ca_monthly_therms_load_profiles_copy",
        gas_av_costs_table="oeem-avdcosts-platform.avoided_costs_platform_use.full_ca_avoided_costs_2020acc_gas",
        project_info_table="flexvalue_refactor_tables.formatted_for_metered_deer_run_p2021",
        output_table="flexvalue_refactor_tables.rdcts_output_table",
        aggregation_columns=["id", "hour_of_year", "year"],
        elec_components=[
            "energy",
            "losses",
            "ancillary_services",
            "capacity",
            "transmission",
            "distribution",
            "cap_and_trade",
            "ghg_adder_rebalancing",
            "ghg_adder",
            "ghg_rebalancing",
            "methane_leakage",
            "marginal_ghg",
        ],
        gas_components=[
            "market",
            "t_d",
            "environment",
            "btm_methane",
            "upstream_methane",
        ],
        separate_output_tables=False,
        process_elec_load_shape=True,
        process_therms_profiles=True,
    )
    real_data_calculations_time_series.run()

    # Now cherry-pick some hourly calculations and compare them also
    # Maps the column names from FV1:FV2
    column_mapping = {
        "flexvalue_id": "id",
        "year": "elec_year",
        "hour_of_year": "elec_hour_of_year",
        "energy": "energy",
        "losses": "losses",
        "ancillary_services": "ancillary_services",
        "capacity": "capacity",
        "transmission": "transmission",
        "distribution": "distribution",
        "cap_and_trade": "cap_and_trade",
        "ghg_adder_rebalancing": "ghg_adder_rebalancing",
    }
    query_str = "SELECT {columns} FROM {table_name} WHERE id IN ({projects}) ORDER BY id, elec_year, elec_hour_of_year"
    results_dict = {
        "MAR101323": [
            0.41806757266493833,
            0.013389025632615839,
            0.0015681452798683364 / 1000.0,
        ],
        "MAR100695": [
            -0.0994271147418045,
            -0.000387365436068152,
            -0.000387365436068152 / 1000.0,
        ],
        "MAR101024": [
            0.97777267304016569,
            -0.0031842512428534538,
            0.0037377979065195471 / 1000.0,
        ],
    }
    projects = ",".join([f"'{x}'" for x in results_dict.keys()])
    columns = ", ".join([x for x in column_mapping.values()])
    query = query_str.format(
        columns=columns,
        table_name="flexvalue_refactor_tables.rdcts_output_table",
        projects=projects,
    )
    result = real_data_calculations_time_series.db_manager._exec_select_sql(sql=query)
    test_results = []
    for row in result:
        row_dict = dict(row)
        test_results.append(row_dict)

    # test_df = real_data_calculations_time_series.db_manager._select_as_df(sql=query)
    like_clause = " OR ".join(
        [f"flexvalue_id LIKE '{x}%'" for x in results_dict.keys()]
    )
    query_str = "SELECT {columns} from `{table_name}` where ({like_clause}) ORDER BY flexvalue_id, year, hour_of_year"
    columns = ", ".join([f"{x}" for x in column_mapping.keys()])
    query = query_str.format(
        columns=columns,
        table_name="oeem-mcemktpl-platform.cmkt_2023_04_01_flexvalue_metered_outputs.flexvalue_outputs_deer_p2021_ts_elec_latest",
        like_clause=like_clause,
    )
    # prod_df = real_data_calculations_time_series.db_manager._select_as_df(sql=query)
    result = real_data_calculations_time_series.db_manager._exec_select_sql(sql=query)
    prod_results = []
    for row in result:
        row_dict = dict(row)
        prod_results.append(row_dict)

    assert len(test_results) == len(prod_results)
    for k, v in column_mapping.items():
        if k != "flexvalue_id":
            print(f"Testing {k}:{v}")
            for x in range(0, len(test_results), 1000):
                print(test_results[x][v], prod_results[x][k])
                assert math.isclose(
                    test_results[x][v], prod_results[x][k], rel_tol=0.005
                )


if __name__ == "__main__":
    troubleshoot()
