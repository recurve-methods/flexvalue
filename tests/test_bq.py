#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""

   Copyright 2021 Recurve Analytics, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

"""

import math
import pytest
from flexvalue.db import DBManager
from flexvalue.flexvalue import FlexValueRun


@pytest.fixture
def addl_fields_sep_output():
    return FlexValueRun(
        database_type="bigquery",
        project="oeem-avdcosts-platform",
        elec_av_costs_table="oeem-avdcosts-platform.avoided_costs_platform_use.full_ca_avoided_costs_2020acc",
        elec_load_shape_table="flexvalue_refactor_tables.ca_hourly_electric_load_shapes_horizontal_copy",
        therms_profiles_table="flexvalue_refactor_tables.ca_monthly_therms_load_profiles_copy",
        gas_av_costs_table="oeem-avdcosts-platform.avoided_costs_platform_use.full_ca_avoided_costs_2020acc_gas",
        project_info_table="flexvalue_refactor_tables.example_user_inputs_38",
        electric_output_table="flexvalue_refactor_tables.afsepo_output_table_electric",
        gas_output_table="flexvalue_refactor_tables.afsepo_output_table_gas",
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
        elec_addl_fields=[
            "hour_of_year",
            "utility",
            "region",
            "month",
            "quarter",
            "hour_of_day",
            "discount",
        ],
        gas_addl_fields=["total", "month", "quarter"],
        separate_output_tables=True,
    )


@pytest.fixture
def addl_fields_same_output():
    return FlexValueRun(
        database_type="bigquery",
        project="oeem-avdcosts-platform",
        elec_av_costs_table="oeem-avdcosts-platform.avoided_costs_platform_use.full_ca_avoided_costs_2020acc",
        elec_load_shape_table="flexvalue_refactor_tables.ca_hourly_electric_load_shapes_horizontal_copy",
        therms_profiles_table="flexvalue_refactor_tables.ca_monthly_therms_load_profiles_copy",
        gas_av_costs_table="oeem-avdcosts-platform.avoided_costs_platform_use.full_ca_avoided_costs_2020acc_gas",
        project_info_table="flexvalue_refactor_tables.example_user_inputs_38",
        output_table="flexvalue_refactor_tables.output_table",
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
        elec_addl_fields=[
            "hour_of_year",
            "utility",
            "region",
            "month",
            "quarter",
            "hour_of_day",
            "discount",
        ],
        gas_addl_fields=["total", "month", "quarter"],
        separate_output_tables=False,
    )


@pytest.fixture
def no_addl_fields_same_output():
    return FlexValueRun(
        database_type="bigquery",
        project="oeem-avdcosts-platform",
        elec_av_costs_table="oeem-avdcosts-platform.avoided_costs_platform_use.full_ca_avoided_costs_2020acc",
        elec_load_shape_table="flexvalue_refactor_tables.ca_hourly_electric_load_shapes_horizontal_copy",
        therms_profiles_table="flexvalue_refactor_tables.ca_monthly_therms_load_profiles_copy",
        gas_av_costs_table="oeem-avdcosts-platform.avoided_costs_platform_use.full_ca_avoided_costs_2020acc_gas",
        project_info_table="flexvalue_refactor_tables.example_user_inputs_38",
        output_table="flexvalue_refactor_tables.output_table",
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
    )


@pytest.fixture
def no_addl_fields_sep_output():
    return FlexValueRun(
        database_type="bigquery",
        project="oeem-avdcosts-platform",
        elec_av_costs_table="oeem-avdcosts-platform.avoided_costs_platform_use.full_ca_avoided_costs_2020acc",
        elec_load_shape_table="flexvalue_refactor_tables.ca_hourly_electric_load_shapes_horizontal_copy",
        therms_profiles_table="flexvalue_refactor_tables.ca_monthly_therms_load_profiles_copy",
        gas_av_costs_table="oeem-avdcosts-platform.avoided_costs_platform_use.full_ca_avoided_costs_2020acc_gas",
        project_info_table="flexvalue_refactor_tables.example_user_inputs_38",
        electric_output_table="flexvalue_refactor_tables.nafsepo_output_table_electric",
        gas_output_table="flexvalue_refactor_tables.nafsepo_output_table_gas",
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
        separate_output_tables=True,
    )


@pytest.fixture
def agg_id_no_fields_same_output():
    return FlexValueRun(
        database_type="bigquery",
        project="oeem-avdcosts-platform",
        elec_av_costs_table="oeem-avdcosts-platform.avoided_costs_platform_use.full_ca_avoided_costs_2020acc",
        elec_load_shape_table="flexvalue_refactor_tables.ca_hourly_electric_load_shapes_horizontal_copy",
        therms_profiles_table="flexvalue_refactor_tables.ca_monthly_therms_load_profiles_copy",
        gas_av_costs_table="oeem-avdcosts-platform.avoided_costs_platform_use.full_ca_avoided_costs_2020acc_gas",
        project_info_table="flexvalue_refactor_tables.example_user_inputs_38",
        output_table="flexvalue_refactor_tables.apinfso_output_table",
        aggregation_columns=["id"],
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
    )


@pytest.fixture
def metered_load_shape():
    return FlexValueRun(
        database_type="bigquery",
        project="oeem-avdcosts-platform",
        therms_profiles_table="flexvalue_refactor_tables.ca_monthly_therms_load_profiles_copy",
        gas_av_costs_table="oeem-avdcosts-platform.avoided_costs_platform_use.full_ca_avoided_costs_2020acc_gas",
        elec_av_costs_table="oeem-avdcosts-platform.avoided_costs_platform_use.full_ca_avoided_costs_2020acc",
        elec_load_shape_table="flexvalue_refactor_tables.ca_hourly_electric_load_shapes_horizontal_copy",
        metered_load_shape_table="flexvalue_refactor_tables.example_metered_load_shape",
        reset_elec_load_shape=True,
        process_elec_load_shape=True,
        process_metered_load_shape=True,
        project_info_table="flexvalue_refactor_tables.example_user_inputs_38",
        output_table="flexvalue_refactor_tables.apinfso_output_table",
        aggregation_columns=["id"],
        separate_output_tables=False,
    )


@pytest.fixture
def real_data_calculations_aggregated():
    return FlexValueRun(
        database_type="bigquery",
        project="oeem-avdcosts-platform",
        elec_av_costs_table="oeem-avdcosts-platform.avoided_costs_platform_use.full_ca_avoided_costs_2020acc",
        elec_load_shape_table="flexvalue_refactor_tables.ca_hourly_electric_load_shapes_horizontal_copy",
        therms_profiles_table="flexvalue_refactor_tables.ca_monthly_therms_load_profiles_copy",
        gas_av_costs_table="oeem-avdcosts-platform.avoided_costs_platform_use.full_ca_avoided_costs_2020acc_gas",
        project_info_table="flexvalue_refactor_tables.formatted_for_metered_deer_run_p2021",
        output_table="flexvalue_refactor_tables.rdc_output_table",
        aggregation_columns=[
            "id",
        ],
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
        elec_addl_fields=["admin_cost", "measure_cost", "incentive_cost"],
        # gas_addl_fields = ["total", "month", "quarter"],
        separate_output_tables=False,
    )


@pytest.fixture
def real_data_calculations_time_series():
    return FlexValueRun(
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
    )


@pytest.fixture
def real_data_calculations_time_series_sep():
    return FlexValueRun(
        database_type="bigquery",
        project="oeem-avdcosts-platform",
        elec_av_costs_table="oeem-avdcosts-platform.avoided_costs_platform_use.full_ca_avoided_costs_2020acc",
        elec_load_shape_table="flexvalue_refactor_tables.ca_hourly_electric_load_shapes_horizontal_copy",
        therms_profiles_table="flexvalue_refactor_tables.ca_monthly_therms_load_profiles_copy",
        gas_av_costs_table="oeem-avdcosts-platform.avoided_costs_platform_use.full_ca_avoided_costs_2020acc_gas",
        project_info_table="flexvalue_refactor_tables.formatted_for_metered_deer_run_p2021",
        electric_output_table="flexvalue_refactor_tables.rdcts_output_table_electric",
        gas_output_table="flexvalue_refactor_tables.rdcts_output_table_gas",
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
        separate_output_tables=True,
    )


def test_metered_load_shape(metered_load_shape):
    metered_load_shape.run()
    result = metered_load_shape.db_manager._exec_select_sql(
        "SELECT COUNT(*) FROM flexvalue_refactor_tables.elec_load_shape"
    )
    assert result[0][0] == 665760 + 8760


def test_addl_fields_sep_output(addl_fields_sep_output):
    addl_fields_sep_output.run()
    result = addl_fields_sep_output.db_manager._exec_select_sql(
        "SELECT COUNT(*) FROM flexvalue_refactor_tables.electric_afsepo_output_table"
    )
    assert result[0][0] == 3153600
    result = addl_fields_sep_output.db_manager._exec_select_sql(
        "SELECT COUNT(*) FROM flexvalue_refactor_tables.gas_afsepo_output_table"
    )
    # sum(eul) of the projects = 380; since we have to agg by the addl_columns, we include month, so 380 * 12 = 4560
    assert result[0][0] == 4560


def test_addl_fields_same_output(addl_fields_same_output):
    addl_fields_same_output.run()
    result = addl_fields_same_output.db_manager._exec_select_sql(
        "SELECT COUNT(*) FROM flexvalue_refactor_tables.output_table"
    )
    # sum(eul) of the projects that we have load shape data for == 375; 375 * 8760 == 3285000
    assert result[0][0] == 3285000


def test_no_addl_fields_sep_output(no_addl_fields_sep_output):
    no_addl_fields_sep_output.run()
    result = no_addl_fields_sep_output.db_manager._exec_select_sql(
        "SELECT COUNT(*) FROM flexvalue_refactor_tables.electric_nafsepo_output_table"
    )
    # sum(eul) of the projects that we have load shape data for == 360; 360 * 8760 == 3153600
    assert result[0][0] == 3153600
    result = no_addl_fields_sep_output.db_manager._exec_select_sql(
        "SELECT COUNT(*) FROM flexvalue_refactor_tables.gas_nafsepo_output_table"
    )
    # sum(eul) of the projects that we have therm profile data for == 380; we are aggregating at the year level, so this should be 380
    assert result[0][0] == 380


def test_no_addl_fields_same_output(no_addl_fields_same_output):
    no_addl_fields_same_output.run()
    result = no_addl_fields_same_output.db_manager._exec_select_sql(
        "SELECT COUNT(*) FROM flexvalue_refactor_tables.output_table"
    )
    # sum(eul) of the projects that we have load shape data for == 375; 375 * 8760 == 3285000
    assert result[0][0] == 3285000


def test_agg_id_no_fields_same_output(agg_id_no_fields_same_output):
    agg_id_no_fields_same_output.run()
    result = agg_id_no_fields_same_output.db_manager._exec_select_sql(
        "SELECT COUNT(*) FROM flexvalue_refactor_tables.apinfso_output_table"
    )
    assert (
        result[0][0] == 37
    )  # 39 distinct projects minus 2 with no matching load shape


def test_real_data_calculations_aggregated(real_data_calculations_aggregated):
    real_data_calculations_aggregated.run()
    result = real_data_calculations_aggregated.db_manager._exec_select_sql(
        "SELECT COUNT(*) FROM flexvalue_refactor_tables.rdc_output_table"
    )
    assert result[0][0] == 30
    query_str = "SELECT id, SUM(admin_cost), SUM(measure_cost), SUM(incentive_cost), SUM(electric_benefits) FROM flexvalue_refactor_tables.rdc_output_table WHERE id IN ({0}) GROUP BY id"
    results_dict = {
        "MAR101323": [8583.76, 13193.17, 0.0, 55231.79],
        "MAR100695": [2368.87, 11619.835, 0.0, -13833.56],
        "MAR101024": [57233.72, 49459.71, 0.0, 127686.39],
    }
    query = query_str.format(",".join([f"'{x}'" for x in results_dict.keys()]))
    result = real_data_calculations_aggregated.db_manager._exec_select_sql(sql=query)

    for row in result:
        correct_vals = results_dict[row[0]]
        for i, val in enumerate(row[1:]):
            assert math.isclose(val, correct_vals[i], rel_tol=0.005)


def test_real_data_calculations_time_series_sep(real_data_calculations_time_series_sep):
    real_data_calculations_time_series_sep.run()
    elec_result = real_data_calculations_time_series_sep.db_manager._exec_select_sql(
        "SELECT COUNT(*) FROM flexvalue_refactor_tables.rdcts_output_table_electric"
    )
    gas_result = real_data_calculations_time_series_sep.db_manager._exec_select_sql(
        "SELECT COUNT(*) FROM flexvalue_refactor_tables.rdcts_output_table_gas"
    )
    print(f"elec_result = {elec_result[0][0]}; gas_result = {gas_result[0][0]}")


def test_real_data_calculations_time_series(real_data_calculations_time_series):
    real_data_calculations_time_series.run()
    result = real_data_calculations_time_series.db_manager._exec_select_sql(
        "SELECT COUNT(*) FROM flexvalue_refactor_tables.rdcts_output_table"
    )
    assert result[0][0] == 30 * 12 * 8760  # num projects * EUL * hours per year
    query_str = "SELECT id, SUM(electric_benefits), MAX(trc_costs), MAX(pac_costs), SUM(elec_avoided_ghg), SUM(lifecycle_gas_ghg_savings) FROM flexvalue_refactor_tables.rdcts_output_table WHERE id IN ({0}) GROUP BY id"
    results_dict = {
        "MAR101323": [55231.79, 20881.76, 8583.76, 191.286 / 1000.0, 0],
        "MAR100695": [-13833.56, 13200.29, 2368.87, -46.548 / 1000.0, 0],
        "MAR101024": [127686.39, 103337.56, 57233.72, 451.753 / 1000.0, 0],
    }
    query = query_str.format(",".join([f"'{x}'" for x in results_dict.keys()]))
    result = real_data_calculations_time_series.db_manager._exec_select_sql(sql=query)
    for row in result:
        correct_vals = results_dict[row[0]]
        for i, val in enumerate(row[1:]):
            assert math.isclose(val, correct_vals[i], rel_tol=0.005)

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
    test_df = real_data_calculations_time_series.db_manager._select_as_df(sql=query)
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
    prod_df = real_data_calculations_time_series.db_manager._select_as_df(sql=query)
    for k, v in column_mapping.items():
        if k != "flexvalue_id":
            print(f"Testing {k}:{v}: {len(test_df[v])}")
            assert len(test_df[v]) == len(prod_df[k])
            for x in range(0, len(test_df[v]), 1000):
                assert math.isclose(test_df[v][x], prod_df[k][x], rel_tol=0.005)
