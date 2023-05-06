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

import pytest
from flexvalue.db import DBManager
from flexvalue.flexvalue import FlexValueRun

@pytest.fixture
def addl_fields_sep_output():
    return FlexValueRun(
        database_type="bigquery",
        project="oeem-avdcosts-platform",
        source_dataset="flexvalue_refactor_tables",
        target_dataset="flexvalue_refactor_tables",
        elec_av_costs_table="full_ca_avoided_costs_2020acc_copy",
        elec_load_shape_table="ca_hourly_electric_load_shapes_horizontal_copy",
        therms_profiles_table="ca_monthly_therms_load_profiles_copy",
        gas_av_costs_table="full_ca_avoided_costs_2020acc_gas_copy",
        project_info_table="example_user_inputs_38",
        output_table="afsepo_output_table",
        aggregation_columns=["project_id", "hour_of_year", "year"],
        elec_components=["electric_savings", "energy", "losses", "ancillary_services", "capacity", "transmission", "distribution", "cap_and_trade", "ghg_adder_rebalancing", "ghg_adder", "ghg_rebalancing", "methane_leakage", "marginal_ghg"],
        gas_components=["market", "t_d", "environment", "btm_methane", "upstream_methane"],
        elec_addl_fields = ["hour_of_year", "utility", "region", "month", "quarter", "hour_of_day", "discount"],
        gas_addl_fields = ["total", "month", "quarter"],
        separate_output_tables=True
    )

@pytest.fixture
def addl_fields_same_output():
    return FlexValueRun(
        database_type="bigquery",
        project="oeem-avdcosts-platform",
        source_dataset="flexvalue_refactor_tables",
        target_dataset="flexvalue_refactor_tables",
        elec_av_costs_table="full_ca_avoided_costs_2020acc_copy",
        elec_load_shape_table="ca_hourly_electric_load_shapes_horizontal_copy",
        therms_profiles_table="ca_monthly_therms_load_profiles_copy",
        gas_av_costs_table="full_ca_avoided_costs_2020acc_gas_copy",
        project_info_table="example_user_inputs_38",
        output_table="output_table",
        aggregation_columns=["project_id", "hour_of_year", "year"],
        elec_components=["electric_savings", "energy", "losses", "ancillary_services", "capacity", "transmission", "distribution", "cap_and_trade", "ghg_adder_rebalancing", "ghg_adder", "ghg_rebalancing", "methane_leakage", "marginal_ghg"],
        gas_components=["market", "t_d", "environment", "btm_methane", "upstream_methane"],
        elec_addl_fields = ["hour_of_year", "utility", "region", "month", "quarter", "hour_of_day", "discount"],
        gas_addl_fields = ["total", "month", "quarter"],
        separate_output_tables=False
    )

@pytest.fixture
def no_addl_fields_same_output():
    return FlexValueRun(
        database_type="bigquery",
        project="oeem-avdcosts-platform",
        source_dataset="flexvalue_refactor_tables",
        target_dataset="flexvalue_refactor_tables",
        elec_av_costs_table="full_ca_avoided_costs_2020acc_copy",
        elec_load_shape_table="ca_hourly_electric_load_shapes_horizontal_copy",
        therms_profiles_table="ca_monthly_therms_load_profiles_copy",
        gas_av_costs_table="full_ca_avoided_costs_2020acc_gas_copy",
        project_info_table="example_user_inputs_38",
        output_table="output_table",
        aggregation_columns=["project_id", "hour_of_year", "year"],
        elec_components=["electric_savings", "energy", "losses", "ancillary_services", "capacity", "transmission", "distribution", "cap_and_trade", "ghg_adder_rebalancing", "ghg_adder", "ghg_rebalancing", "methane_leakage", "marginal_ghg"],
        gas_components=["market", "t_d", "environment", "btm_methane", "upstream_methane"],
        separate_output_tables=False
    )

@pytest.fixture
def no_addl_fields_sep_output():
    return FlexValueRun(
        database_type="bigquery",
        project="oeem-avdcosts-platform",
        source_dataset="flexvalue_refactor_tables",
        target_dataset="flexvalue_refactor_tables",
        elec_av_costs_table="full_ca_avoided_costs_2020acc_copy",
        elec_load_shape_table="ca_hourly_electric_load_shapes_horizontal_copy",
        therms_profiles_table="ca_monthly_therms_load_profiles_copy",
        gas_av_costs_table="full_ca_avoided_costs_2020acc_gas_copy",
        project_info_table="example_user_inputs_38",
        output_table="nafsepo_output_table",
        aggregation_columns=["project_id", "hour_of_year", "year"],
        elec_components=["electric_savings", "energy", "losses", "ancillary_services", "capacity", "transmission", "distribution", "cap_and_trade", "ghg_adder_rebalancing", "ghg_adder", "ghg_rebalancing", "methane_leakage", "marginal_ghg"],
        gas_components=["market", "t_d", "environment", "btm_methane", "upstream_methane"],
        separate_output_tables=True
    )

@pytest.fixture
def agg_project_id_no_fields_same_output():
    return FlexValueRun(
        database_type="bigquery",
        project="oeem-avdcosts-platform",
        source_dataset="flexvalue_refactor_tables",
        target_dataset="flexvalue_refactor_tables",
        elec_av_costs_table="full_ca_avoided_costs_2020acc_copy",
        elec_load_shape_table="ca_hourly_electric_load_shapes_horizontal_copy",
        therms_profiles_table="ca_monthly_therms_load_profiles_copy",
        gas_av_costs_table="full_ca_avoided_costs_2020acc_gas_copy",
        project_info_table="example_user_inputs_38",
        output_table="apinfso_output_table",
        aggregation_columns=["project_id"],
        elec_components=["electric_savings", "energy", "losses", "ancillary_services", "capacity", "transmission", "distribution", "cap_and_trade", "ghg_adder_rebalancing", "ghg_adder", "ghg_rebalancing", "methane_leakage", "marginal_ghg"],
        gas_components=["market", "t_d", "environment", "btm_methane", "upstream_methane"],
        separate_output_tables=False
    )

# TODO: find actual expected counts, do some math to get expected values, etc.
def test_addl_fields_sep_output(addl_fields_sep_output):
    addl_fields_sep_output.run()
    result = addl_fields_sep_output.db_manager._exec_select_sql("SELECT COUNT(*) FROM flexvalue_refactor_tables.electric_afsepo_output_table")
    assert result[0][0] == 3153600
    result = addl_fields_sep_output.db_manager._exec_select_sql("SELECT COUNT(*) FROM flexvalue_refactor_tables.gas_afsepo_output_table")
    # sum(eul) of the projects = 380; since we have to agg by the addl_columns, we include month, so 380 * 12 = 4560
    assert result[0][0] == 4560


def test_addl_fields_same_output(addl_fields_same_output):
    addl_fields_same_output.run()
    result = addl_fields_same_output.db_manager._exec_select_sql("SELECT COUNT(*) FROM flexvalue_refactor_tables.output_table")
    # sum(eul) of the projects that we have load shape data for == 360; 360 * 8760 == 3153600
    assert result[0][0] == 3153600

def test_no_addl_fields_sep_output(no_addl_fields_sep_output):
    no_addl_fields_sep_output.run()
    result = no_addl_fields_sep_output.db_manager._exec_select_sql("SELECT COUNT(*) FROM flexvalue_refactor_tables.electric_nafsepo_output_table")
    # sum(eul) of the projects that we have load shape data for == 360; 360 * 8760 == 3153600
    assert result[0][0] == 3153600
    result = no_addl_fields_sep_output.db_manager._exec_select_sql("SELECT COUNT(*) FROM flexvalue_refactor_tables.gas_nafsepo_output_table")
    # sum(eul) of the projects that we have therm profile data for == 380; we are aggregating at the year level, so this should be 380
    assert result[0][0] == 380

def test_no_addl_fields_same_output(no_addl_fields_same_output):
    no_addl_fields_same_output.run()
    result = no_addl_fields_same_output.db_manager._exec_select_sql("SELECT COUNT(*) FROM flexvalue_refactor_tables.output_table")
    # sum(eul) of the projects that we have load shape data for == 360; 360 * 8760 == 3153600
    assert result[0][0] == 3153600

def test_agg_project_id_no_fields_same_output(agg_project_id_no_fields_same_output):
    agg_project_id_no_fields_same_output.run()
    result = agg_project_id_no_fields_same_output.db_manager._exec_select_sql("SELECT COUNT(*) FROM flexvalue_refactor_tables.apinfso_output_table")
    assert result[0][0] == 36 # 38 distinct projects minus 2 with no matching load shape
