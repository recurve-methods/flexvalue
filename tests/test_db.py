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

TEST_HOST="postgresql"
TEST_PORT=5432
TEST_USER="postgres"
TEST_PASSWORD="example"
TEST_DATABASE="postgres"

@pytest.fixture
def dbm():
    return DBManager("")

@pytest.fixture
def dbm_with_project_loaded(dbm):
    dbm.load_project_info_file("example_user_inputs.csv")
    return dbm

@pytest.fixture
def dbm_with_elec_load_shape_loaded(dbm):
    # TRUNCATE the table first so we can assert the correct number of rows
    dbm.load_elec_load_shapes_file("ca_hourly_electric_load_shapes.csv", truncate=True)
    return dbm

@pytest.fixture
def dbm_with_therms_profiles_loaded(dbm):
    dbm.load_therms_profiles_file("ca_monthly_therms_load_profiles.csv", truncate=True)
    return dbm

@pytest.fixture
def dbm_with_elec_avcosts_loaded(dbm):
    # NOTE This takes several minutes
    dbm.load_elec_avoided_costs_file("full_ca_avoided_costs_2020acc.csv", truncate=True)
    return dbm

@pytest.fixture
def dbm_with_gas_avcosts_loaded(dbm):
    dbm.load_gas_avoided_costs_file("full_ca_avoided_costs_2020acc_gas.csv", truncate=True)
    return dbm

@pytest.fixture
def addl_fields_sep_output():
    return FlexValueRun(
        database_type="postgresql",
        host=TEST_HOST,
        port=TEST_PORT,
        user=TEST_USER,
        password=TEST_PASSWORD,
        database=TEST_DATABASE,
        elec_av_costs_table="elec_av_costs",
        elec_load_shape_table="elec_load_shape",
        therms_profiles_table="therms_profile",
        gas_av_costs_table="gas_av_costs",
        project_info_file="example_user_inputs_cz12_37.csv",
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
        database_type="postgresql",
        host=TEST_HOST,
        port=TEST_PORT,
        user=TEST_USER,
        password=TEST_PASSWORD,
        database=TEST_DATABASE,
        elec_av_costs_table="elec_av_costs",
        elec_load_shape_table="elec_load_shape",
        therms_profiles_table="therms_profile",
        gas_av_costs_table="gas_av_costs",
        project_info_file="example_user_inputs_cz12_37.csv",
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
        database_type="postgresql",
        host=TEST_HOST,
        port=TEST_PORT,
        user=TEST_USER,
        password=TEST_PASSWORD,
        database=TEST_DATABASE,
        elec_av_costs_table="elec_av_costs",
        elec_load_shape_table="elec_load_shape",
        therms_profiles_table="therms_profile",
        gas_av_costs_table="gas_av_costs",
        project_info_file="example_user_inputs_cz12_37.csv",
        output_table="output_table",
        aggregation_columns=["project_id", "hour_of_year", "year"],
        elec_components=["electric_savings", "energy", "losses", "ancillary_services", "capacity", "transmission", "distribution", "cap_and_trade", "ghg_adder_rebalancing", "ghg_adder", "ghg_rebalancing", "methane_leakage", "marginal_ghg"],
        gas_components=["market", "t_d", "environment", "btm_methane", "upstream_methane"],
        separate_output_tables=False
    )

@pytest.fixture
def no_addl_fields_sep_output():
    return FlexValueRun(
        database_type="postgresql",
        host=TEST_HOST,
        port=TEST_PORT,
        user=TEST_USER,
        password=TEST_PASSWORD,
        database=TEST_DATABASE,
        elec_av_costs_table="elec_av_costs",
        elec_load_shape_table="elec_load_shape",
        therms_profiles_table="therms_profile",
        gas_av_costs_table="gas_av_costs",
        project_info_file="example_user_inputs_cz12_37.csv",
        output_table="nafsepo_output_table",
        aggregation_columns=["project_id", "hour_of_year", "year"],
        elec_components=["electric_savings", "energy", "losses", "ancillary_services", "capacity", "transmission", "distribution", "cap_and_trade", "ghg_adder_rebalancing", "ghg_adder", "ghg_rebalancing", "methane_leakage", "marginal_ghg"],
        gas_components=["market", "t_d", "environment", "btm_methane", "upstream_methane"],
        separate_output_tables=True
    )

@pytest.fixture
def agg_project_id_no_fields_same_output():
    return FlexValueRun(
        database_type="postgresql",
        host=TEST_HOST,
        port=TEST_PORT,
        user=TEST_USER,
        password=TEST_PASSWORD,
        database=TEST_DATABASE,
        elec_av_costs_table="elec_av_costs",
        elec_load_shape_table="elec_load_shape",
        therms_profiles_table="therms_profile",
        gas_av_costs_table="gas_av_costs",
        project_info_file="example_user_inputs_cz12_37.csv",
        output_table="apinfso_output_table",
        aggregation_columns=["project_id"],
        elec_components=["electric_savings", "energy", "losses", "ancillary_services", "capacity", "transmission", "distribution", "cap_and_trade", "ghg_adder_rebalancing", "ghg_adder", "ghg_rebalancing", "methane_leakage", "marginal_ghg"],
        gas_components=["market", "t_d", "environment", "btm_methane", "upstream_methane"],
        separate_output_tables=False
    )

# _exec_select_sql returns a list of tuples, so for SELECT COUNT(*) queries
# we will always be looking at `result[0][0]`
def test_addl_fields_sep_output(addl_fields_sep_output):
    addl_fields_sep_output.run()
    result = addl_fields_sep_output.db_manager._exec_select_sql("SELECT COUNT(*) FROM electric_afsepo_output_table")
    assert result[0][0] == 3153600
    result = addl_fields_sep_output.db_manager._exec_select_sql("SELECT COUNT(*) FROM gas_afsepo_output_table")
    # sum(eul) of the projects = 380; since we have to agg by the addl_columns, we include month, so 380 * 12 = 4560
    assert result[0][0] == 4560

def test_addl_fields_same_output(addl_fields_same_output):
    addl_fields_same_output.run()
    result = addl_fields_same_output.db_manager._exec_select_sql("SELECT COUNT(*) FROM output_table")
    # sum(eul) of the projects that we have load shape data for == 360; 360 * 8760 == 3153600
    assert result[0][0] == 3153600

def test_no_addl_fields_sep_output(no_addl_fields_sep_output):
    no_addl_fields_sep_output.run()
    result = no_addl_fields_sep_output.db_manager._exec_select_sql("SELECT COUNT(*) FROM electric_nafsepo_output_table")
    # sum(eul) of the projects that we have load shape data for == 360; 360 * 8760 == 3153600
    assert result[0][0] == 3153600
    result = no_addl_fields_sep_output.db_manager._exec_select_sql("SELECT COUNT(*) FROM gas_nafsepo_output_table")
    # sum(eul) of the projects that we have therm profile data for == 380; we are aggregating at the year level, so this should be 380
    assert result[0][0] == 380

def test_no_addl_fields_same_output(no_addl_fields_same_output):
    no_addl_fields_same_output.run()
    result = no_addl_fields_same_output.db_manager._exec_select_sql("SELECT COUNT(*) FROM output_table")
    # sum(eul) of the projects that we have load shape data for == 360; 360 * 8760 == 3153600
    assert result[0][0] == 3153600

def test_agg_project_id_no_fields_same_output(agg_project_id_no_fields_same_output):
    agg_project_id_no_fields_same_output.run()
    result = agg_project_id_no_fields_same_output.db_manager._exec_select_sql("SELECT COUNT(*) FROM apinfso_output_table")
    assert result[0][0] == 36 # 38 distinct projects minus 2 with no matching load shape

def test_project_load(dbm_with_project_loaded):
    result = dbm_with_project_loaded._exec_select_sql("SELECT COUNT(*) FROM project_info;")
    assert result[0][0] == 4

def test_discount(dbm_with_project_loaded):
    result = dbm_with_project_loaded._exec_select_sql("SELECT COUNT(*) FROM discount;")
    assert result[0][0] == 180

def test_elec_load_shape(dbm_with_elec_load_shape_loaded):
    result = dbm_with_elec_load_shape_loaded._exec_select_sql("SELECT COUNT(*) FROM elec_load_shape;")
    assert result[0][0] == 665760

def test_therms_profiles(dbm_with_therms_profiles_loaded):
    result = dbm_with_therms_profiles_loaded._exec_select_sql("SELECT COUNT(*) FROM therms_profile;")
    assert result[0][0] == 144

# NOTE This test takes several minutes to run due to loading the data.
def test_elec_avoided_costs(dbm_with_elec_avcosts_loaded):
    result = dbm_with_elec_avcosts_loaded._exec_select_sql("SELECT COUNT(*) FROM elec_av_costs;")
    assert result[0][0] == 6167040

def test_gas_avoided_costs(dbm_with_gas_avcosts_loaded):
    result = dbm_with_gas_avcosts_loaded._exec_select_sql("SELECT COUNT(*) FROM gas_av_costs;")
    assert result[0][0] == 1488