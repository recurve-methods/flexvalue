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
from flexvalue.config import FLEXValueConfig
from flexvalue.flexvalue import FlexValueRun

TEST_HOST="postgresql"
TEST_PORT=5432
TEST_USER="postgres"
TEST_PASSWORD="example"
TEST_DATABASE="postgres"

@pytest.fixture
def config():
    config = FLEXValueConfig(
        database_type="postgresql",
        host=TEST_HOST,
        port=TEST_PORT,
        user=TEST_USER,
        password=TEST_PASSWORD,
        database=TEST_DATABASE
    )
    return config

@pytest.fixture
def config_with_project(config: FLEXValueConfig):
    config.project_info_file = "example_user_inputs.csv"
    return config

@pytest.fixture
def config_with_elec_load_shape(config: FLEXValueConfig):
    config.elec_load_shape_file = "ca_hourly_electric_load_shapes.csv"
    config.reset_elec_load_shapes = True
    return config

@pytest.fixture
def config_with_therms_profiles(config: FLEXValueConfig):
    config.therms_profiles_file = "ca_monthly_therms_load_profiles.csv"
    config.reset_therms_profiles
    return config

@pytest.fixture
def config_with_elec_avcosts(config: FLEXValueConfig):
    config.elec_av_costs_file = "full_ca_avoided_costs_2020acc.csv"
    config.reset_elec_av_costs = True
    return config

@pytest.fixture
def config_with_gas_avcosts(config: FLEXValueConfig):
    config.gas_av_costs_file = "full_ca_avoided_costs_2020acc_gas.csv"
    config.reset_gas_av_costs = True
    return config

@pytest.fixture
def config_with_metered_load_shape(config: FLEXValueConfig):
    config.metered_load_shape_file = "example_metered_load_shape.csv"
    config.reset_elec_load_shapes = True
    return config

@pytest.fixture
def config_with_both_load_shapes(config: FLEXValueConfig):
    config.elec_load_shape_file = "ca_hourly_electric_load_shapes.csv"
    config.metered_load_shape_file = "example_metered_load_shape.csv"
    config.reset_elec_load_shapes = True
    return config

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
        electric_output_table="afsepo_output_table_electric",
        gas_output_table="afsepo_output_table_gas",
        aggregation_columns=["project_id", "hour_of_year", "year"],
        elec_components=["energy", "losses", "ancillary_services", "capacity", "transmission", "distribution", "cap_and_trade", "ghg_adder_rebalancing", "ghg_adder", "ghg_rebalancing", "methane_leakage", "marginal_ghg"],
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
        elec_components=["energy", "losses", "ancillary_services", "capacity", "transmission", "distribution", "cap_and_trade", "ghg_adder_rebalancing", "ghg_adder", "ghg_rebalancing", "methane_leakage", "marginal_ghg"],
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
        elec_components=["energy", "losses", "ancillary_services", "capacity", "transmission", "distribution", "cap_and_trade", "ghg_adder_rebalancing", "ghg_adder", "ghg_rebalancing", "methane_leakage", "marginal_ghg"],
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
        electric_output_table="nafsepo_output_table_electric",
        gas_output_table="nafsepo_output_table_gas",
        aggregation_columns=["project_id", "hour_of_year", "year"],
        elec_components=["energy", "losses", "ancillary_services", "capacity", "transmission", "distribution", "cap_and_trade", "ghg_adder_rebalancing", "ghg_adder", "ghg_rebalancing", "methane_leakage", "marginal_ghg"],
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
        elec_components=["energy", "losses", "ancillary_services", "capacity", "transmission", "distribution", "cap_and_trade", "ghg_adder_rebalancing", "ghg_adder", "ghg_rebalancing", "methane_leakage", "marginal_ghg"],
        gas_components=["market", "t_d", "environment", "btm_methane", "upstream_methane"],
        separate_output_tables=False
    )

# _exec_select_sql returns a list of tuples, so for SELECT COUNT(*) queries
# we will always be looking at `result[0][0]`
def test_addl_fields_sep_output(addl_fields_sep_output):
    addl_fields_sep_output.run()
    result = addl_fields_sep_output.db_manager._exec_select_sql("SELECT COUNT(*) FROM afsepo_output_table_electric")
    assert result[0][0] == 3153600
    result = addl_fields_sep_output.db_manager._exec_select_sql("SELECT COUNT(*) FROM afsepo_output_table_gas")
    # sum(eul) of the projects = 380; since we have to agg by the addl_columns, we include month, so 380 * 12 = 4560
    assert result[0][0] == 4560

def test_addl_fields_same_output(addl_fields_same_output):
    addl_fields_same_output.run()
    result = addl_fields_same_output.db_manager._exec_select_sql("SELECT COUNT(*) FROM output_table")
    # sum(eul) of the projects that we have load shape data for == 360; 360 * 8760 == 3153600
    assert result[0][0] == 3153600

def test_no_addl_fields_sep_output(no_addl_fields_sep_output):
    no_addl_fields_sep_output.run()
    result = no_addl_fields_sep_output.db_manager._exec_select_sql("SELECT COUNT(*) FROM nafsepo_output_table_electric")
    # sum(eul) of the projects that we have load shape data for == 360; 360 * 8760 == 3153600
    assert result[0][0] == 3153600
    result = no_addl_fields_sep_output.db_manager._exec_select_sql("SELECT COUNT(*) FROM nafsepo_output_table_gas")
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

def test_project_load(config_with_project: FLEXValueConfig):
    dbm = DBManager.get_db_manager(config_with_project)
    dbm.process_project_info(config_with_project.project_info_file)
    result = dbm._exec_select_sql("SELECT COUNT(*) FROM project_info;")
    assert result[0][0] == 4

def test_elec_load_shape(config_with_elec_load_shape: FLEXValueConfig):
    dbm = DBManager.get_db_manager(config_with_elec_load_shape)
    dbm.reset_elec_load_shape()
    dbm.process_elec_load_shape(config_with_elec_load_shape.elec_load_shape_file)
    result = dbm._exec_select_sql("SELECT COUNT(*) FROM elec_load_shape;")
    assert result[0][0] == 665760

def test_metered_load_shape(config_with_metered_load_shape: FLEXValueConfig):
    dbm = DBManager.get_db_manager(config_with_metered_load_shape)
    dbm.reset_elec_load_shape()
    dbm.process_metered_load_shape(config_with_metered_load_shape.metered_load_shape_file)
    result = dbm._exec_select_sql("SELECT COUNT(*) FROM elec_load_shape;")
    assert result[0][0] == 8760

def test_both_load_shapes(config_with_both_load_shapes: FLEXValueConfig):
    dbm = DBManager.get_db_manager(config_with_both_load_shapes)
    dbm.reset_elec_load_shape()
    dbm.process_elec_load_shape(config_with_both_load_shapes.elec_load_shape_file)
    dbm.process_metered_load_shape(config_with_both_load_shapes.metered_load_shape_file)
    result = dbm._exec_select_sql("SELECT COUNT(*) FROM elec_load_shape;")
    assert result[0][0] == 665760 + 8760

def test_therms_profiles(config_with_therms_profiles: FLEXValueConfig):
    dbm = DBManager.get_db_manager(config_with_therms_profiles)
    dbm.reset_therms_profiles()
    dbm.process_therms_profile(config_with_therms_profiles.therms_profiles_file)
    result = dbm._exec_select_sql("SELECT COUNT(*) FROM therms_profile;")
    assert result[0][0] == 144

# NOTE This test takes several minutes to run due to loading the data.
def test_elec_avoided_costs(config_with_elec_avcosts: FLEXValueConfig):
    dbm = DBManager.get_db_manager(config_with_elec_avcosts)
    dbm.reset_elec_av_costs()
    dbm.process_elec_av_costs(config_with_elec_avcosts.elec_av_costs_file)
    result = dbm._exec_select_sql("SELECT COUNT(*) FROM elec_av_costs;")
    assert result[0][0] == 6167040

def test_gas_avoided_costs(config_with_gas_avcosts: FLEXValueConfig):
    dbm = DBManager.get_db_manager(config_with_gas_avcosts)
    dbm.reset_gas_av_costs()
    dbm.process_gas_av_costs(config_with_gas_avcosts.gas_av_costs_file)
    result = dbm._exec_select_sql("SELECT COUNT(*) FROM gas_av_costs;")
    assert result[0][0] == 1488