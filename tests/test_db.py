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
from flexvalue.config import FLEXValueConfig
from flexvalue.flexvalue import FlexValueRun
from typing import Callable
from sqlalchemy import text

TEST_HOST = "postgresql"
TEST_PORT = 5432
TEST_USER = "postgres"
TEST_PASSWORD = "example"
TEST_DATABASE = "postgres"

# TODO: Add some ghg tests in the future.


@pytest.fixture
def config():
    config = FLEXValueConfig(
        database_type="postgresql",
        host=TEST_HOST,
        port=TEST_PORT,
        user=TEST_USER,
        password=TEST_PASSWORD,
        database=TEST_DATABASE,
        elec_av_costs_file="tests/test_data/full_ca_avoided_costs_2020acc.csv",
        gas_av_costs_file="tests/test_data/full_ca_avoided_costs_2020acc_gas.csv"
    )
        
    return config


@pytest.fixture
def config_with_project(config: FLEXValueConfig):
    config.project_info_file = "tests/test_data/example_user_inputs.csv"
    return config


@pytest.fixture
def config_with_no_header_project(config: FLEXValueConfig):
    config.project_info_file = "tests/test_data/example_user_inputs_no_header.csv"
    return config


@pytest.fixture
def config_with_more_projects(config: FLEXValueConfig):
    config.project_info_file = "tests/test_data/example_user_inputs_380.csv"
    return config


@pytest.fixture
def config_with_elec_load_shape(config: FLEXValueConfig):
    config.elec_load_shape_file = "tests/test_data/ca_hourly_electric_load_shapes.csv"
    config.reset_elec_load_shapes = True
    return config


@pytest.fixture
def config_with_therms_profiles(config: FLEXValueConfig):
    config.therms_profiles_file = "tests/test_data/ca_monthly_therms_load_profiles.csv"
    config.reset_therms_profiles
    return config


@pytest.fixture
def config_with_elec_avcosts(config: FLEXValueConfig):
    config.reset_elec_av_costs = True
    return config


@pytest.fixture
def config_with_gas_avcosts(config: FLEXValueConfig):
    config.reset_gas_av_costs = True
    return config


@pytest.fixture
def config_with_metered_load_shape(config: FLEXValueConfig):
    config.metered_load_shape_file = "tests/test_data/example_metered_load_shape.csv"
    config.project_info_file = "tests/test_data/example_user_inputs.csv"
    config.reset_elec_load_shapes = True
    return config


@pytest.fixture
def config_with_two_metered_load_shapes(config: FLEXValueConfig):
    config.metered_load_shape_file = "tests/test_data/example_two_metered_load_shapes.csv"
    config.project_info_file = "tests/test_data/example_user_inputs_two_metered.csv"
    config.reset_elec_load_shapes = True
    return config


@pytest.fixture
def config_with_both_load_shapes(config: FLEXValueConfig):
    config.project_info_file = "tests/test_data/example_user_inputs.csv"
    config.elec_load_shape_file = "tests/test_data/ca_hourly_electric_load_shapes.csv"
    config.metered_load_shape_file = "tests/test_data/example_metered_load_shape.csv"
    config.reset_elec_load_shapes = True
    return config


@pytest.fixture
def basic_calc_config(config: FLEXValueConfig):
    config.project_info_file = "tests/test_data/example_user_inputs.csv"
    config.separate_output_tables = False
    config.aggregation_columns = ["id"]
    config.output_table = "basic_calc_test_output"
    return config

@pytest.fixture
def reset_project_info(config: FLEXValueConfig):
    dbm = DBManager.get_db_manager(config)
    with dbm.engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS project_info;"))

@pytest.fixture
def check_av_costs(config: FLEXValueConfig):
    reload_elec_table = False
    reload_gas_table = False

    dbm = DBManager.get_db_manager(config)
    sql = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_name IN ('elec_av_costs', 'gas_av_costs')
    """
    results = dbm._exec_select_sql(sql)

    if len(results) == 0:
        print('No av costs tables found.')
        reload_elec_table = True
        reload_gas_table = True

    for result in results:
        if result[0] == "elec_av_costs":
            reload_elec_table = False
            row_count = dbm._exec_select_sql("SELECT COUNT(*) FROM elec_av_costs;")
            if row_count[0][0] == 0:
                print('No elec_av_costs table found.')
                reload_elec_table = True
            break
        reload_elec_table = True

    for result in results:
        if result[0] == "gas_av_costs":
            reload_gas_table = False
            row_count = dbm._exec_select_sql("SELECT COUNT(*) FROM gas_av_costs;")
            if row_count[0][0] == 0:
                print('No gas_av_costs table found.')
                reload_gas_table = True
            break
        reload_gas_table = True
            
    if not reload_elec_table:
        sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'elec_av_costs'
        """
        results = dbm._exec_select_sql(sql)
        for result in results:
            if result[0] == "value_curve_name":
                reload_elec_table = False
                break
            reload_elec_table = True
        if reload_elec_table:
            print('Value curve name not found in elec av costs table.')
        

    if not reload_gas_table:
        sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'gas_av_costs'
        """
        results = dbm._exec_select_sql(sql)
        for result in results:
            if result[0] == "value_curve_name":
                reload_gas_table = False
                break
            reload_gas_table = True
        if reload_gas_table:
            print('Value curve name not found in gas av costs table.')

    if not reload_elec_table:
        print(reload_elec_table)
        sql = """
        SELECT DISTINCT value_curve_name
        FROM elec_av_costs
        """
        results = dbm._exec_select_sql(sql)
        if len(results) < 2:
            print('Only a single value_curve_name found in elec av costs table.')
            reload_elec_table = True

    if not reload_gas_table:
        sql = """
        SELECT DISTINCT value_curve_name
        FROM gas_av_costs
        """
        results = dbm._exec_select_sql(sql)
        if len(results) < 2:
            print('Only a single value_curve_name found in gas av costs table.')
            reload_gas_table = True
        
    if reload_elec_table:
        print('reloading elec table')
        with dbm.engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS elec_av_costs;")) 
        dbm.process_elec_av_costs(config.elec_av_costs_file)
    
    if reload_gas_table:
        print('reloading gas table')
        with dbm.engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS gas_av_costs;"))
        dbm.process_gas_av_costs(config.gas_av_costs_file)
    
    return None

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
        gas_av_costs_table="gas_av_costs",
        elec_load_shape_table="elec_load_shape",
        therms_profiles_table="therms_profile",
        project_info_file="tests/test_data/example_user_inputs_cz12_37.csv",
        electric_output_table="afsepo_output_table_electric",
        gas_output_table="afsepo_output_table_gas",
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
        database_type="postgresql",
        host=TEST_HOST,
        port=TEST_PORT,
        user=TEST_USER,
        password=TEST_PASSWORD,
        database=TEST_DATABASE,
        elec_av_costs_table="elec_av_costs",
        gas_av_costs_table="gas_av_costs",
        elec_load_shape_table="elec_load_shape",
        therms_profiles_table="therms_profile",
        project_info_file="tests/test_data/example_user_inputs_cz12_37.csv",
        output_table="output_table",
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
        database_type="postgresql",
        host=TEST_HOST,
        port=TEST_PORT,
        user=TEST_USER,
        password=TEST_PASSWORD,
        database=TEST_DATABASE,
        elec_av_costs_table="elec_av_costs",
        gas_av_costs_table="gas_av_costs",
        elec_load_shape_table="elec_load_shape",
        therms_profiles_table="therms_profile",
        project_info_file="tests/test_data/example_user_inputs_cz12_37.csv",
        output_table="output_table",
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
        database_type="postgresql",
        host=TEST_HOST,
        port=TEST_PORT,
        user=TEST_USER,
        password=TEST_PASSWORD,
        database=TEST_DATABASE,
        elec_av_costs_table="elec_av_costs",
        gas_av_costs_table="gas_av_costs",
        elec_load_shape_table="elec_load_shape",
        therms_profiles_table="therms_profile",
        project_info_file="tests/test_data/example_user_inputs_cz12_37.csv",
        electric_output_table="nafsepo_output_table_electric",
        gas_output_table="nafsepo_output_table_gas",
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
        database_type="postgresql",
        host=TEST_HOST,
        port=TEST_PORT,
        user=TEST_USER,
        password=TEST_PASSWORD,
        database=TEST_DATABASE,
        elec_av_costs_table="elec_av_costs",
        gas_av_costs_table="gas_av_costs",
        elec_load_shape_table="elec_load_shape",
        project_info_file="tests/test_data/example_user_inputs_cz12_37.csv",
        output_table="apinfso_output_table",
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

def test_project_load(reset_project_info: Callable[[FLEXValueConfig], None], config_with_project: FLEXValueConfig):
    dbm = DBManager.get_db_manager(config_with_project)
    dbm.process_project_info(config_with_project.project_info_file)
    result = dbm._exec_select_sql("SELECT COUNT(*) FROM project_info;")
    assert result[0][0] == 5


def test_project_load_no_header(reset_project_info: Callable[[FLEXValueConfig], None], config_with_no_header_project: FLEXValueConfig):
    dbm = DBManager.get_db_manager(config_with_no_header_project)
    dbm._reset_table("project_info")
    dbm.process_project_info(config_with_no_header_project.project_info_file)
    result = dbm._exec_select_sql("SELECT COUNT(*) FROM project_info;")
    assert result[0][0] == 5


def test_project_load_more_projects(reset_project_info: Callable[[FLEXValueConfig], None], config_with_more_projects: FLEXValueConfig):
    dbm = DBManager.get_db_manager(config_with_more_projects)
    dbm._reset_table("project_info")
    dbm.process_project_info(config_with_more_projects.project_info_file)
    result = dbm._exec_select_sql("SELECT COUNT(*) FROM project_info;")
    assert result[0][0] == 380


def test_elec_load_shape(config_with_elec_load_shape: FLEXValueConfig):
    dbm = DBManager.get_db_manager(config_with_elec_load_shape)
    dbm.reset_elec_load_shape()
    dbm.process_elec_load_shape(config_with_elec_load_shape.elec_load_shape_file)
    result = dbm._exec_select_sql("SELECT COUNT(*) FROM elec_load_shape;")
    assert result[0][0] == 665760


def test_metered_load_shape(config_with_metered_load_shape: FLEXValueConfig):
    dbm = DBManager.get_db_manager(config_with_metered_load_shape)
    dbm.reset_elec_load_shape()
    dbm.process_project_info(config_with_metered_load_shape.project_info_file)
    dbm.process_metered_load_shape(
        config_with_metered_load_shape.metered_load_shape_file
    )
    result = dbm._exec_select_sql("SELECT COUNT(*) FROM elec_load_shape;")
    assert result[0][0] == 8760 * 2


def test_two_metered_load_shapes(config_with_two_metered_load_shapes: FLEXValueConfig):
    dbm = DBManager.get_db_manager(config_with_two_metered_load_shapes)
    dbm.reset_elec_load_shape()
    dbm.process_project_info(config_with_two_metered_load_shapes.project_info_file)
    dbm.process_metered_load_shape(
        config_with_two_metered_load_shapes.metered_load_shape_file
    )
    result = dbm._exec_select_sql("SELECT COUNT(*) FROM elec_load_shape;")
    assert result[0][0] == 8760 * 2


def test_both_load_shapes(config_with_both_load_shapes: FLEXValueConfig):
    dbm = DBManager.get_db_manager(config_with_both_load_shapes)
    dbm.reset_elec_load_shape()
    dbm.process_elec_load_shape(config_with_both_load_shapes.elec_load_shape_file)
    dbm.process_project_info(config_with_both_load_shapes.project_info_file)
    dbm.process_metered_load_shape(config_with_both_load_shapes.metered_load_shape_file)
    result = dbm._exec_select_sql("SELECT COUNT(*) FROM elec_load_shape;")
    assert result[0][0] == 665760 + (8760 * 2)


def test_therms_profiles(config_with_therms_profiles: FLEXValueConfig):
    dbm = DBManager.get_db_manager(config_with_therms_profiles)
    dbm.reset_therms_profiles()
    dbm.process_therms_profile(config_with_therms_profiles.therms_profiles_file)
    result = dbm._exec_select_sql("SELECT COUNT(*) FROM therms_profile;")
    assert result[0][0] == 144


# NOTE This test takes several minutes to run due to loading the data.
def test_elec_avoided_costs(check_av_costs: Callable[[FLEXValueConfig], None], config_with_elec_avcosts: FLEXValueConfig):
    dbm = DBManager.get_db_manager(config_with_elec_avcosts)
    dbm.reset_elec_av_costs()
    dbm.process_elec_av_costs(config_with_elec_avcosts.elec_av_costs_file)
    result = dbm._exec_select_sql("SELECT COUNT(*) FROM elec_av_costs;")
    assert result[0][0] == 1121280


def test_gas_avoided_costs(check_av_costs: Callable[[FLEXValueConfig], None], config_with_gas_avcosts: FLEXValueConfig):
    dbm = DBManager.get_db_manager(config_with_gas_avcosts)
    dbm.reset_gas_av_costs()
    dbm.process_gas_av_costs(config_with_gas_avcosts.gas_av_costs_file)
    result = dbm._exec_select_sql("SELECT COUNT(*) FROM gas_av_costs;")
    assert result[0][0] == 1488


def test_basic_calculations(check_av_costs: Callable[[FLEXValueConfig], None], basic_calc_config: FLEXValueConfig):
    dbm = DBManager.get_db_manager(basic_calc_config)
    dbm.process_project_info(basic_calc_config.project_info_file)
    dbm.run()
    result = dbm._exec_select_sql("SELECT COUNT(*) FROM basic_calc_test_output;")
    # We expect 5 rows because even though id heat_pump2 doesn't match on the electric side, the gas values still come through
    assert result[0][0] == 5
    expected_results = {
        "heat_pump": -626.2409452335787, 
        "heat_pump2": 4569.487287092273,
        "deer_id_2": 301.3736100226564, 
        "deer_id_0": 1073.82886247403675, 
        "deer_id_1": 13278.400865620453, 
    }
    result = dbm._exec_select_sql(
        "SELECT id, SUM(electric_benefits), SUM(gas_benefits) from basic_calc_test_output GROUP BY id;"
    )
    # We're checking the electric benefits for all projects other than heat_pump2, for which we check gas benefits
    for row in result:
        assert math.isclose(
            expected_results[row[0]], row[2] if row[0] == "heat_pump2" else row[1]
        )


# _exec_select_sql returns a list of tuples, so for SELECT COUNT(*) queries
# we will always be looking at `result[0][0]`
def test_addl_fields_sep_output(check_av_costs: Callable[[FLEXValueConfig], None], addl_fields_sep_output):
    addl_fields_sep_output.run()
    result = addl_fields_sep_output.db_manager._exec_select_sql(
        "SELECT COUNT(*) FROM afsepo_output_table_electric"
    )
    assert result[0][0] == 3153600
    result = addl_fields_sep_output.db_manager._exec_select_sql(
        "SELECT COUNT(*) FROM afsepo_output_table_gas"
    )
    # sum(eul) of the projects = 380; since we have to agg by the addl_columns, we include month, so 380 * 12 = 4560
    assert result[0][0] == 4560


def test_addl_fields_same_output(check_av_costs: Callable[[FLEXValueConfig], None], addl_fields_same_output):
    addl_fields_same_output.run()
    result = addl_fields_same_output.db_manager._exec_select_sql(
        "SELECT COUNT(*) FROM output_table"
    )
    # sum(eul) of the projects that we have load shape data for == 360; 360 * 8760 == 3153600
    # We also need to include project ids 138 and 139 that have an electric load shape that doesn't match the load shape database
    # These projects do match on the gas profile, so we expect 20 more rows (from a 15 and 5 year gas eul)
    # These leaves the total expected number of rows at 3153600 + 20 = 3153620
    assert result[0][0] == 3153620


def test_no_addl_fields_sep_output(check_av_costs: Callable[[FLEXValueConfig], None], no_addl_fields_sep_output):
    no_addl_fields_sep_output.run()
    result = no_addl_fields_sep_output.db_manager._exec_select_sql(
        "SELECT COUNT(*) FROM nafsepo_output_table_electric"
    )
    # sum(eul) of the projects that we have load shape data for == 360; 360 * 8760 == 3153600
    assert result[0][0] == 3153600
    result = no_addl_fields_sep_output.db_manager._exec_select_sql(
        "SELECT COUNT(*) FROM nafsepo_output_table_gas"
    )
    # sum(eul) of the projects that we have therm profile data for == 380; we are aggregating at the year level, so this should be 380
    assert result[0][0] == 380


def test_no_addl_fields_same_output(check_av_costs: Callable[[FLEXValueConfig], None], no_addl_fields_same_output):
    no_addl_fields_same_output.run()
    result = no_addl_fields_same_output.db_manager._exec_select_sql(
        "SELECT COUNT(*) FROM output_table"
    )
    # sum(eul) of the projects that we have load shape data for == 360; 360 * 8760 == 3153600
    # We also need to include project ids 138 and 139 that have an electric load shape that doesn't match the load shape database
    # These projects do match on the gas profile, so we expect 20 more rows (from a 15 and 5 year gas eul)
    # These leaves the total expected number of rows at 3153600 + 20 = 3153620
    assert result[0][0] == 3153620


def test_agg_id_no_fields_same_output(check_av_costs: Callable[[FLEXValueConfig], None], agg_id_no_fields_same_output):
    agg_id_no_fields_same_output.run()
    result = agg_id_no_fields_same_output.db_manager._exec_select_sql(
        "SELECT COUNT(*) FROM apinfso_output_table"
    )
    assert (
        result[0][0] == 38
    )  # 38 distinct projects even with 2 projects with no matching loadshape (because they match on the gas loadshape)
