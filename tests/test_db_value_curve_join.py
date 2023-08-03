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
        elec_av_costs_file="tests/test_data/test_value_curve_join_elec_acc.csv",
        gas_av_costs_file="tests/test_data/test_value_curve_join_gas_acc.csv"
    )
        
    return config


@pytest.fixture
def config_with_value_curve_projects(config: FLEXValueConfig):
    config.project_info_file = "tests/test_data/example_user_inputs.csv"
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
def agg_same_output_value_curve_name():
    return FlexValueRun(
        database_type="postgresql",
        host=TEST_HOST,
        port=TEST_PORT,
        user=TEST_USER,
        password=TEST_PASSWORD,
        database=TEST_DATABASE,
        elec_load_shape_table="elec_load_shape",
        therms_profiles_table="therms_profile",
        project_info_file="tests/test_data/value_curve_join_inputs_2.csv",
        elec_av_costs_table="elec_av_costs",
        gas_av_costs_table="gas_av_costs",
        output_table="agg_value_curve_name_output_table",
        aggregation_columns=["id"],
        elec_addl_fields=["value_curve_name"],
        gas_addl_fields=["value_curve_name"],
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
        use_value_curve_name_for_join=True
    )

@pytest.fixture
def agg_sep_output_value_curve_name():
    return FlexValueRun(
        database_type="postgresql",
        host=TEST_HOST,
        port=TEST_PORT,
        user=TEST_USER,
        password=TEST_PASSWORD,
        database=TEST_DATABASE,
        elec_load_shape_table="elec_load_shape",
        therms_profiles_table="therms_profile",
        project_info_file="tests/test_data/value_curve_join_inputs_2.csv",
        elec_av_costs_table="elec_av_costs",
        gas_av_costs_table="gas_av_costs",
        electric_output_table="agg_value_curve_name_output_table_electric",
        gas_output_table="agg_value_curve_name_output_table_gas",
        aggregation_columns=["id"],
        elec_addl_fields=["value_curve_name"],
        gas_addl_fields=["value_curve_name"],
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
        use_value_curve_name_for_join=True
    )


def test_value_curve_name_join_same_output(check_av_costs: Callable[[FLEXValueConfig], None], agg_same_output_value_curve_name):
    agg_same_output_value_curve_name.run()

    results_list = agg_same_output_value_curve_name.db_manager._exec_select_sql(
        "SELECT id, value_curve_name, electric_benefits, gas_benefits FROM agg_value_curve_name_output_table"
    )

    # We expect both input meters to appear in the outputs because the value curve joined has not dropped any meters.
    assert len(results_list) == 2
    # We expect rows with the same inputs but different value curve names to yield distinct values.
    for result in results_list:
        if result[0] == 'v2020':
            assert result[1] == 'v2020'
            assert math.isclose(result[2], 10442.47, rel_tol=0.01)
            assert math.isclose(result[3], 95.36, rel_tol=0.01)
        if result[0] == 'v2022':
            assert result[1] == 'v2022'
            assert math.isclose(result[2], 8586.40, rel_tol=0.01)
            assert math.isclose(result[3], 128.09, rel_tol=0.01)

def test_value_curve_name_join_sep_output(check_av_costs: Callable[[FLEXValueConfig], None], agg_sep_output_value_curve_name):
    agg_sep_output_value_curve_name.run()
    
    results_list = agg_sep_output_value_curve_name.db_manager._exec_select_sql(
        "SELECT id, value_curve_name, electric_benefits FROM agg_value_curve_name_output_table_electric"
    )

    # We expect both input meters to appear in the outputs because the value curve joined has not dropped any meters.
    assert len(results_list) == 2
    # We expect rows with the same inputs but different value curve names to yield distinct values.
    for result in results_list:
        if result[0] == 'v2020':
            assert result[1] == 'v2020'
            assert math.isclose(result[2], 10442.47, rel_tol=0.01)
        if result[0] == 'v2022':
            assert result[1] == 'v2022'
            assert math.isclose(result[2], 8586.40, rel_tol=0.01)
    
    results_list = agg_sep_output_value_curve_name.db_manager._exec_select_sql(
        "SELECT id, value_curve_name, gas_benefits FROM agg_value_curve_name_output_table_gas"
    )

    # We expect both input meters to appear in the outputs because the value curve joined has not dropped any meters.
    assert len(results_list) == 2
    # We expect rows with the same inputs but different value curve names to yield distinct values.
    for result in results_list:
        if result[0] == 'v2020':
            assert result[1] == 'v2020'
            assert math.isclose(result[2], 95.36, rel_tol=0.01)
        if result[0] == 'v2022':
            assert result[1] == 'v2022'
            assert math.isclose(result[2], 128.09, rel_tol=0.01)

