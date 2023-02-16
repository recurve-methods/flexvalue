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

@pytest.fixture
def dbm():
    return DBManager("")

@pytest.fixture
def dbm_with_project_loaded(dbm):
    dbm.load_project_info_file("example_user_inputs_deer.csv")
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

# _exec_select_sql returns a list of tuples, so for SELECT COUNT(*) queries
# we will always be looking at `result[0][0]`
def test_project_load(dbm_with_project_loaded):
    result = dbm_with_project_loaded._exec_select_sql("SELECT COUNT(*) FROM project_info;")
    assert result[0][0] == 5

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