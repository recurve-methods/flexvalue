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
def dbm_with_project_loaded():
    dbm = DBManager("")
    dbm.load_project_info_file("example_user_inputs_deer.csv")
    return dbm

@pytest.fixture
def dbm_with_elec_load_shape_loaded():
    dbm = DBManager("")
    # TRUNCATE the table first so we can assert the correct number of rows
    dbm.load_elec_load_shapes_file("ca_hourly_electric_load_shapes.csv", truncate=True)
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
