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
import numpy as np
import pandas as pd
import pytest

from flexvalue.calculations import FlexValueRun

from flexvalue.examples import (
    get_example_user_inputs_metered,
    get_example_metered_load_shape,
)


def test_user_inputs_from_example_metered(snapshot):

    metered_load_shape = get_example_metered_load_shape()
    flexvalue_run = FlexValueRun(metered_load_shape=metered_load_shape)

    user_inputs = get_example_user_inputs_metered(metered_load_shape.columns)
    (
        df_output_table,
        df_output_table_totals,
        elec_benefits,
        gas_benefits,
    ) = flexvalue_run.get_results(user_inputs)
    snapshot.assert_match(df_output_table_totals, "df_output_table_totals")


@pytest.fixture
def flexvalue_run(metered_load_shape):
    return FlexValueRun(metered_load_shape=metered_load_shape)


def test_electric_benefits_full_outputs(
    user_inputs, metered_load_shape, snapshot, flexvalue_run
):
    elec_ben = flexvalue_run.get_electric_benefits_full_outputs(user_inputs)
    snapshot.assert_match(elec_ben, "elec_ben")


def test_user_inputs_basic(user_inputs, metered_load_shape, snapshot, flexvalue_run):
    _, df_output_table_totals, _, _ = flexvalue_run.get_results(user_inputs)
    snapshot.assert_match(df_output_table_totals, "df_output_table_totals")


def test_user_inputs_full(user_inputs, metered_load_shape, snapshot, flexvalue_run):
    (
        df_output_table,
        df_output_table_totals,
        elec_ben,
        gas_ben,
    ) = flexvalue_run.get_results(user_inputs)
    snapshot.assert_match(df_output_table_totals, "df_output_table_totals")


def test_user_inputs_single_row(
    user_inputs, metered_load_shape, snapshot, flexvalue_run
):
    (
        df_output_table,
        df_output_table_totals,
        elec_ben,
        gas_ben,
    ) = flexvalue_run.get_results(user_inputs[-1:])
    snapshot.assert_match(df_output_table_totals, "df_output_table_totals")
