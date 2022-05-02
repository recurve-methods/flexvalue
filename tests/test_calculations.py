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
import os
import random
import sqlite3
from tempfile import mkdtemp

from flexvalue.calculations import FlexValueRun
from flexvalue.settings import ACC_COMPONENTS_ELECTRICITY, ACC_COMPONENTS_GAS


@pytest.fixture
def metered_ids():
    return [f"id_{i}" for i in range(5)]


@pytest.fixture
def deer_ids():
    return ["DEER_LS_1", "DEER_LS_2"]


@pytest.fixture
def metered_load_shape(metered_ids):

    random.seed(0)
    output = []
    for _id in metered_ids:
        for hour in range(8760):
            savings = random.random() * 0.1
            output.append(
                {"identifier": _id, "hour_of_year": hour, "hourly_mwh_savings": savings}
            )
    df = (
        pd.DataFrame(output)
        .pivot(index="hour_of_year", columns="identifier", values="hourly_mwh_savings")
        .reset_index()
        .set_index("hour_of_year")
    )
    df.columns.name = None
    return df


@pytest.fixture
def user_inputs(metered_ids, deer_ids):
    return pd.DataFrame(
        [
            {
                "ID": id_,
                "load_shape": id_,
                "start_year": 2021,
                "start_quarter": 1,
                "utility": "PGE",
                "climate_zone": "CZ1",
                "units": 1,
                "eul": 5,
                "ntg": 1.0,
                "discount_rate": 0.0766,
                "admin": 100,
                "measure": 2000,
                "incentive": 1000,
                "therms_profile": "winter",
                "therms_savings": 400,
                "mwh_savings": 1,
            }
            for id_ in metered_ids + deer_ids
        ]
    ).set_index("ID")


@pytest.fixture
def database_version(pytestconfig):
    database_version = pytestconfig.getoption("database_version")
    if not database_version:
        database_version = "1111"
        db_path = mkdtemp()
        os.environ["DATABASE_LOCATION"] = db_path
        con = sqlite3.connect(f"{db_path}/{database_version}.db")

        random.seed(1)

        acc_elec_cols = {
            col: random.random()
            for col in ACC_COMPONENTS_ELECTRICITY + ["marginal_ghg"]
        }
        df_acc_elec = pd.DataFrame(
            [
                {
                    "climate_zone": "CZ1",
                    "utility": "PGE",
                    "hour_of_year": hour,
                    "hour_of_day": hour % 24,
                    "year": year,
                    "month": (
                        pd.Timestamp("2020-01-01") + pd.Timedelta(hour, unit="H")
                    ).month,
                    **acc_elec_cols,
                }
                for hour in range(0, 8760)
                for year in range(2020, 2051)
            ]
        )
        df_acc_elec.to_sql("acc_electricity", con=con)

        acc_gas_cols = {col: random.random() for col in ACC_COMPONENTS_GAS}
        df_acc_gas = pd.DataFrame(
            [
                {
                    "climate_zone": "CZ1",
                    "utility": "PGE",
                    "year": year,
                    "month": month,
                    **acc_gas_cols,
                }
                for month in range(1, 13)
                for year in range(2020, 2051)
            ]
        )
        df_acc_gas.to_sql("acc_gas", con=con)

        df_deer_load_shapes = pd.DataFrame(
            [
                {
                    "DEER_LS_1": random.random(),
                    "DEER_LS_2": random.random(),
                    "hour_of_year": hour,
                }
                for hour in range(0, 8760)
            ]
        )
        df_deer_load_shapes.to_sql("deer_load_shapes", con=con)
    return database_version


@pytest.fixture
def flexvalue_run(metered_load_shape, database_version):
    return FlexValueRun(
        metered_load_shape=metered_load_shape, database_version=database_version
    )


def test_user_inputs_from_example_metered(
    snapshot, database_version, user_inputs, flexvalue_run
):

    (
        df_output_table,
        df_output_table_totals,
        elec_benefits,
        gas_benefits,
    ) = flexvalue_run.get_results(user_inputs)
    snapshot.assert_match(df_output_table_totals, "df_output_table_totals")


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


def test_time_series_outputs(user_inputs, metered_load_shape, snapshot, flexvalue_run):
   
    outputs = flexvalue_run.get_time_series_results(user_inputs)
    elec_outputs = pd.concat([e for (e,g) in outputs]).reset_index(drop=True)
    outputs = flexvalue_run.get_time_series_results(user_inputs)
    gas_outputs = pd.concat([g for (e,g) in outputs]).reset_index(drop=True)
    
    snapshot.assert_match(elec_outputs, "time_series_elec_outputs")
    snapshot.assert_match(gas_outputs, "time_series_gas_outputs")