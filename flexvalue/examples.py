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
import random
import pandas as pd
from .db import get_all_valid_deer_load_shapes

__all__ = (
    "get_example_user_inputs_deer",
    "get_example_user_inputs_metered",
    "get_example_metered_load_shape",
)


def _generate_example_ids():
    return [f"id_{i}" for i in range(5)]


def get_example_metered_load_shape():
    """Generates an example metered load shape file for use in evaluating this tool

    Returns
    -------
    metered load shape: pd.DataFrame
    """
    test_ids = _generate_example_ids()
    random.seed(0)
    output = []
    for _id in test_ids:
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


def get_example_user_inputs_metered():
    """Generates an example user_inputs file that references the example metered load shape

    Returns
    -------
    user_inputs: pd.DataFrame
    """
    metered_load_shape = get_example_metered_load_shape()
    return pd.DataFrame(
        [
            {
                "ID": _id,
                "load_shape": _id,
                "start_year": 2021,
                "start_quarter": (i % 4) + 1,
                "utility": "PGE",
                "climate_zone": "CZ3A",
                "units": 1,
                "eul": 9,
                "ntg": 0.95,
                "discount_rate": 0.0766,
                "admin": 10000 * (i + 1),
                "measure": 2000 * (i + 1),
                "incentive": 3000 * (i + 1),
                "therms_profile": "annual",
                "therms_savings": (i + 1) * 100,
                "mwh_savings": metered_load_shape[_id].sum(),
            }
            for i, _id in enumerate(_generate_example_ids())
        ]
    ).set_index("ID")


def get_example_user_inputs_deer(database_year):
    """Generates an example user_inputs file tied to a few deer load shapes

    Parameters
    ----------
    database_year: str
        The year of the database to use when selecting DEER load shapes to reference

    Returns
    -------
    user_inputs: pd.DataFrame
    """
    return pd.DataFrame(
        [
            {
                "ID": f"deer_id_{i}",
                "load_shape": deer,
                "start_year": database_year,
                "start_quarter": (i % 4) + 1,
                "utility": "PGE",
                "climate_zone": "CZ3A",
                "units": 1,
                "eul": 9,
                "ntg": 0.95,
                "discount_rate": 0.0766,
                "admin": 10000 * (i + 1),
                "measure": 2000 * (i + 1),
                "incentive": 3000 * (i + 1),
                "therms_profile": "annual",
                "therms_savings": (i + 1) * 100,
                "mwh_savings": (i + 1) * 1000,
            }
            for i, deer in enumerate(get_all_valid_deer_load_shapes(database_year)[:5])
        ]
    ).set_index("ID")
