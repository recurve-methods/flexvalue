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
from io import BytesIO
import os
import random
import requests
import pandas as pd

from .db import get_all_valid_deer_load_shapes

__all__ = (
    "get_example_user_inputs_deer",
    "get_example_user_inputs_metered",
    "get_example_metered_load_shape",
)


def get_example_metered_load_shape():
    """Generates an example metered load shape file for use in evaluating this tool

    Returns
    -------
    metered load shape: pd.DataFrame
    """

    url_prefix = "https://storage.googleapis.com/flexvalue-public-resources/examples/"
    url = os.path.join(url_prefix, "example_metered_load_shapes.csv")
    response = requests.get(url)
    df = pd.read_csv(BytesIO(response.content)).set_index("hour_of_year")
    return df


def get_example_user_inputs_metered(ids):
    """Generates an example user_inputs file that references the example metered load shape

    Returns
    -------
    user_inputs: pd.DataFrame
    """
    return pd.DataFrame(
        [
            {
                "ID": id_,
                "load_shape": id_,
                "start_year": 2021,
                "start_quarter": 1,
                "utility": "PGE",
                "climate_zone": "CZ12",
                "units": 1,
                "eul": 15,
                "ntg": 1.0,
                "discount_rate": 0.0766,
                "admin": 100,
                "measure": 2000,
                "incentive": 1000,
                "therms_profile": "winter",
                "therms_savings": 400,
                "mwh_savings": 1,
            }
            for id_ in ids
        ]
    ).set_index("ID")


def get_example_user_inputs_deer(database_version):
    """Generates an example user_inputs file tied to a few deer load shapes

    Parameters
    ----------
    database_version: str
        The version of the database to use when selecting DEER load shapes to reference

    Returns
    -------
    user_inputs: pd.DataFrame
    """
    return pd.DataFrame(
        [
            {
                "ID": f"deer_id_{i}",
                "load_shape": deer,
                "start_year": database_version,
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
            for i, deer in enumerate(
                get_all_valid_deer_load_shapes(database_version)[:5]
            )
        ]
    ).set_index("ID")
