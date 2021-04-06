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
import os


def database_location():
    return os.environ.get("DATABASE_LOCATION", ".")


#: Columns that represent the electricity avoided costs components that must be aggregated
ACC_COMPONENTS_ELECTRICITY = [
    "total",
    "energy",
    "losses",
    "ancillary_services",
    "capacity",
    "transmission",
    "distribution",
    "cap_and_trade",
    "ghg_adder_rebalancing",
    "methane_leakage",
]
ACC_COMPONENTS_GAS = ["market", "t_d", "environment", "upstream_methane", "total"]

THERMS_PROFILE_ADJUSTMENT = {
    "PGE": {"annual": 0.9427, "summer": 0.8293, "winter": 1.0558},
    "SCE": {"annual": 0.8948, "summer": 0.8282, "winter": 0.9611},
    "SDGE": {"annual": 0.9435, "summer": 0.8394, "winter": 1.0469},
}
