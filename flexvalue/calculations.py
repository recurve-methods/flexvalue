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

from .db import DBManager #get_filtered_acc_elec, get_filtered_acc_gas, get_deer_load_shape
# from .settings import (
#     ACC_COMPONENTS_ELECTRICITY,
#     ACC_COMPONENTS_GAS,
#     THERMS_PROFILE_ADJUSTMENT,
# )

__all__ = (
    "run",
    "get_quarterly_discount_df",
    "calculate_trc_costs",
    "calculate_pac_costs",
    "FlexValueProject",
    "FlexValueRun",
)


def run(db_config_path=None, project_info=None):
    db_manager = DBManager(db_config_path=db_config_path)
    db_manager.load_project_info_file(project_info_path=project_info)

