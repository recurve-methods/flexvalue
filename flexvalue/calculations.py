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
)

# TODO Do we even want this file?
def run(db_config_path=None, project_info=None, elec_av_costs=None, gas_av_costs=None,
    elec_load_shape_file=None, therms_profiles_path=None, reset_elec_load_shape=None,
    reset_elec_av_costs=None, reset_therms_profiles=None, reset_gas_av_costs=None):
    db_manager = DBManager(db_config_path=db_config_path)
    if reset_elec_load_shape:
        db_manager.reset_elec_load_shape()
    if reset_elec_av_costs:
        db_manager.reset_elec_av_costs()
    if reset_therms_profiles:
        db_manager.reset_therms_profiles()
    if reset_gas_av_costs:
        db_manager.reset_gas_av_costs()
    if elec_av_costs:
        db_manager.load_elec_avoided_costs_file(elec_av_costs_path=elec_av_costs)
    if gas_av_costs:
        db_manager.load_gas_avoided_costs_file(gas_av_costs_path=gas_av_costs)
    if elec_load_shape_file:
        db_manager.load_elec_load_shapes_file(elec_load_shape_file)
    if project_info:
        db_manager.load_project_info_file(project_info_path=project_info)
    if therms_profiles_path:
        db_manager.load_therms_profiles_file(therms_profiles_path=therms_profiles_path)

