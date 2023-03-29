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
from dataclasses import dataclass
from .config import FLEXValueConfig

from .db import (
    DBManager,
)  # get_filtered_acc_elec, get_filtered_acc_gas, get_deer_load_shape

# from .settings import (
#     ACC_COMPONENTS_ELECTRICITY,
#     ACC_COMPONENTS_GAS,
#     THERMS_PROFILE_ADJUSTMENT,
# )

__all__ = ("run",)


# TODO Do we even want this file?
def run(
    config_file=None,
    **kwargs
):
    try:
        config = FLEXValueConfig.from_file(config_file)
    except TypeError:
        config = FLEXValueConfig(**kwargs)
    db_manager = DBManager.get_db_manager(config)
    if config.reset_elec_load_shape:
        db_manager.reset_elec_load_shape()
    if config.reset_elec_av_costs:
        db_manager.reset_elec_av_costs()
    if config.reset_therms_profiles:
        db_manager.reset_therms_profiles()
    if config.reset_gas_av_costs:
        db_manager.reset_gas_av_costs()

    if config.process_elec_av_costs:
        db_manager.process_elec_av_costs(
            config.elec_av_costs_file if config.elec_av_costs_file else config.elec_av_costs_table
        )
    # Have to load elec load shape after avoided costs
    if config.process_elec_load_shape:
        db_manager.process_elec_load_shape(
            config.elec_load_shape_file if config.elec_load_shape_file else config.elec_load_shape_table
        )
    if config.process_gas_av_costs:
        db_manager.process_gas_av_costs(
            config.gas_av_costs_file if config.gas_av_costs_file else config.gas_av_costs_table
        )
    if config.process_therms_profiles:
        db_manager.process_therms_profile(
            config.therms_profiles_file if config.therms_profiles_file else config.therms_profiles_file
        )
    if config.project_info_file or config.project_info_table:
        db_manager.process_project_info(
            config.project_info_file if config.project_info_file else config.project_info_table
        )
        db_manager.run()
