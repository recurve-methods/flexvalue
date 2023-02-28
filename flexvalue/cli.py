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
import click
from pathlib import Path

from .calculations import run

__all__ = (
    "get_results",
)


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--project-info",
    help="Filepath to the project information file that is used to calculate results"
)
@click.option(
    "--config-file",
    help="Path to the toml configuration file."
)
@click.option(
    "--elec-av-costs-file",
    help="Filepath to the electric avoided costs."
)
@click.option(
    "--gas-av-costs-file",
    help="Filepath to the gas avoided costs."
)
@click.option(
    "--elec-load-shape-file",
    help="Filepath to the hourly electric load shape file."
)
@click.option(
    "--therms-profiles-file",
    help="Filepath to the therms profiles file."
)
@click.option(
    "--reset-elec-load-shape",
    help="Clear all data from the electric load shape table.",
    is_flag=True
)
@click.option(
    "--reset-elec-av-costs",
    help="Clear all data from the electric avoided costs table.",
    is_flag=True
)
@click.option(
    "--reset-therms-profiles",
    help="Clear all data from the therms profiles table.",
    is_flag=True
)
@click.option(
    "--reset-gas-av-costs",
    help="Clear all data from the gas avoided costs table.",
    is_flag=True
)
def get_results(
    config_file,
    project_info,
    elec_av_costs_file,
    gas_av_costs_file,
    elec_load_shape_file,
    therms_profiles_file,
    reset_elec_load_shape,
    reset_elec_av_costs,
    reset_therms_profiles,
    reset_gas_av_costs
):
    # print(f"reset_elec_load_shape = {reset_elec_load_shape}, reset_elec_av_costs={reset_elec_av_costs}, reset_therms_profiles={reset_therms_profiles}, reset_gas_av_costs={reset_gas_av_costs}")
    print(f"config file = {config_file}, project_info = {project_info}")
    try:
        run(config_file,
            project_info=project_info,
            elec_av_costs=elec_av_costs_file,
            gas_av_costs=gas_av_costs_file,
            elec_load_shape_file=elec_load_shape_file,
            therms_profiles_path=therms_profiles_file,
            reset_elec_load_shape=reset_elec_load_shape,
            reset_elec_av_costs=reset_elec_av_costs,
            reset_therms_profiles=reset_therms_profiles,
            reset_gas_av_costs=reset_gas_av_costs
        )
    except ValueError as e:
        print(e)
