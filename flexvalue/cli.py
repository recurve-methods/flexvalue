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

from .calculations import run
from .config import FLEXValueException

__all__ = (
    "get_results",
)


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--project-info-file",
    help="Filepath to the project information file that is used to calculate results"
)
@click.option(
    "--database-type",
    help="One of 'postgresql', 'bigquery', or 'sqlite'"
)
@click.option(
    "--host",
    help="The host for the postgresql database to which you are connecting."
)
@click.option(
    "--port",
    help="The port for the postgresql database to which you are connecting."
)
@click.option(
    "--user",
    help="The user for the postgresql database to which you are connecting."
)
@click.option(
    "--password",
    help="The password for the postgresql database to which you are connecting."
)
@click.option(
    "--database",
    help="The database for the postgresql database to which you are connecting."
)
@click.option(
    "--elec-av-costs-table",
    help="Used when --database-type is bigquery. Specifies the electric avoided costs table."
)
@click.option(
    "--elec-load-shape-table",
    help="Used when --database-type is bigquery. Specifies the electric load shape table."
)
@click.option(
    "--gas-av-costs-table",
    help="Used when --database-type is bigquery. Specifies the gas avoided costs table."
)
@click.option(
    "--therms-profiles-table",
    help="Used when --database-type is bigquery. Specifies the therms profiles table."
)
@click.option(
    "--project-info-table",
    help="Used when --database-type is bigquery. Specifies the table containing the project information."
)
@click.option(
    "--project",
    help="Used when --database-type is bigquery. Specifies the google project."
)
@click.option(
    "--dataset",
    help="Used when --database-type is bigquery. Specifies the dataset."
)
@click.option(
    "--output-table",
    help="The database table to write output to. This table gets overwritten (not appended to)."
)
@click.option(
    "--config-file",
    help="Path to the toml configuration file."
)
@click.option(
    "--elec-av-costs-file",
    help="Filepath to the electric avoided costs. Used when --database-type is not BigQuery to load this data into the database from a file."
)
@click.option(
    "--gas-av-costs-file",
    help="Filepath to the gas avoided costs. Used when --database-type is not BigQuery to load this data into the database from a file."
)
@click.option(
    "--elec-load-shape-file",
    help="Filepath to the hourly electric load shape file. Used when --database-type is not BigQuery to load this data into the database from a file."
)
@click.option(
    "--therms-profiles-file",
    help="Filepath to the therms profiles file. Used when --database-type is not BigQuery to load this data into the database from a file."
)
@click.option(
    "--aggregation-columns",
    default=[],
    help="Comma-separated list of field names on which to aggregate the query."
)
@click.option(
    "--reset-elec-load-shape",
    help="Reset the data in the electric load shape table. This restores it to its contents prior to running FLEXvalue.",
    is_flag=True
)
@click.option(
    "--reset-elec-av-costs",
    help="Reset the data in the electric avoided costs table. This restores it to its contents prior to running FLEXvalue.",
    is_flag=True
)
@click.option(
    "--reset-therms-profiles",
    help="Reset the data in the therms profiles table. This restores it to its contents prior to running FLEXvalue.",
    is_flag=True
)
@click.option(
    "--reset-gas-av-costs",
    help="Reset the data in the gas avoided costs table. This restores it to its contents prior to running FLEXvalue.",
    is_flag=True
)
@click.option(
    "--process-elec-load-shape",
    help="Process (load/transform) the electric load shape data.",
    is_flag=True
)
@click.option(
    "--process-elec-av-costs",
    help="Process (load/transform) the electric avoided costs data.",
    is_flag=True
)
@click.option(
    "--process-therms-profiles",
    help="Process (load/transform) the therms profiles table.",
    is_flag=True
)
@click.option(
    "--process-gas-av-costs",
    help="Process (load/transform) the gas avoided costs data.",
    is_flag=True
)

def get_results(
    config_file,
    project_info_file,
    database_type,
    host,
    port,
    user,
    password,
    database,
    elec_av_costs_table,
    elec_load_shape_table,
    gas_av_costs_table,
    therms_profiles_table,
    project_info_table,
    project,
    dataset,
    output_table,
    elec_av_costs_file,
    gas_av_costs_file,
    elec_load_shape_file,
    therms_profiles_file,
    aggregation_columns,
    reset_elec_load_shape,
    reset_elec_av_costs,
    reset_therms_profiles,
    reset_gas_av_costs,
    process_elec_load_shape,
    process_elec_av_costs,
    process_therms_profiles,
    process_gas_av_costs
):
    try:
        print(f"******aggregation_columns = {aggregation_columns}")
        run(config_file=config_file,
            project_info_file=project_info_file,
            database_type=database_type,
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            elec_av_costs_table=elec_av_costs_table,
            elec_load_shape_table=elec_load_shape_table,
            gas_av_costs_table=gas_av_costs_table,
            therms_profiles_table=therms_profiles_table,
            project_info_table=project_info_table,
            project=project,
            dataset=dataset,
            output_table=output_table,
            elec_av_costs_file=elec_av_costs_file,
            gas_av_costs_file=gas_av_costs_file,
            elec_load_shape_file=elec_load_shape_file,
            therms_profiles_file=therms_profiles_file,
            reset_elec_load_shape=reset_elec_load_shape,
            reset_elec_av_costs=reset_elec_av_costs,
            reset_therms_profiles=reset_therms_profiles,
            reset_gas_av_costs=reset_gas_av_costs,
            aggregation_columns=aggregation_columns.split(",") if aggregation_columns else aggregation_columns,
            process_elec_load_shape=process_elec_load_shape,
            process_elec_av_costs=process_elec_av_costs,
            process_therms_profiles=process_therms_profiles,
            process_gas_av_costs=process_gas_av_costs
        )
    except FLEXValueException as e:
        print(e)
