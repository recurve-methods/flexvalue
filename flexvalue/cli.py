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
import logging
import json
import ntpath
from pathlib import Path
import pandas as pd
import os
import re
import requests
from textwrap import dedent

from .calculations import FlexValueRun
from flexvalue import (
    get_all_valid_utility_climate_zone_combinations,
    get_all_valid_deer_load_shapes,
)
from .settings import database_location
from .examples import (
    get_example_metered_load_shape,
    get_example_user_inputs_deer,
    get_example_user_inputs_metered,
)
from .report import Notebook

__all__ = (
    "download_avoided_costs_data_db",
    "generate_example_inputs",
    "get_results",
    "valid_utility_climate_zone_combos",
    "valid_deer_load_shapes",
)


@click.group()
def cli():
    pass


@cli.command()
@click.option("-v", "--version", default="2020", show_default=True)
@click.option(
    "--url-prefix",
    default="https://storage.googleapis.com/flexvalue-public-resources/db/v1/",
    show_default=True,
)
@click.option("--skip-if-exists/--overwrite-if-exists", default=False)
def download_avoided_costs_data_db(url_prefix, version, skip_if_exists):
    """Downloads the avoided costs database. Options for version are 2020, 2021, and adjusted_acc_map.

    If you are having trouble downlading this database,
    you can instead use the following command:

    `curl --output 2020.db https://storage.googleapis.com/flexvalue-public-resources/db/v1/2020.db`

    """
    db_filename = f"{version}.db"
    output_filepath = os.path.join(database_location(), db_filename)
    Path(database_location()).mkdir(parents=True, exist_ok=True)

    if skip_if_exists and os.path.exists(output_filepath):
        logging.warning("This file already exists so it will not be downloaded again.")
    else:
        url = os.path.join(url_prefix, db_filename)
        response = requests.get(url)
        open(output_filepath, "wb").write(response.content)


@cli.command()
@click.option(
    "-v",
    "--version",
    default="2020",
    show_default=True,
    help="What version of the avoided costs data to use in the analysis",
)
@click.option(
    "--output-filepath",
    default=".",
    show_default=True,
    help="Filepath to where the example inputs will be stored",
)
def generate_example_inputs(output_filepath, version):
    """Generates the sample inputs that can be used in the get-results command"""
    Path(output_filepath).mkdir(parents=True, exist_ok=True)
    metered_df = get_example_metered_load_shape()

    metered_df.to_csv(os.path.join(output_filepath, "example_metered_load_shape.csv"))
    get_example_user_inputs_deer(version).to_csv(
        os.path.join(output_filepath, "example_user_inputs_deer.csv")
    )
    get_example_user_inputs_metered(metered_df.columns).to_csv(
        os.path.join(output_filepath, "example_user_inputs_metered.csv")
    )


@cli.command()
@click.option(
    "--user-inputs-filepath",
    required=True,
    help="Filepath to the user-inputs CSV file that is used to calculate results",
)
@click.option(
    "--metered-load-shape-filepath",
    help="Optional filepath to the CSV file containing metered load shapes",
)
@click.option(
    "--include-report/--exclude-report",
    default=True,
    show_default=True,
    help="Whether to include an HTML report in the outputs",
)
@click.option(
    "--report-filepath",
    default="report.html",
    show_default=True,
    help="Filepath to where the report will be saved",
)
@click.option(
    "--outputs-table-filepath",
    default="outputs_table.csv",
    show_default=True,
    help="Filepath to where the outputs table CSV will be saved",
)
@click.option(
    "-v",
    "--version",
    default="2020",
    show_default=True,
    help="What version of the avoided costs data to use in the analysis",
)
def get_results(
    user_inputs_filepath,
    metered_load_shape_filepath,
    include_report,
    outputs_table_filepath,
    report_filepath,
    version,
):
    """Calculates results and optionally writes a report"""
    if not include_report:
        metered_load_shape = (
            pd.read_csv(metered_load_shape_filepath, index_col="hour_of_year")
            if metered_load_shape_filepath
            else None
        )
        user_inputs = pd.read_csv(user_inputs_filepath)
        flexvalue_run = FlexValueRun(
            metered_load_shape=metered_load_shape, database_version=version
        )
        (
            outputs_table,
            outputs_table_totals,
            elec_benefits,
            gas_benefits,
        ) = flexvalue_run.get_results(user_inputs)
        outputs_table.to_csv(outputs_table_filepath, index=False)
    else:
        Path(ntpath.dirname(report_filepath)).mkdir(parents=True, exist_ok=True)
        nbf_nt = Notebook()
        # convert paths from filename into 'filename' so they are intepreted as strings
        metered_load_shape_filepath = (
            f'"{metered_load_shape_filepath}"'
            if metered_load_shape_filepath
            else metered_load_shape_filepath
        )
        user_inputs_filepath = f'"{user_inputs_filepath}"'
        outputs_table_filepath = f'"{outputs_table_filepath}"'
        version = f'"{version}"'
        content = dedent(
            f"""
            import pandas as pd
            from flexvalue import FlexValueRun
            from flexvalue.plots import plot_results
            from IPython.display import display
            metered_load_shape_filepath = {metered_load_shape_filepath}
            user_inputs_filepath = {user_inputs_filepath}
            outputs_table_filepath = {outputs_table_filepath}
            database_version = {version}
            metered_load_shape = (
                pd.read_csv(metered_load_shape_filepath, index_col="hour_of_year")
                if metered_load_shape_filepath
                else None
            )
            user_inputs = pd.read_csv(user_inputs_filepath)
            flexvalue_run = FlexValueRun(metered_load_shape=metered_load_shape, database_version=database_version)
            outputs_table, outputs_table_totals, elec_benefits, gas_benefits = flexvalue_run.get_results(
                user_inputs
            )
            display(user_inputs)
            display(outputs_table)
            plot_results(outputs_table_totals, elec_benefits, gas_benefits)
            outputs_table.to_csv(outputs_table_filepath, index=False)
            """
        )
        nbf_nt.add_code_cell(content)
        nbf_nt.execute()
        nbf_nt.to_html(report_filepath)


@cli.command()
@click.option("--utility", default=None, show_default=True)
@click.option(
    "-v",
    "--version",
    default="2020",
    show_default=True,
    help="What version of the avoided costs data to use in the analysis",
)
def valid_utility_climate_zone_combos(utility, version):
    """Returns all utility-climate zone combinations"""
    utility_cz = get_all_valid_utility_climate_zone_combinations(version, utility)
    click.echo(
        json.dumps(
            utility_cz.groupby("utility")["climate_zone"]
            .agg(lambda x: ", ".join(x))
            .to_dict(),
            indent=2,
        )
    )


@cli.command()
@click.option(
    "-v",
    "--version",
    default="2020",
    show_default=True,
    help="What version of the avoided costs data to use in the analysis",
)
def valid_deer_load_shapes(version):
    """Returns all valid DEER load shapes"""
    click.echo("\n".join(get_all_valid_deer_load_shapes(version)))

@cli.command()
@click.option(
    "--user-inputs-filepath",
    required=True,
    help="Filepath to the user-inputs CSV file that is used to calculate results",
)
@click.option(
    "--metered-load-shape-filepath",
    help="Optional filepath to the CSV file containing metered load shapes",
)
@click.option(
    "--outputs-electricity-filepath",
    default="outputs_time_series_electricity.csv",
    show_default=True,
    help="Filepath to where the time series electricity data will be saved",
)
@click.option(
    "--outputs-gas-filepath",
    default="outputs_time_series_gas.csv",
    show_default=True,
    help="Filepath to where the time series gas data will be saved",
)
@click.option(
    "-v",
    "--version",
    default="2020",
    show_default=True,
    help="What version of the avoided costs data to use in the analysis",
)
def get_time_series_results(
    user_inputs_filepath,
    metered_load_shape_filepath,
    outputs_electricity_filepath,
    outputs_gas_filepath,
    version,
):
    """ Return raw time series electricity and gas benefits data. """
    metered_load_shape = (
        pd.read_csv(metered_load_shape_filepath, index_col="hour_of_year")
        if metered_load_shape_filepath
        else None
    )
    user_inputs = pd.read_csv(user_inputs_filepath)
    flexvalue_run = FlexValueRun(
        metered_load_shape=metered_load_shape, database_version=version
    )

    outputs = flexvalue_run.get_time_series_results(
        user_inputs
    )
    if os.path.exists(outputs_electricity_filepath):
        os.remove(outputs_electricity_filepath)
    if os.path.exists(outputs_gas_filepath):
        os.remove(outputs_gas_filepath)
    
    for row in outputs:
        
        (outputs_electricity, outputs_gas) = row 
        outputs_electricity.to_csv(outputs_electricity_filepath, mode='a', header= not os.path.exists(outputs_electricity_filepath), index=False)
        outputs_gas.to_csv(outputs_gas_filepath, mode='a', header= not os.path.exists(outputs_gas_filepath), index=False)

