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


@click.group()
def cli():
    pass


@cli.command()
@click.option("-y", "--year", default="2020", show_default=True)
@click.option(
    "--url-prefix",
    default="https://storage.googleapis.com/flexvalue-public-resources/",
    show_default=True,
)
@click.option("--skip-if-exists/--overwrite-if-exists", default=False)
def download_avoided_costs_data_db(url_prefix, year, skip_if_exists):
    db_filename = f"{year}.db"
    output_filepath = os.path.join(database_location(), db_filename)
    Path(database_location()).mkdir(parents=True, exist_ok=True)

    if skip_if_exists and os.path.exists(output_filepath):
        logging.warning("This file already exists so it will not be downloaded again.")
    else:
        url = os.path.join(url_prefix, db_filename)
        response = requests.get(url)
        open(output_filepath, "wb").write(response.content)


@cli.command()
@click.option("--test-data-filepath", default="test_data", show_default=True)
def generate_example_inputs(test_data_filepath):
    Path(test_data_filepath).mkdir(parents=True, exist_ok=True)
    get_example_user_inputs_deer().to_csv(
        os.path.join(test_data_filepath, "example_user_inputs_deer.csv")
    )
    get_example_user_inputs_metered().to_csv(
        os.path.join(test_data_filepath, "example_user_inputs_metered.csv")
    )
    get_example_metered_load_shape().to_csv(
        os.path.join(test_data_filepath, "example_metered_load_shape.csv")
    )


@cli.command()
@click.option("--user-inputs-filepath", required=True)
@click.option("--metered-load-shape-filepath")
@click.option("--include-report/--exclude-report", default=True, show_default=True)
@click.option("--report-filepath", default="report.html", show_default=True)
@click.option("--outputs-table-filepath", default="outputs_table.csv", show_default=True)
def get_results(
    user_inputs_filepath, metered_load_shape_filepath, include_report, report_filepath
):
    if not include_report:
        metered_load_shape = (
            pd.read_csv(metered_load_shape_filepath, index_col="hour_of_year")
            if metered_load_shape_filepath
            else None
        )
        user_inputs = pd.read_csv(user_inputs_filepath)
        flexvalue_run = FlexValueRun(metered_load_shape=metered_load_shape)
        (
            outputs_table,
            elec_benefits,
            gas_benefits,
        ) = flexvalue_run.get_flexvalue_results(user_inputs)
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
        content = dedent(
            f"""
            import pandas as pd
            from flexvalue import FlexValueRun
            from flexvalue.plots import plot_results
            from IPython.display import display
            metered_load_shape_filepath = {metered_load_shape_filepath}
            user_inputs_filepath = {user_inputs_filepath}
            metered_load_shape = (
                pd.read_csv(metered_load_shape_filepath, index_col="hour_of_year")
                if metered_load_shape_filepath
                else None
            )
            user_inputs = pd.read_csv(user_inputs_filepath)
            flexvalue_run = FlexValueRun(metered_load_shape=metered_load_shape)
            outputs_table, elec_benefits, gas_benefits = flexvalue_run.get_results(
                user_inputs
            )
            display(user_inputs)
            display(outputs_table)
            plot_results(outputs_table, elec_benefits, gas_benefits)
            """
        )
        nbf_nt.add_code_cell(content)
        nbf_nt.execute()
        nbf_nt.to_html(report_filepath)


@cli.command()
@click.option("--utility", default=None, show_default=True)
@click.option("-y", "--year", default="2020", show_default=True)
def valid_utility_climate_zone_combos(utility, year):
    """Returns all utility-climate zone combinations"""
    utility_cz = get_all_valid_utility_climate_zone_combinations(year, utility)
    click.echo(
        json.dumps(
            utility_cz.groupby("utility")["climate_zone"]
            .agg(lambda x: ", ".join(x))
            .to_dict(),
            indent=2,
        )
    )


@cli.command()
@click.option("-y", "--year", default="2020", show_default=True)
def valid_deer_load_shapes(year):
    """Returns all valid DEER load shapes"""
    click.echo("\n".join(get_all_valid_deer_load_shapes(year)))
