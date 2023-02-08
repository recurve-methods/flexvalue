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
    "--db-config-file",
    help="Filepath to the json file containing the database configuration information."
)
def get_results(
    project_info,
    db_config_file,
):
    print(f"in cli.py, project_info={project_info}, db_config_file={db_config_file}")
    run(db_config_path=db_config_file, project_info=project_info)
