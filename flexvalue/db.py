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
import pandas as pd
from sqlalchemy import create_engine

from .settings import ACC_COMPONENTS_ELECTRICITY, ACC_COMPONENTS_GAS, database_location

__all__ = (
    "get_db_connection",
    "get_deer_load_shape",
    "get_filtered_acc_elec",
    "get_filtered_acc_gas",
)


def get_db_connection(database_version="2020"):
    """Get the db connection for a given version

    Parameters
    ----------
    database_version: str
        The version corresponding to the database that contains the avoided costs data.
        Requires that version's database to have already been downloaded
        using the `flexvalue downloaded-avoided-costs-data-db --version 20XX` command.

    Returns
    -------
    sqlalchemy.engine.Engine
    """
    full_db_path = os.path.join(database_location(), f"{database_version}.db")
    if not os.path.exists(full_db_path):
        raise ValueError(f"Can not find SQLite file at this path: {full_db_path}")
    database_url = f"sqlite:///{full_db_path}"
    return create_engine(database_url)


def execute_query(database_version, query):
    """Execute arbitrary query on the avoided costs db

    Parameters
    ----------
    database_version: str
        The version corresponding to the database that contains the avoided costs data.
        Requires that version's database to have already been downloaded
        using the `flexvalue downloaded-avoided-costs-data-db --version 20XX` command.

    Returns
    -------
    pd.DataFrame
    """
    con = get_db_connection(database_version=database_version)
    return pd.read_sql(query, con=con).drop("local_pkid_", axis=1)


def get_deer_load_shape(database_version):
    """Returns all of the deer load shape 8760 load profiles

    Parameters
    ----------
    database_version: str
        The version corresponding to the database that contains the avoided costs data.
        Requires that version's database to have already been downloaded
        using the `flexvalue downloaded-avoided-costs-data-db --version 20XX` command.

    Returns
    -------
    pd.DataFrame
    """
    con = get_db_connection(database_version=database_version)
    return pd.read_sql_table("deer_load_shapes", con=con).set_index("hour_of_year")


def get_filtered_acc_elec(
    database_version, utility, climate_zone, start_year, end_year
):
    """Returns the electricity avoided costs data

    Parameters
    ----------
    database_version: str
        The version corresponding to the database that contains the avoided costs data.
        Requires that version's database to have already been downloaded
        using the `flexvalue downloaded-avoided-costs-data-db --version 20XX` command.
    utility: str
        Which uility to filter by when loading avoided costs data
    climate_zone: str
        Which climate zone to filter by when loading avoided costs data
    start_year: int
        Which year to start the filter of avoided costs data
    end_year: int
        Which year to end the filter of avoided costs data

    Returns
    -------
    pd.DataFrame
    """
    columns = [
        "year",
        "month",
        "hour_of_day",
        "hour_of_year",
        *ACC_COMPONENTS_ELECTRICITY,
        "marginal_ghg",
    ]
    columns_str = ", ".join(columns)
    climate_zone = (
        climate_zone if climate_zone.startswith("CZ") else f"CZ{climate_zone}"
    )
    sql_str = f""" 
        SELECT * 
        FROM acc_electricity
        WHERE utility = '{utility}'
        AND climate_zone = '{climate_zone}'
        AND year >= {start_year}
        AND year <= {end_year}
    """
    con = get_db_connection(database_version=database_version)
    df = pd.read_sql(sql_str, con=con)
    if df.empty:
        raise ValueError(
            "Can not find avoided costs for\n:"
            f"utility:{utility}\nclimate_zone:{climate_zone}\nstart_year:{start_year}\n"
            f"end_year:{end_year}"
        )
    return df


def get_filtered_acc_gas(database_version, start_year, end_year):
    """Returns gas avoided costs data

    Parameters
    ----------
    database_version: str
        The version corresponding to the database that contains the avoided costs data.
        Requires that version's database to have already been downloaded
        using the `flexvalue downloaded-avoided-costs-data-db --version 20XX` command.
    start_year: int
        Which year to start the filter of avoided costs data
    end_year: int
        Which year to end the filter of avoided costs data

    Returns
    -------
    pd.DataFrame
    """
    columns = ["year", "month", *ACC_COMPONENTS_GAS]
    columns_str = ", ".join(columns)
    sql_str = f""" 
        SELECT * 
        FROM acc_gas
        WHERE year >= {start_year}
        AND year <= {end_year}
    """
    con = get_db_connection(database_version=database_version)
    return pd.read_sql(sql_str, con=con)


def get_all_valid_utility_climate_zone_combinations(database_version, utility=None):
    """Returns all utility-climate zone combinations

    Parameters
    ----------
    database_version: str
        The version corresponding to the database that contains the avoided costs data.
        Requires that version's database to have already been downloaded
        using the `flexvalue downloaded-avoided-costs-data-db --version 20XX` command.
    utility: str
        (optional) Which uility to filter by when loading avoided costs data

    Returns
    -------
    pd.DataFrame
    """
    where_str = f"WHERE utility = '{utility}'" if utility else ""
    query = f"""
    SELECT * 
    FROM acc_electricity_utilities_climate_zones
    {where_str}
    """
    return execute_query(database_version, query)


def get_all_valid_deer_load_shapes(database_version):
    """Returns all valid DEER load shapes

    Parameters
    ----------
    database_version: str
        The version corresponding to the database that contains the avoided costs data.
        Requires that version's database to have already been downloaded
        using the `flexvalue downloaded-avoided-costs-data-db --version 20XX` command.

    Returns
    -------
    list
    """
    query = """
        SELECT *
        FROM deer_load_shapes
        limit 1
        """
    valid_deer_load_shapes = execute_query(database_version, query)
    all_columns_w_utilities = list(
        valid_deer_load_shapes.drop("hour_of_year", axis=1).columns
    )
    # TODO (ssuffian): Reshape db so it is a query by utility column
    return list(
        set(
            [
                c.replace("PGE_", "")
                .replace("SDGE_", "")
                .replace("SCG_", "")
                .replace("SCE_", "")
                for c in all_columns_w_utilities
            ]
        )
    )
