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
import json
import csv
import math
from jinja2 import Environment, PackageLoader, select_autoescape
# import pandas as pd
from sqlalchemy import create_engine, text
from .settings import ACC_COMPONENTS_ELECTRICITY, ACC_COMPONENTS_GAS, database_location

SUPPORTED_DBS = ('postgres', 'sqlite')  # 'bigquery')

__all__ = (
    "get_db_connection",
    "get_deer_load_shape",
    "get_filtered_acc_elec",
    "get_filtered_acc_gas",
)

PROJECT_INFO_FIELDS = [
    'project_id', 'state', 'utility', 'region', 'mwh_savings', 'therms_savings',
    'elec_load_shape', 'therms_profile', 'start_year', 'start_quarter', 'units',
    'eul', 'ntg', 'discount_rate', 'admin_cost', 'measure_cost', 'incentive_cost'
]
ELEC_AV_COSTS_FIELDS = [
    'utility', 'region', 'year', 'hour_of_year', 'total', 'marginal_ghg'
]
GAS_AV_COSTS_FIELDS = [
    "state", "utility", "region", "year", "quarter", "month",
    "market", "t_d", "environment", "btm_methane", "total",
    "upstream_methane", "marginal_ghg"
]
ELEC_AVOIDED_COSTS_FIELDS = [
    "state", "utility", "region", "datetime", "year", "quarter", "month",
    "hour_of_day", "hour_of_year", "energy", "losses", "ancillary_services",
    "capacity", "transmission", "distribution", "cap_and_trade", "ghg_adder",
    "ghg_rebalancing", "methane_leakage", "total", "marginal_ghg",
    "ghg_adder_rebalancing"
]


# This is the number of bytes to read when determining whether a csv file has
# a header. 4096 was determined empirically; I don't recommend reading fewer
# bytes than this, since some files can have many columns.
HEADER_READ_SIZE = 4096

# The number of rows to read from csv files when chunking
INSERT_ROW_COUNT = 100000

class DBManager:
    def __init__(self, db_config_path:str) -> None:
        self.template_env = Environment(loader=PackageLoader('flexvalue'), autoescape=select_autoescape())
        self.engine = self._get_db_engine(db_config_path)

    def _get_db_connection_string(self, db_config_path: str) -> str:
        """ Get the sqlalchemy db connection string for the given settings."""
        database_settings = self._get_database_config(db_config_path)
        database = database_settings['database']
        host = database_settings['host']
        port = database_settings['port']
        user = database_settings.get('user', None)
        password = database_settings.get('password', None)

        if database not in SUPPORTED_DBS:
            raise ValueError(f"Unknown database type '{database}' in database config file. Please choose one of {','.join(SUPPORTED_DBS)}")

        # TODO: get these all working
        # TODO: add support for BigQuery via https://github.com/googleapis/python-bigquery-sqlalchemy
        if database == "postgres":
            db = database_settings.get("db", "postgres")
            conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
        elif database == "sqlite":
            db = database_settings.get('db', 'dbfile.db')
            conn_str = f"sqlite+pysqlite://{user}:{password}/{db}"
        return conn_str

    def _get_db_engine(self, db_config_path: str): # TODO: get this type hint to work -> sqlalchemy.engine.Engine:
        conn_str = self._get_db_connection_string(db_config_path) if db_config_path else self._get_default_db_conn_str()
        engine = create_engine(conn_str)
        return engine

    def _get_database_config(self, db_config_path:str) -> dict:
        db_config = {}
        with open(db_config_path) as f:
            config_str = f.read()
            db_config = json.loads(config_str)
        return db_config

    def _get_default_db_conn_str(self) -> str:
        """ If no db config file is provided, default to a local sqlite database."""
        return "sqlite+pysqlite:///flexvalue.db"

    def _file_to_string(self, filename):
        ret = None
        with open(filename) as f:
            ret = f.read()
        return ret

    def _prepare_table(self, table_name: str, sql_filepath:str, index_filepath:str=None, truncate=False):
        # if the table doesn't exist, create it
        table_exists = self.engine.has_table(table_name)
        if not table_exists:
            sql = self._file_to_string(sql_filepath)
            with self.engine.begin() as conn:
                sql_results = conn.execute(text(sql))
        if index_filepath:
            sql = self._file_to_string(index_filepath)
            with self.engine.begin() as conn:
                sql_results = conn.execute(text(sql))
        if truncate:
            truncate_prefix = f'DELETE FROM' if self.engine.dialect.name == 'sqlite' else f'TRUNCATE TABLE'
            # sqlite doesn't support TRUNCATE
            self._exec_delete_sql(f"{truncate_prefix} {table_name}")

    def _load_discount_table(self, project_dicts):
        self._prepare_table(
            'discount',
            'flexvalue/sql/create_discount.sql',
            index_filepath="flexvalue/sql/discount_index.sql",
            truncate=True
        )
        discount_dicts = []
        for project_dict in project_dicts:
            year = int(project_dict['start_year'])
            for quarter in range(4 * int(project_dict['eul'])):
                discount_rate = float(project_dict['discount_rate'])
                discount = 1.0 / math.pow((1.0 + (discount_rate / 4.0)), quarter)
                new_q = ((int(project_dict['start_quarter']) + quarter - 1) % 4) + 1
                if new_q == 1:
                    year += 1
                discount_dicts.append({'project_id': project_dict['project_id'], 'year': year, 'quarter': new_q, 'discount': discount})
        insert_text = self._file_to_string('flexvalue/templates/load_discount.sql')
        with self.engine.begin() as conn:
            conn.execute(text(insert_text), discount_dicts)

    def load_project_info_file(self, project_info_path: str):
        self._prepare_table(
            'project_info',
            'flexvalue/sql/create_project_info.sql',
            index_filepath="flexvalue/sql/project_info_index.sql",
            truncate=True
        )
        dicts = self._csv_file_to_dicts(
            project_info_path, fieldnames=PROJECT_INFO_FIELDS,
            fields_to_upper=['elec_load_shape', 'state', 'region', 'utility']
        )
        insert_text = self._file_to_string('flexvalue/templates/load_project_info.sql')
        with self.engine.begin() as conn:
            conn.execute(text(insert_text), dicts)
        self._load_discount_table(dicts)
        self._perform_calculation()

    def _get_empty_tables(self):
        empty_tables = []
        with self.engine.begin() as conn:
            for table_name in ['therms_profile', 'project_info', 'elec_av_costs', 'gas_av_costs', 'elec_load_shape']:
                if not self.engine.has_table(table_name):
                    empty_tables.append(table_name)
                    continue
                sql = f"SELECT COUNT(*) FROM {table_name}"
                result = conn.execute(text(sql))
                first = result.first()
                if first[0] == 0:
                    empty_tables.append(table_name)
        return empty_tables

    def _perform_calculation(self):
        empty_tables = self._get_empty_tables()
        if empty_tables:
            # TODO the table names are implementation-dependent, let's see if we can give a better error message here
            raise ValueError(f"Not all data has been loaded. Please provide data for the following tables: {', '.join(empty_tables)}")
        sql = self._get_calculation_sql()
        with self.engine.begin() as conn:
            result = conn.execute(text(sql))
            for x in result:
                print(x)

    def _get_calculation_sql(self):
        context = {}
        template = self.template_env.get_template("calculation.sql")
        sql = template.render(context)
        return sql

    def _csv_file_to_dicts(self, csv_file_path: str, fieldnames:str, fields_to_upper=None):
        dicts = []
        with open(csv_file_path, newline='') as f:
            has_header = csv.Sniffer().has_header(f.read(HEADER_READ_SIZE))
            f.seek(0)
            csv_reader = csv.DictReader(f, fieldnames=fieldnames)
            if has_header:
                next(csv_reader)
            for row in csv_reader:
                processed = row
                for field in fields_to_upper:
                    processed[field] = processed[field].upper()
                dicts.append(processed)
        return dicts

    def _csv_file_to_rows(self, csv_file_path):
        """ Reads a csv file into memory and returns a list of tuples representing
        the data. If no header row is present, it raises a ValueError."""
        rows = []
        with open(csv_file_path, newline='') as f:
            has_header = csv.Sniffer().has_header(f.read(HEADER_READ_SIZE))
            if not has_header:
                raise ValueError(f"The file you provided, {csv_file_path}, \
                                 doesn't seem to have a header row. Please provide a header row \
                                 containing the column names.")
            f.seek(0)
            csv_reader = csv.reader(f)
            rows = []
            # Note that we're reading the whole file into memory - don't use this on big files.
            for row in csv_reader:
                rows.append(row)
        return rows

    def load_elec_load_shapes_file(self, elec_load_shapes_path: str, truncate=False):
        """ Load the hourly electric load shapes (csv) file. The first 7 columns
        are fixed. Then there are a variable number of columns, one for each
        load shape. This function parses that file to construct a SQL INSERT
        statement with the data, then inserts the data into the elec_load_shape
        table.
        """
        self._prepare_table(
            'elec_load_shape',
            'flexvalue/sql/create_elec_load_shape.sql',
            index_filepath="flexvalue/sql/elec_load_shape_index.sql",
            truncate=truncate
        )
        rows = self._csv_file_to_rows(elec_load_shapes_path)
        # TODO can we clean this up?
        num_columns = len(rows[0])
        buffer = []
        for col in range(7, num_columns):
            for row in range(1, len(rows)):
                buffer.append({
                    'state': rows[row][0].upper(),
                    'utility': rows[row][1].upper(),
                    'region': rows[row][2].upper(),
                    'quarter': rows[row][3],
                    'month': rows[row][4],
                    'hour_of_day': rows[row][5],
                    'hour_of_year': rows[row][6],
                    'load_shape_name': rows[0][col].upper(),
                    'value': rows[row][col]
                })
        insert_text = self._file_to_string('flexvalue/templates/load_elec_load_shape.sql')
        with self.engine.begin() as conn:
            conn.execute(text(insert_text), buffer)

    def load_therms_profiles_file(self, therms_profiles_path: str, truncate=False):
        """ Loads the therms profiles csv file. This file has 5 fixed columns and then
        a variable number of columns after that, each of which represents a therms
        profile. This method parses that file to construct a SQL INSERT statement, then
        inserts the data into the therms_profile table."""
        self._prepare_table('therms_profile', 'flexvalue/sql/create_therms_profile.sql', truncate=truncate)
        rows = self._csv_file_to_rows(therms_profiles_path)
        num_columns = len(rows[0])
        buffer = []
        for col in range(5, num_columns):
            for row in range(1, len(rows)):
                buffer.append({
                    'state': rows[row][0],
                    'utility': rows[row][1],
                    'region': rows[row][2],
                    'quarter': rows[row][3],
                    'month': rows[row][4],
                    'profile_name': rows[0][col],
                    'value': rows[row][col]
                })
        insert_text = self._file_to_string('flexvalue/templates/load_therms_profiles.sql')
        with self.engine.begin() as conn:
            conn.execute(text(insert_text), buffer)

    def _load_csv_file(self, csv_file_path:str, table_name: str, fieldnames, load_sql_file_path:str):
        """ Loads the table_name table, Since some of the input data can be over a gibibyte,
        the load reads in chunks of data and inserts them sequentially. The chunk size is
        determined by INSERT_ROW_COUNT in this file. """
        # TODO when we support postgres there are other approaches to load csv data much more quickly; use them
        with open(csv_file_path, newline='') as f:
            has_header = csv.Sniffer().has_header(f.read(HEADER_READ_SIZE))
            f.seek(0)
            csv_reader = csv.DictReader(f, fieldnames=fieldnames)
            if has_header:
                next(csv_reader)
            buffer = []
            rownum = 0
            insert_text = self._file_to_string(load_sql_file_path)
            for row in csv_reader:
                buffer.append(row)
                rownum += 1
                if rownum == INSERT_ROW_COUNT:
                    with self.engine.begin() as conn:
                        conn.execute(text(insert_text), buffer)
                    buffer = []
                    rownum = 0
            else:
                with self.engine.begin() as conn:
                    conn.execute(text(insert_text), buffer)

    def load_elec_avoided_costs_file(self, elec_av_costs_path: str, truncate=False):
        self._prepare_table(
            'elec_av_costs',
            'flexvalue/sql/create_elec_av_cost.sql',
            index_filepath="flexvalue/sql/elec_av_costs_index.sql",
            truncate=truncate
        )
        self._load_csv_file(elec_av_costs_path, 'elec_av_costs', ELEC_AVOIDED_COSTS_FIELDS, "flexvalue/templates/load_elec_av_costs.sql")

    def load_gas_avoided_costs_file(self, gas_av_costs_path: str, truncate=False):
        self._prepare_table('gas_av_costs', 'flexvalue/sql/create_gas_av_cost.sql', truncate=truncate)
        self._load_csv_file(gas_av_costs_path, 'gas_av_costs', GAS_AV_COSTS_FIELDS, "flexvalue/templates/load_gas_av_costs.sql")

    def _exec_delete_sql(self, sql):
        with self.engine.begin() as conn:
            result = conn.execute(text(sql))

    def _exec_select_sql(self, sql):
        """ Returns a list of tuples that have been copied from the sqlalchemy result. """
        # This is just here to support testing
        ret = None
        with self.engine.begin() as conn:
            result = conn.execute(text(sql))
            ret = [x for x in result]
        return ret


