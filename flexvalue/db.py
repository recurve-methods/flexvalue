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

PROJECT_INFO_FIELDS = ['project_id', 'elec_load_shape', 'start_year', 'start_quarter', 'utility', 'region', 'units', 'eul', 'ntg', 'discount_rate', 'admin_cost', 'measure_cost', 'incentive_cost', 'therms_profile', 'therms_savings', 'mwh_savings']


class DBManager:
    def __init__(self, db_config_path:str) -> None:
        self.env = Environment(loader=PackageLoader('flexvalue'), autoescape=select_autoescape())
        self.engine = self._get_db_engine(db_config_path)

    def _get_db_connection_string(self, db_config_path: str) -> str:
        database_settings = self._get_database_config(db_config_path)
        """ Get the sqlalchemy db connection string for the given settings."""
        database = database_settings['database']
        host = database_settings['host']
        port = database_settings['port']
        user = database_settings.get('user', None)
        password = database_settings.get('password', None)

        if database not in SUPPORTED_DBS:
            raise ValueError(f"Unknown database type '{database}' in database config file. Please choose one of {','.join(SUPPORTED_DBS)}")

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
        #print(f"in _get_db_engine, engine = {engine}")
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

    def _csv_file_to_context(self, filepath, fieldnames, context_key):
        context = {context_key: []}
        with open(filepath, newline='') as f:
            has_header = csv.Sniffer().has_header(f.read(1024))
            f.seek(0)
            csv_reader = csv.DictReader(f, fieldnames=fieldnames)
            if has_header:
                next(csv_reader, None)
            for row in csv_reader:
                context['projects'].append(row)
        return context

    def load_project_info_file(self, project_info_path: str):
        # if the table doesn't exist, create it
        table_exists = self.engine.has_table('project_info')
        #print(f"table_exists = {table_exists}")
        if not table_exists:
            sql = self._file_to_string('flexvalue/sql/create_project_info.sql')
            #print(f"in load_project_info, table creation sql = {sql}")
            with self.engine.begin() as conn:
                sql_results = conn.execute(text(sql))
                print(f"sql_results = {sql_results}")
        context = self._csv_file_to_context(project_info_path, PROJECT_INFO_FIELDS, 'projects')

        template = self.env.get_template('load_project_info.sql')
        sql = template.render(context)
        #print(f"rendered sql = {sql}")
        ret = self._execute_sql(sql)
        #print(f"Loading project info returned {ret}")

    def _execute_sql(self, sql):
        result = None
        with self.engine.begin() as conn:
            result = conn.execute(text(sql))
        return result



