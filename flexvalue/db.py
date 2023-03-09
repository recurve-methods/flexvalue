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
import sys
import csv
import math
import logging
import calendar
from datetime import datetime, timedelta

from jinja2 import Environment, PackageLoader, select_autoescape
from sqlalchemy import create_engine, text, inspect
import psycopg
from .settings import ACC_COMPONENTS_ELECTRICITY, ACC_COMPONENTS_GAS, database_location
from .config import FLEXValueConfig

SUPPORTED_DBS = ("postgres", "sqlite")  # 'bigquery')

__all__ = (
    "get_db_connection",
    "get_deer_load_shape",
    "get_filtered_acc_elec",
    "get_filtered_acc_gas",
)

PROJECT_INFO_FIELDS = [
    "project_id",
    "state",
    "utility",
    "region",
    "mwh_savings",
    "therms_savings",
    "elec_load_shape",
    "therms_profile",
    "start_year",
    "start_quarter",
    "units",
    "eul",
    "ntg",
    "discount_rate",
    "admin_cost",
    "measure_cost",
    "incentive_cost",
]
ELEC_AV_COSTS_FIELDS = [
    "utility",
    "region",
    "year",
    "hour_of_year",
    "total",
    "marginal_ghg",
]
GAS_AV_COSTS_FIELDS = [
    "state",
    "utility",
    "region",
    "year",
    "quarter",
    "month",
    "market",
    "t_d",
    "environment",
    "btm_methane",
    "total",
    "upstream_methane",
    "marginal_ghg",
]
ELEC_AVOIDED_COSTS_FIELDS = [
    "state",
    "utility",
    "region",
    "datetime",
    "year",
    "quarter",
    "month",
    "hour_of_day",
    "hour_of_year",
    "energy",
    "losses",
    "ancillary_services",
    "capacity",
    "transmission",
    "distribution",
    "cap_and_trade",
    "ghg_adder",
    "ghg_rebalancing",
    "methane_leakage",
    "total",
    "marginal_ghg",
    "ghg_adder_rebalancing",
]

logging.basicConfig(stream=sys.stderr, format="%(levelname)s:%(message)s", level=logging.DEBUG)

# This is the number of bytes to read when determining whether a csv file has
# a header. 4096 was determined empirically; I don't recommend reading fewer
# bytes than this, since some files can have many columns.
HEADER_READ_SIZE = 4096

# The number of rows to read from csv files when chunking
INSERT_ROW_COUNT = 100000


class DBManager:
    def __init__(self, fv_config: FLEXValueConfig) -> None:
        self.template_env = Environment(
            loader=PackageLoader("flexvalue"), autoescape=select_autoescape()
        )
        self.config = fv_config
        self.engine = self._get_db_engine(fv_config)

    def _get_db_connection_string(self, config: FLEXValueConfig) -> str:
        """Get the sqlalchemy db connection string for the given settings."""
        database_type = config.database_type
        # TODO: add support for BigQuery via https://github.com/googleapis/python-bigquery-sqlalchemy
        if database_type not in SUPPORTED_DBS:
            raise ValueError(
                f"Unknown database type '{database_type}' in database config file. Please choose one of {','.join(SUPPORTED_DBS)}"
            )
        if database_type == "postgres":
            user = config.user
            password = config.password
            host = config.host
            port = config.port
            database = config.database
            conn_str = (
                f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"
            )
        elif database_type == "sqlite":
            database = config.database
            conn_str = f"sqlite+pysqlite://{database}"
        logging.debug(f'returning connection string "{conn_str}"')
        return conn_str

    def _get_db_engine(
        self, config: FLEXValueConfig
    ):  # TODO: get this type hint to work -> sqlalchemy.engine.Engine:
        if config.use_specified_db():
            logging.debug("using specified db")
            conn_str = self._get_db_connection_string(config)
        else:
            logging.debug("using default db connection")
            conn_str = self._get_default_db_conn_str()
        engine = create_engine(conn_str)
        logging.debug(f"dialect = {engine.dialect.name}")
        return engine

    def _get_default_db_conn_str(self) -> str:
        """If no db config file is provided, default to a local sqlite database."""
        return "sqlite+pysqlite:///flexvalue.db"

    def _file_to_string(self, filename):
        ret = None
        with open(filename) as f:
            ret = f.read()
        return ret

    def reset_elec_load_shape(self):
        logging.debug("Resetting elec load shape")
        self._reset_table("elec_load_shape")

    def reset_elec_av_costs(self):
        logging.debug("Resetting elec_av_costs")
        self._reset_table("elec_av_costs")

    def reset_therms_profiles(self):
        logging.debug("Resetting therms_profile")
        self._reset_table("therms_profile")

    def reset_gas_av_costs(self):
        logging.debug("Resetting gas avoided costs")
        self._reset_table("gas_av_costs")

    def _reset_table(self, table_name):
        # sqlite doesn't support TRUNCATE
        truncate_prefix = (
            f"DELETE FROM"
            if self.engine.dialect.name == "sqlite"
            else f"TRUNCATE TABLE"
        )
        sql = f"{truncate_prefix} {table_name}"
        with self.engine.begin() as conn:
            result = conn.execute(text(sql))

    def _prepare_table(
        self,
        table_name: str,
        sql_filepath: str,
        index_filepaths=[],
        truncate: bool = False,
    ):
        # if the table doesn't exist, create it and all related indexes
        inspection = inspect(self.engine)
        table_exists = inspection.has_table(table_name)
        with self.engine.begin() as conn:
            if not table_exists:
                sql = self._file_to_string(sql_filepath)
                _ = conn.execute(text(sql))
            for index_filepath in index_filepaths:
                sql = self._file_to_string(index_filepath)
                _ = conn.execute(text(sql))
        if truncate:
            self._reset_table(table_name)

    def _load_discount_table(self, project_dicts):
        """project_dicts is a list of dicts. Each dict represents a different
        project from the project info file. The keys are the names of the
        columns in the file/project_info table.
        """
        self._prepare_table(
            "discount",
            "flexvalue/sql/create_discount.sql",
            # index_filepaths=["flexvalue/sql/discount_index.sql"],
            truncate=True,
        )
        discount_dicts = []
        for project_dict in project_dicts:
            start_date = datetime.strptime(project_dict["start_date"], "%Y-%m-%d")
            end_date = datetime.strptime(project_dict["end_date"], "%Y-%m-%d")
            discount_rate = float(project_dict["discount_rate"])
            start_year = int(project_dict["start_year"])
            start_quarter = int(project_dict["start_quarter"])

            start_timestamp = datetime(
                start_date.year,
                start_date.month,
                start_date.day,
                0,
                0,
                0,
            )
            end_timestamp = datetime(
                end_date.year,
                end_date.month,
                end_date.day,
                23,
                0,
                0,
            )

            cur_timestamp = start_timestamp

            while cur_timestamp <= end_timestamp:
                # TODO double-check that this is correct, starting at 1st quarter
                discount = 1.0 / pow(
                    (1.0 + discount_rate / 4.0),
                    ((cur_timestamp.year - start_year) * 4) + (start_quarter - 1),
                )
                discount_dicts.append(
                    {
                        "project_id": project_dict["project_id"],
                        "timestamp": cur_timestamp,
                        "date": cur_timestamp.date(),
                        "discount": discount,
                    }
                )
                cur_timestamp = cur_timestamp + timedelta(hours=1)

        insert_text = self._file_to_string("flexvalue/templates/load_discount.sql")
        with self.engine.begin() as conn:
            conn.execute(text(insert_text), discount_dicts)

    def load_project_info_file(self, project_info_path: str):
        self._prepare_table(
            "project_info",
            "flexvalue/sql/create_project_info.sql",
            index_filepaths=[
                "flexvalue/sql/project_info_index.sql",
                "flexvalue/sql/project_info_dates_index.sql",
            ],
            truncate=True,
        )
        dicts = self._csv_file_to_dicts(
            project_info_path,
            fieldnames=PROJECT_INFO_FIELDS,
            fields_to_upper=["elec_load_shape", "state", "region", "utility"],
        )
        for d in dicts:
            start_year = int(d["start_year"])
            eul = int(d["eul"])
            quarter = d["start_quarter"]
            month = self._quarter_to_month(quarter)
            d["start_date"] = f"{start_year}-{month}-01"
            d["end_date"] = f"{start_year + eul}-{month}-01"
            d["util_load_shape"] = d["utility"] + d["elec_load_shape"]

        logging.debug(f"in loading project_info, dicts = {dicts}")
        insert_text = self._file_to_string("flexvalue/templates/load_project_info.sql")
        with self.engine.begin() as conn:
            conn.execute(text(insert_text), dicts)
        self._load_discount_table(dicts)
        from datetime import datetime

        logging.debug(f"About to start calculation, it is {datetime.now()}")
        self._perform_calculation()
        logging.debug(f"after calc, it is {datetime.now()}")

    def _quarter_to_month(self, qtr):
        quarter = int(qtr)
        return "{:02d}".format(((quarter - 1) * 3) + 1)

    def _get_empty_tables(self):
        empty_tables = []
        inspection = inspect(self.engine)
        with self.engine.begin() as conn:
            for table_name in [
                "therms_profile",
                "project_info",
                "elec_av_costs",
                "gas_av_costs",
                "elec_load_shape",
            ]:
                if not inspection.has_table(table_name):
                    empty_tables.append(table_name)
                    continue
                sql = f"SELECT COUNT(*) FROM {table_name}"
                result = conn.execute(text(sql))
                first = result.first()
                if first[0] == 0:
                    empty_tables.append(table_name)
        return empty_tables

    def _perform_calculation(self):
        from datetime import datetime

        logging.debug(f"before empty tables it is {datetime.now()}")
        empty_tables = self._get_empty_tables()
        logging.debug(f"after empty tables it is {datetime.now()}")
        if empty_tables:
            # TODO the table names are implementation-dependent, let's see if we can give a better error message here
            raise ValueError(
                f"Not all data has been loaded. Please provide data for the following tables: {', '.join(empty_tables)}"
            )
        sql = self._get_calculation_sql()
        with self.engine.begin() as conn:
            logging.debug(f"before calc sql, it is {datetime.now()}")
            result = conn.execute(text(sql))
            logging.debug(f"after calc sql, it is {datetime.now()}")
            print(", ".join(result.keys()))
            for row in result:
                print(", ".join([str(col) for col in row]))

    def _get_calculation_sql(self):
        context = {}
        template = self.template_env.get_template("calculation.sql")
        sql = template.render(context)
        return sql

    def _csv_file_to_dicts(
        self, csv_file_path: str, fieldnames: str, fields_to_upper=None
    ):
        """Returns a dictionary representing the data in the csv file pointed
        to at csv_file_path.
        fields_to_upper is a list of strings. The strings in this list must
        be present in the header row of the csv file being read, and are
        capitalized (with string.upper()) before returning the dict."""
        dicts = []
        with open(csv_file_path, newline="") as f:
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

    def _csv_file_to_rows(self, csv_file_path: str):
        """Reads a csv file into memory and returns a list of tuples representing
        the data. If no header row is present, it raises a ValueError."""
        rows = []
        with open(csv_file_path, newline="") as f:
            has_header = csv.Sniffer().has_header(f.read(HEADER_READ_SIZE))
            if not has_header:
                raise ValueError(
                    f"The file you provided, {csv_file_path}, \
                                 doesn't seem to have a header row. Please provide a header row \
                                 containing the column names."
                )
            f.seek(0)
            csv_reader = csv.reader(f)
            rows = []
            # Note that we're reading the whole file into memory - don't use this on big files.
            for row in csv_reader:
                rows.append(row)
        return rows

    def _pg_connect(self):
        connection = psycopg.connect(
            dbname=self.config.database,
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.password
        )
        logging.debug(f"connection = {connection}")
        return connection

    def _get_timestamp_range(self):
        min_ts_query = "SELECT MIN(timestamp) FROM elec_av_costs;"
        max_ts_query = "SELECT MAX(timestamp) FROM elec_av_costs;"
        min_ts = None
        max_ts = None
        with self.engine.begin() as conn:
            result = conn.execute(text(min_ts_query))
            min_ts = result.scalar()
            result = conn.execute(text(max_ts_query))
            max_ts = result.scalar()
        return (min_ts, max_ts)


    def _postgres_load_elec_load_shapes(self, elec_load_shapes_path: str):
        def copy_write(cur, rows):
            with cur.copy(
                "COPY elec_load_shape (timestamp, state, utility, util_load_shape, region, quarter, month, hour_of_day, hour_of_year, load_shape_name, value) FROM STDIN"
            ) as copy:
                for row in rows:
                    copy.write_row(row)

        inspection = inspect(self.engine)
        table_exists = inspection.has_table('elec_av_costs')
        if not table_exists:
            raise ValueError("You must load the electric avoided costs data before you load the electric load shape data")

        min_ts, max_ts = self._get_timestamp_range()
        logging.debug(f'min_ts = {min_ts}, max_ts = {max_ts}')
        # try:
        conn = self._pg_connect()
        cur = conn.cursor()
        # if you're concerned about RAM change this to sane number
        MAX_ROWS = sys.maxsize

        buf = []
        min_year = min_ts.year
        year_span = max_ts.year - min_ts.year
        logging.debug(f"year_span = {year_span}")
        with open(elec_load_shapes_path) as f:
            # this probably escapes fine but a csv reader is a safer bet
            columns = f.readline().split(",")
            load_shape_names = [
                c.strip()
                for c in columns
                if columns.index(c) > columns.index("hour_of_year")
            ]

            f.seek(0)
            reader = csv.DictReader(f)
            for r in reader:
                for eul_year in range(year_span):
                    year = min_year + eul_year
                    hour_of_year = int(r["hour_of_year"])
                    eac_timestamp = datetime(year, 1, 1) + timedelta(
                        hours=hour_of_year
                    )

                    # If the year is a leap year, move forward a day to avoid Feb 29
                    if calendar.isleap(year) and eac_timestamp >= datetime(
                        year, 2, 29
                    ):
                        eac_timestamp = eac_timestamp + timedelta(hours=24)

                    for load_shape in load_shape_names:
                        buf.append(
                            (
                                eac_timestamp,
                                r["state"].upper(),
                                r["utility"].upper(),
                                r["region"].upper(),
                                int(r["quarter"]),
                                int(r["month"]),
                                int(r["hour_of_day"]),
                                hour_of_year,
                                load_shape.upper(),
                                float(r[load_shape]),
                            )
                        )
                if len(buf) >= MAX_ROWS:
                    copy_write(cur, buf)
                    buf = []
            else:
                copy_write(cur, buf)
        conn.commit()
        conn.close()
        # except Exception as e:
        #     logging.error(f"Exception loading the electric load shapes: {e}")

    def load_elec_load_shapes_file(self, elec_load_shapes_path: str, truncate=False):
        """Load the hourly electric load shapes (csv) file. The first 7 columns
        are fixed. Then there are a variable number of columns, one for each
        load shape. This function parses that file to construct a SQL INSERT
        statement with the data, then inserts the data into the elec_load_shape
        table.
        """
        self._prepare_table(
            "elec_load_shape",
            "flexvalue/sql/create_elec_load_shape.sql",
            # index_filepaths=["flexvalue/sql/elec_load_shape_index.sql"],
            truncate=truncate,
        )
        if self.engine.dialect.name == "postgresql":
            self._postgres_load_elec_load_shapes(elec_load_shapes_path)
        else:
            rows = self._csv_file_to_rows(elec_load_shapes_path)
            # TODO can we clean this up?
            num_columns = len(rows[0])
            buffer = []
            for col in range(7, num_columns):
                for row in range(1, len(rows)):
                    buffer.append(
                        {
                            "state": rows[row][0].upper(),
                            "utility": rows[row][1].upper(),
                            "region": rows[row][2].upper(),
                            "quarter": rows[row][3],
                            "month": rows[row][4],
                            "hour_of_day": rows[row][5],
                            "hour_of_year": rows[row][6],
                            "load_shape_name": rows[0][col].upper(),
                            "value": rows[row][col],
                            "hoy_util_st": rows[row][6]
                            + rows[row][1].upper()
                            + rows[row][0].upper(),
                        }
                    )
            insert_text = self._file_to_string(
                "flexvalue/templates/load_elec_load_shape.sql"
            )
            with self.engine.begin() as conn:
                conn.execute(text(insert_text), buffer)

    def load_therms_profiles_file(
        self, therms_profiles_path: str, truncate: bool = False
    ):
        """Loads the therms profiles csv file. This file has 5 fixed columns and then
        a variable number of columns after that, each of which represents a therms
        profile. This method parses that file to construct a SQL INSERT statement, then
        inserts the data into the therms_profile table."""
        self._prepare_table(
            "therms_profile",
            "flexvalue/sql/create_therms_profile.sql",
            truncate=truncate,
        )
        rows = self._csv_file_to_rows(therms_profiles_path)
        num_columns = len(rows[0])
        buffer = []
        for col in range(5, num_columns):
            for row in range(1, len(rows)):
                buffer.append(
                    {
                        "state": rows[row][0],
                        "utility": rows[row][1],
                        "region": rows[row][2],
                        "quarter": rows[row][3],
                        "month": rows[row][4],
                        "profile_name": rows[0][col],
                        "value": rows[row][col],
                    }
                )
        insert_text = self._file_to_string(
            "flexvalue/templates/load_therms_profiles.sql"
        )
        with self.engine.begin() as conn:
            conn.execute(text(insert_text), buffer)

    def _load_csv_file(
        self,
        csv_file_path: str,
        table_name: str,
        fieldnames,
        load_sql_file_path: str,
        dict_processor=None,
    ):
        """Loads the table_name table, Since some of the input data can be over a gibibyte,
        the load reads in chunks of data and inserts them sequentially. The chunk size is
        determined by INSERT_ROW_COUNT in this file.
        fieldnames is the list of expected values in the header row of the csv file being read.
        dict_processor is a function that takes a single dictionary and returns a single dictionary
        """
        with open(csv_file_path, newline="") as f:
            has_header = csv.Sniffer().has_header(f.read(HEADER_READ_SIZE))
            f.seek(0)
            csv_reader = csv.DictReader(f, fieldnames=fieldnames)
            if has_header:
                next(csv_reader)
            buffer = []
            rownum = 0
            insert_text = self._file_to_string(load_sql_file_path)
            with self.engine.begin() as conn:
                for row in csv_reader:
                    buffer.append(dict_processor(row) if dict_processor else row)
                    rownum += 1
                    if rownum == INSERT_ROW_COUNT:
                        conn.execute(text(insert_text), buffer)
                        buffer = []
                        rownum = 0
                else:  # this is for/else
                    conn.execute(text(insert_text), buffer)

    def _postgres_load_elec_av_costs(self, elec_av_costs_path):
        def copy_write(cur, rows):
            with cur.copy(
                """COPY elec_av_costs (
                    state,
                    utility,
                    region,
                    timestamp,
                    year,
                    quarter,
                    month,
                    hour_of_day,
                    hour_of_year,
                    energy,
                    losses,
                    ancillary_services,
                    capacity,
                    transmission,
                    distribution,
                    cap_and_trade,
                    ghg_adder,
                    ghg_rebalancing,
                    methane_leakage,
                    total,
                    marginal_ghg,
                    ghg_adder_rebalancing)
                    FROM STDIN"""
            ) as copy:
                for row in rows:
                    copy.write_row(row)

        logging.debug("in pg version of load_elec_av_costs")
        # if you're concerned about RAM change this to sane number
        MAX_ROWS = sys.maxsize

        try:
            conn = self._pg_connect()
            cur = conn.cursor()
            buf = []
            with open(elec_av_costs_path) as f:
                reader = csv.DictReader(f)
                for i, r in enumerate(reader):
                    eac_timestamp = datetime.strptime(
                        r["datetime"], "%Y-%m-%d %H:%M:%S %Z"
                    )
                    buf.append(
                        [
                            r["state"],
                            r["utility"],
                            r["region"],
                            eac_timestamp,
                            r["year"],
                            r["quarter"],
                            r["month"],
                            r["hour_of_day"],
                            r["hour_of_year"],
                            r["energy"],
                            r["losses"],
                            r["ancillary_services"],
                            r["capacity"],
                            r["transmission"],
                            r["distribution"],
                            r["cap_and_trade"],
                            r["ghg_adder"],
                            r["ghg_rebalancing"],
                            r["methane_leakage"],
                            float(r["total"]),
                            r["marginal_ghg"],
                            r["ghg_adder_rebalancing"],
                        ]
                    )
                    if len(buf) == MAX_ROWS:
                        copy_write(cur, buf)
                        buf = []
                else:
                    copy_write(cur, buf)
            conn.commit()
        except Exception as e:
            logging.error(f"Error loading the electric avoided costs: {e}")

    def load_elec_avoided_costs_file(self, elec_av_costs_path: str, truncate=False):
        self._prepare_table(
            "elec_av_costs",
            "flexvalue/sql/create_elec_av_cost.sql",
            #index_filepaths=["flexvalue/sql/elec_av_costs_index.sql"],
            truncate=truncate,
        )
        logging.debug("about to load elec av costs")
        if self.engine.dialect.name == "postgresql":
            self._postgres_load_elec_av_costs(elec_av_costs_path)
        else:
            self._load_csv_file(
                elec_av_costs_path,
                "elec_av_costs",
                ELEC_AVOIDED_COSTS_FIELDS,
                "flexvalue/templates/load_elec_av_costs.sql",
                dict_processor=self._eac_dict_mapper,
            )

    def _eac_dict_mapper(self, dict_to_process):
        dict_to_process["date_str"] = dict_to_process["datetime"][
            :10
        ]  # just the 'yyyy-mm-dd'
        dict_to_process["hoy_util_st"] = (
            dict_to_process["hour_of_year"]
            + dict_to_process["utility"]
            + dict_to_process["state"]
        )
        return dict_to_process

    def load_gas_avoided_costs_file(self, gas_av_costs_path: str, truncate=False):
        self._prepare_table(
            "gas_av_costs", "flexvalue/sql/create_gas_av_cost.sql", truncate=truncate
        )
        self._load_csv_file(
            gas_av_costs_path,
            "gas_av_costs",
            GAS_AV_COSTS_FIELDS,
            "flexvalue/templates/load_gas_av_costs.sql",
        )

    def _exec_select_sql(self, sql: str):
        """Returns a list of tuples that have been copied from the sqlalchemy result."""
        # This is just here to support testing
        ret = None
        with self.engine.begin() as conn:
            result = conn.execute(text(sql))
            ret = [x for x in result]
        return ret
