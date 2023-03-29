import toml

from dataclasses import dataclass, field
from typing import List


class FLEXValueException(Exception):
    pass

@dataclass
class FLEXValueConfig:
    database_type: str
    host: str = None
    port: int = None
    user: str = None
    password: str = None
    database: str = None
    project: str = None
    dataset: str = None
    elec_load_shape_file: str = None
    elec_av_costs_file: str = None
    therms_profiles_file: str = None
    gas_av_costs_file: str = None
    project_info_file: str = None
    elec_av_costs_table: str = None
    elec_load_shape_table: str = None
    therms_profiles_table: str = None
    gas_av_costs_table: str = None
    project_info_table: str = None
    output_file: str = None
    output_table: str = None
    aggregation_columns: List[str] = field(default_factory=list)
    process_elec_load_shape: bool = False
    process_elec_av_costs: bool = False
    process_therms_profiles: bool = False
    process_gas_av_costs: bool = False
    reset_elec_load_shape: bool = False
    reset_elec_av_costs: bool = False
    reset_therms_profiles: bool = False
    reset_gas_av_costs: bool = False

    @staticmethod
    def from_file(config_file):
        data = toml.load(config_file)
        print(f"config data = {data}")
        db = data.get('database', dict())
        run_info = data.get('run', dict())
        return FLEXValueConfig(
            database_type=db.get('database_type', None),
            host=db.get('host', None),
            port=db.get('port', None),
            user=db.get('user', None),
            password=db.get('password', None),
            database=db.get('database', None),
            project=db.get('project', None),
            dataset=db.get('dataset', None),
            elec_av_costs_table=db.get("elec_av_costs_table", None),
            elec_load_shape_table=db.get("elec_load_shape_table", None),
            therms_profiles_table=db.get("therms_profiles_table", None),
            gas_av_costs_table=db.get("gas_av_costs_table", None),
            project_info_table=db.get("project_info_table", None),
            elec_load_shape_file=run_info.get('elec_load_shape', None),
            elec_av_costs_file=run_info.get('elec_av_costs', None),
            therms_profiles_file=run_info.get('therms_profiles', None),
            gas_av_costs_file=run_info.get('gas_av_costs', None),
            project_info_file=run_info.get('project_info', None),
            output_file=run_info.get('output_file', None),
            output_table=run_info.get('output_table', None),
            aggregation_columns=run_info.get("aggregation_columns", []),
            reset_elec_load_shape=run_info.get("reset_elec_load_shape", None),
            reset_elec_av_costs=run_info.get("reset_elec_av_costs", None),
            reset_gas_av_costs=run_info.get("reset_gas_av_costs", None),
            reset_therms_profiles=run_info.get("reset_therms_profiles", None)
        )


    def use_specified_db(self):
        """ If the user has specified any of the database fields,
        use this configuration.
        """
        return self.database_type != None and self.database_type != ""

    def validate(self):
        if not self.database_type:
            return
        if self.database_type == "postgresql":
            if not any([self.database_type, self.host, self.port, self.user, self.password, self.database]):
                raise FLEXValueException("When using postgresql, you must provide at least of the following values in the config file: host, port, user, password.")
        if self.database_type == "bigquery":
            if not all([self.project, self.dataset]):
                raise FLEXValueException("When using bigquery, you must provide all of the following values in the config file: project, dataset.")

    def float_type(self):
        if self.database_type == "bigquery":
            return "FLOAT64"
        else:
            return "FLOAT"
