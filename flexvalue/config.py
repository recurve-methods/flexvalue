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
    elec_load_shape_file: str = None
    elec_av_costs_file: str = None
    therms_profiles_file: str = None
    gas_av_costs_file: str = None
    project_info_file: str = None
    metered_load_shape_file: str = None
    elec_av_costs_table: str = None
    elec_load_shape_table: str = None
    therms_profiles_table: str = None
    gas_av_costs_table: str = None
    project_info_table: str = None
    metered_load_shape_table: str = None
    output_file: str = None
    output_table: str = None
    electric_output_table: str = None
    gas_output_table: str = None
    aggregation_columns: List[str] = field(default_factory=list)
    process_elec_load_shape: bool = False
    process_elec_av_costs: bool = False
    process_therms_profiles: bool = False
    process_gas_av_costs: bool = False
    process_metered_load_shape: bool = False
    reset_elec_load_shape: bool = False
    reset_elec_av_costs: bool = False
    reset_therms_profiles: bool = False
    reset_gas_av_costs: bool = False
    elec_components: List[str] = field(default_factory=list)
    gas_components: List[str] = field(default_factory=list)
    elec_addl_fields: List[str] = field(default_factory=list)
    gas_addl_fields: List[str] = field(default_factory=list)
    separate_output_tables: bool = False
    use_value_curve_name_for_join: bool = False

    @staticmethod
    def from_file(config_file):
        data = toml.load(config_file)
        db = data.get("database", dict())
        run_info = data.get("run", dict())
        return FLEXValueConfig(
            database_type=db.get("database_type", None),
            host=db.get("host", None),
            port=db.get("port", None),
            user=db.get("user", None),
            password=db.get("password", None),
            database=db.get("database", None),
            project=db.get("project", None),
            elec_av_costs_table=db.get("elec_av_costs_table", None),
            elec_load_shape_table=db.get("elec_load_shape_table", None),
            therms_profiles_table=db.get("therms_profiles_table", None),
            gas_av_costs_table=db.get("gas_av_costs_table", None),
            metered_load_shape_table=db.get("metered_load_shape_table", None),
            project_info_table=db.get("project_info_table", None),
            elec_load_shape_file=run_info.get("elec_load_shape", None),
            elec_av_costs_file=run_info.get("elec_av_costs", None),
            therms_profiles_file=run_info.get("therms_profiles", None),
            gas_av_costs_file=run_info.get("gas_av_costs", None),
            project_info_file=run_info.get("project_info", None),
            metered_load_shape_file=run_info.get("metered_load_shape_file", None),
            output_file=run_info.get("output_file", None),
            electric_output_table=run_info.get("electric_output_table", None),
            gas_output_table=run_info.get("gas_output_table", None),
            aggregation_columns=run_info.get("aggregation_columns", []),
            reset_elec_load_shape=run_info.get("reset_elec_load_shape", None),
            reset_elec_av_costs=run_info.get("reset_elec_av_costs", None),
            reset_gas_av_costs=run_info.get("reset_gas_av_costs", None),
            reset_therms_profiles=run_info.get("reset_therms_profiles", None),
            process_elec_load_shape=run_info.get("process_elec_load_shape", None),
            process_elec_av_costs=run_info.get("process_elec_av_costs", None),
            process_therms_profiles=run_info.get("process_therms_profiles", None),
            process_gas_av_costs=run_info.get("process_gas_av_costs", None),
            process_metered_load_shape=run_info.get("process_metered_load_shape", None),
            elec_components=run_info.get("elec_components", []),
            gas_components=run_info.get("gas_components", []),
            separate_output_tables=run_info.get("separate_output_tables", None),
            elec_addl_fields=run_info.get("elec_addl_fields", []),
            gas_addl_fields=run_info.get("gas_addl_fields", []),
            use_value_curve_name_for_join=run_info.get(
                "use_value_curve_name_for_join", None
            ),
        )

    def validate(self):
        if not self.database_type:
            return
        if self.database_type == "postgresql":
            if not any(
                [
                    self.database_type,
                    self.host,
                    self.port,
                    self.user,
                    self.password,
                    self.database,
                ]
            ):
                raise FLEXValueException(
                    "When using postgresql, you must provide at least of the following values in the config file: host, port, user, password."
                )
        if self.database_type == "bigquery":
            if not all(
                [
                    self.project,
                    self.project_info_table,
                    self.elec_load_shape_table,
                    self.therms_profiles_table,
                    self.elec_av_costs_table,
                    self.gas_av_costs_table,
                ]
            ):
                raise FLEXValueException(
                    "When using bigquery, you must provide all of the following values in the config file: project, project_info_table, elec_load_shape_table, therms_profiles_table, elec_av_costs_table, gas_av_costs_table."
                )
            if self.separate_output_tables == True:
                if not self.electric_output_table or not self.gas_output_table:
                    raise FLEXValueException(
                        "When you specify separate_output_tables, you must specify both electric_output_table and gas_output_table."
                    )

    def float_type(self):
        if self.database_type == "bigquery":
            return "FLOAT64"
        else:
            return "FLOAT"
