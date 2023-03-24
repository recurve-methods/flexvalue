from dataclasses import dataclass
import toml

class FLEXValueException(Exception):
    pass

@dataclass
class FLEXValueConfig:
    database_type: str
    host: str
    port: int
    user: str
    password: str
    database: str
    project: str
    dataset: str
    elec_load_shape: str
    elec_av_costs: str
    therms_profiles: str
    gas_av_costs: str
    project_info: str
    elec_av_cost_table: str
    elec_load_shape_table: str
    therms_profiles_table: str
    gas_av_cost_table: str
    project_info_table: str
    output_file: str
    output_table: str

    @staticmethod
    def from_file(config_file):
        data = toml.load(config_file)
        print(f"config data = {data}")
        db = data.get('database', dict())
        run = data.get('run', dict())
        return FLEXValueConfig(
            database_type=db.get('database_type', None),
            host=db.get('host', None),
            port=db.get('port', None),
            user=db.get('user', None),
            password=db.get('password', None),
            database=db.get('database', None),
            project=db.get('project', None),
            dataset=db.get('dataset', None),
            elec_av_cost_table=db.get("elec_av_cost_table", None),
            elec_load_shape_table=db.get("elec_load_shape_table", None),
            therms_profiles_table=db.get("therms_profiles_table", None),
            gas_av_cost_table=db.get("gas_av_cost_table", None),
            project_info_table=db.get("project_info_table", None),
            elec_load_shape=run.get('elec_load_shape', None),
            elec_av_costs=run.get('elec_av_costs', None),
            therms_profiles=run.get('therms_profiles', None),
            gas_av_costs=run.get('gas_av_costs', None),
            project_info=run.get('project_info', None),
            output_file=run.get('output_file', None),
            output_table=run.get('output_table', None)
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

    def aggregation_columns(self):
        return [
        {
            "prefix": "pcwdea",
            "columns":["hour_of_year", "year", "eul", "utility", "region", "month", "quarter", "discount", "hour_of_day", "timestamp"]
        }, {
            "prefix": "elec_load_shape",
            "columns": ["load_shape_name"]
        }
    ]

