from dataclasses import dataclass
import toml

@dataclass
class FLEXValueConfig:
    database_type: str
    host: str
    port: int
    user: str
    password: str
    database: str
    elec_load_shape: str
    elec_av_costs: str
    therms_profiles: str
    gas_av_costs: str
    project_info: str

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
            elec_load_shape=run.get('elec_load_shape', None),
            elec_av_costs=run.get('elec_av_costs', None),
            therms_profiles=run.get('therms_profiles', None),
            gas_av_costs=run.get('gas_av_costs', None),
            project_info=run.get('project_info', None)
        )

    def use_specified_db(self):
        """ If the user has specified any of the database fields,
        use this configuration.
        """
        return any([self.database_type, self.host, self.port, self.user, self.password, self.database])

