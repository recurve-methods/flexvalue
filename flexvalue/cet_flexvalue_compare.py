import os
import shutil
from zipfile import ZipFile
import glob
import re
import pandas as pd
from pathlib import Path
from tempfile import TemporaryDirectory


DEER_NON_RES = [
    "DEER:HVAC_Chillers",
    "DEER:HVAC_Split-Package_AC",
    "DEER:HVAC_Split-Package_HP",
    "DEER:Indoor_Non-CFL_Ltg",
]


def generate_cet_input_id(program_admin, program_year, identifier):
    return f"{program_admin}-{program_year}-{identifier}"


def _get_flexvalue_load_shape_name(deer_load_shape, sector):
    flexvalue_sector_map = {"Non_Res": "NONRES", "Res": "RES"}
    load_shape_suffix = deer_load_shape.upper().replace("DEER:", "").replace("-", "_")
    load_shape_prefix = flexvalue_sector_map[sector]
    return f"{load_shape_prefix}_{load_shape_suffix}"


class CET_Scan:
    def __init__(
        self,
        program_year,
        acc_version,
        program_admin,
        climate_zone,
        mwh,
        therms,
        units,
        ntg,
        eul,
        sector,
        deer_load_shape,
        gas_sector,
        gas_savings_profile,
        admin_cost,
        measure_cost,
        incentive,
        directory=".",
        scan_name="Test",
    ):
        self.directory = directory
        self.scan_name = scan_name
        self.path = os.path.join(directory, scan_name)

        self.cet_path = os.path.join(self.path, "cet")
        Path(self.cet_path).mkdir(parents=True, exist_ok=True)
        self.cet_zip_path = os.path.join(self.cet_path, f"{self.scan_name}.zip")

        self.flexvalue_path = os.path.join(self.path, "flexvalue")
        Path(self.flexvalue_path).mkdir(parents=True, exist_ok=True)

        self.program_year = program_year
        self.acc_version = acc_version
        self.program_admin = program_admin
        self.climate_zone = climate_zone
        self.units = units
        self.ntg = ntg
        self.eul = eul
        self.sector = sector
        self.deer_load_shape = deer_load_shape
        self.gas_sector = gas_sector
        self.gas_savings_profile = gas_savings_profile
        self.admin_cost = admin_cost
        self.measure_cost = measure_cost
        self.incentive = incentive

        self.index = [110] + list(111 + i for i in range(len(mwh) - 1))
        self.kwh = [m * u * 1000 for m, u in zip(list(i for i in mwh), units)]
        self.mwh = [m * u for m, u in zip(list(i for i in mwh), units)]
        self.therms = [t * u for t, u in zip(list(i for i in therms), units)]

    def generate_cet_input_file(self):

        # Create Folders and Path
        Path(self.path).mkdir(parents=True, exist_ok=True)

        # Create ProgramCost.csv file for CET and write columns
        fname_costs = "ProgramCost.csv"

        # Create Measure.csv file for CET and write columns
        fname_measure = "Measure.csv"

        # Add lines to CET ProgramCost and Measure files, scanning over variable

        def _generate_program_id(program_admin, identifier):
            return f"{program_admin}-{identifier}"

        def _generate_claim_year_quarter(program_year):
            return f"{program_year}Q1"

        for ind in range(len(self.mwh)):
            if self.sector[ind] == "Res" and self.deer_load_shape[ind] in DEER_NON_RES:
                print(
                    f"{self.sector[ind]}/{self.deer_load_shape[ind]}"
                    + " Pairing Not Allowed in CET. Switching to Non_Res"
                )
                self.sector[ind] = "Non_Res"

        cet_program_costs_df = pd.DataFrame(
            [
                {
                    "PrgID": _generate_program_id(self.program_admin, self.index[ind]),
                    "PrgYear": self.program_year,
                    "ClaimYearQuarter": f"{self.program_year}Q1",
                    "AdminCostsOverheadAndGA": self.admin_cost[ind],
                    "AdminCostsOther": 0,
                    "MarketingOutreach": 0,
                    "DIActivity": 0,
                    "DIInstallation": 0,
                    "DIHardwareAndMaterials": 0,
                    "DIRebateAndInspection": 0,
                    "EMV": 0,
                    "UserInputIncentive": 0,
                    "OnBillFinancing": 0,
                    "CostsRecoveredFromOtherSources": 0,
                    "PA": self.program_admin,
                }
                for ind in range(len(self.mwh))
            ]
        )

        cet_measure_costs_df = pd.DataFrame(
            [
                {
                    "CEInputID": generate_cet_input_id(
                        self.program_admin, self.program_year, self.index[ind]
                    ),
                    "PrgID": _generate_program_id(self.program_admin, self.index[ind]),
                    "ClaimYearQuarter": f"{self.program_year}Q1",
                    "Sector": "Commercial",
                    "DeliveryType": "CustIncentDown",
                    "BldgType": "Com",
                    "E3ClimateZone": self.climate_zone[ind],
                    "E3GasSavProfile": self.gas_savings_profile[ind],
                    "E3GasSector": self.gas_sector[ind],
                    "E3MeaElecEndUseShape": self.deer_load_shape[ind],
                    "E3TargetSector": self.sector[ind],
                    "MeasAppType": "AR",
                    "MeasCode": "",
                    "MeasDescription": "NMEC",
                    "MeasImpactType": "Cust-NMEC",
                    "MeasureID": "0",
                    "TechGroup": "",
                    "TechType": "Pilot",
                    "UseCategory": "",
                    "UseSubCategory": "Testing",
                    "PreDesc": "",
                    "StdDesc": "",
                    "SourceDesc": "",
                    "Version": "",
                    "NormUnit": "Each",
                    "NumUnits": 1,
                    "UnitkW1stBaseline": 0,
                    "UnitkWh1stBaseline": self.kwh[ind],
                    "UnitTherm1stBaseline": self.therms[ind],
                    "UnitkW2ndBaseline": 0,
                    "UnitkWh2ndBaseline": 0,
                    "UnitTherm2ndBaseline": 0,
                    "UnitMeaCost1stBaseline": self.measure_cost[ind],
                    "UnitMeaCost2ndBaseline": 0,
                    "UnitDirectInstallLab": 0,
                    "UnitDirectInstallMat": 0,
                    "UnitEndUserRebate": self.incentive[ind],
                    "UnitIncentiveToOthers": 0,
                    "NTG_ID": "NonRes-sAll-NMEC",
                    "NTGRkW": self.ntg[ind],
                    "NTGRkWh": self.ntg[ind],
                    "NTGRTherm": self.ntg[ind],
                    "NTGRCost": self.ntg[ind],
                    "EUL_ID": "",
                    "EUL_Yrs": self.eul[ind],
                    "RUL_ID": "",
                    "RUL_Yrs": 0,
                    "GSIA_ID": "",
                    "RealizationRatekW": 1,
                    "RealizationRatekWh": 1,
                    "RealizationRateTherm": 1,
                    "InstallationRatekW": 1,
                    "InstallationRatekWh": 1,
                    "InstallationRateTherm": 1,
                    "Residential_Flag": 0,
                    "Upstream_Flag": 0,
                    "PA": self.program_admin,
                    "MarketEffectsBenefits": "",
                    "MarketEffectsCosts": "",
                    "RateScheduleElec": "",
                    "RateScheduleGas": "",
                    "CombustionType": "",
                    "MeasInflation": "",
                    "Comments": "",
                }
                for ind in range(len(self.mwh))
            ]
        )

        with TemporaryDirectory() as tmpdirname:
            program_cost_filepath = os.path.join(tmpdirname, "ProgramCost.csv")
            cet_program_costs_df.to_csv(program_cost_filepath, index=False, sep="|")

            measure_filepath = os.path.join(tmpdirname, "Measure.csv")
            cet_measure_costs_df.to_csv(measure_filepath, index=False, sep="|")

            zip_filepath = os.path.join(tmpdirname, "zip_file.zip")
            with ZipFile(zip_filepath, "w") as zip_obj:
                zip_obj.write(measure_filepath, arcname="Measure.csv")
                zip_obj.write(program_cost_filepath, arcname="ProgramCost.csv")
            shutil.move(zip_filepath, self.cet_zip_path)

        print(f"Your CET input file is at {self.cet_zip_path}")

        user_inputs = pd.DataFrame(
            [
                {
                    "ID": self.index[ind],
                    "start_year": self.program_year,
                    "start_quarter": 1,
                    "utility": self.program_admin,
                    "climate_zone": self.climate_zone[ind],
                    "mwh_savings": self.mwh[ind],
                    "load_shape": _get_flexvalue_load_shape_name(
                        self.deer_load_shape[ind], self.sector[ind]
                    ),
                    "therms_savings": self.therms[ind],
                    "therms_profile": self.gas_savings_profile[ind].split(" ")[0],
                    "units": self.units[ind] / self.units[ind],
                    "eul": self.eul[ind],
                    "ntg": self.ntg[ind],
                    "discount_rate": 0.0766,
                    "admin": self.admin_cost[ind],
                    "measure": self.measure_cost[ind],
                    "incentive": self.incentive[ind],
                }
                for ind in range(len(self.mwh))
            ]
        )

        user_inputs_filepath = os.path.join(
            self.flexvalue_path, f"{self.scan_name}_flexvalue_user_inputs.csv"
        )
        user_inputs.to_csv(user_inputs_filepath)

        print(f"Your FLEXvalue input file is at {user_inputs_filepath}")
        return user_inputs.set_index("ID")

    def parse_cet_output(self, cet_output_filepath=None):
        """Parse the CET output that was provided from the calculator

        Parameters
        ----------
        cet_output_filepath: str
            The filepath to the cet output zip file.
            If this is not provided, it searches in the CET folder
            (`{directory}/{scan_name}/cet`) for a zip file that matches
            `{scan_name}*for_cet_ui_run*.zip*`.
        Returns
        -------
        pd.DataFrame
           The output of the CET as a dataframe.
        """

        if cet_output_filepath:
            fname = os.path.basename(cet_output_filepath)
        else:
            glob_search_str = os.path.join(
                self.cet_path, self.scan_name + "*for_cet_ui_run*.zip"
            )
            loc_search = glob.glob(glob_search_str)
            if loc_search:
                loc = loc_search[0]
            else:
                raise ValueError(
                    f"Can not find CET output zip file in {glob_search_str}"
                )
            fname = os.path.basename(loc)
            cet_output_filepath = os.path.join(self.cet_path, fname)

        # Unzip output files
        with ZipFile(cet_output_filepath, "r") as zip_ref:
            zip_ref.extractall(self.cet_path)

        # Extract and print key results to file
        fnum = re.findall(".*cet_ui_run_([0-9]+)", fname)[0]
        output_filepath = os.path.join(self.cet_path, f"{fnum}_outputs.csv")
        return pd.read_csv(output_filepath, delimiter="|")

    def compare_cet_to_flexvalue(self, cet_output_df, flexvalue_output_df):
        flexvalue_output_df["CET_ID"] = flexvalue_output_df.apply(
            lambda x: generate_cet_input_id(x["utility"], x["start_year"], x["ID"]),
            axis=1,
        )
        flexvalue_output_df["source"] = "flexvalue"
        flexvalue_output_df = flexvalue_output_df.reset_index().set_index(
            ["CET_ID", "source"]
        )

        cet_output_df = cet_output_df.set_index("CET_ID").rename(
            columns={
                "ElecBen": "TRC (and PAC) Electric Benefits ($)",
                "GasBen": "TRC (and PAC) Gas Benefits ($)",
                "TRCCost": "TRC Costs ($)",
                "PACCost": "PAC Costs ($)",
                "TRCRatio": "TRC",
                "PACRatio": "PAC",
            }
        )
        cet_output_df["source"] = "CET"

        compare_cols = [
            "TRC (and PAC) Electric Benefits ($)",
            "TRC (and PAC) Gas Benefits ($)",
            "TRC Costs ($)",
            "PAC Costs ($)",
            "TRC",
            "PAC",
        ]
        cet_output_df = cet_output_df.reset_index().set_index(["CET_ID", "source"])
        return (
            pd.concat([flexvalue_output_df[compare_cols], cet_output_df[compare_cols]])
            .sort_index()
            .round(2)
        )

    def get_cleaned_cet_output_df(self, outputs_table, cet_cleaned_results):
        cet_outputs_df = outputs_table.set_index("CET_ID")[
            [
                "climate_zone",
                "mwh_savings",
                "therms_savings",
                "ntg",
                "admin",
                "measure",
                "incentive",
                "load_shape",
            ]
        ]
        cet_outputs_df["Sector"] = self.sector
        cet_outputs_df["PA"] = self.program_admin
        cet_outputs_df["Program Year"] = self.program_year
        cet_outputs_df = cet_outputs_df.join(cet_cleaned_results.set_index("CET_ID"))[
            [
                "Program Year",
                "PA",
                "climate_zone",
                "mwh_savings",
                "therms_savings",
                "ntg",
                "Sector",
                "admin",
                "measure",
                "incentive",
                "load_shape",
                "ElecBen",
                "GasBen",
                "PACCost",
                "TRCCost",
                "PACRatio",
                "TRCRatio",
            ]
        ]
        cet_outputs_df.insert(0, "ACC Version", self.acc_version)
        return cet_outputs_df.rename(
            columns={
                "climate_zone": "Climate Zone",
                "mwh_savings": "MWh Savings",
                "therms_savings": "Therms Savings",
                "ntg": "NTG",
                "admin": "Admin",
                "measure": "Measure",
                "incentive": "Incentive",
                "load_shape": "DEER Load Shape",
                "ElecBen": "TRC (and PAC) Electric Benefits ($)",
                "GasBen": "TRC (and PAC) Gas Benefits ($)",
                "TRCCost": "TRC Costs ($)",
                "PACCost": "PAC Costs ($)",
                "TRCRatio": "TRC",
                "PACRatio": "PAC",
            }
        ).reset_index()
