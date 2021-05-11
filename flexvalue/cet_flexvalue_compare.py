import os
import shutil
from zipfile import *
import glob
import re
import pandas as pd

cwd = os.getcwd()
programcost_columns = "PrgID|PrgYear|ClaimYearQuarter|AdminCostsOverheadAndGA|AdminCostsOther|MarketingOutreach|DIActivity|DIInstallation|DIHardwareAndMaterials|DIRebateAndInspection|EMV|UserInputIncentive|OnBillFinancing|CostsRecoveredFromOtherSources|PA"
measure_columns = "CEInputID|PrgID|ClaimYearQuarter|Sector|DeliveryType|BldgType|E3ClimateZone|E3GasSavProfile|E3GasSector|E3MeaElecEndUseShape|E3TargetSector|MeasAppType|MeasCode|MeasDescription|MeasImpactType|MeasureID|TechGroup|TechType|UseCategory|UseSubCategory|PreDesc|StdDesc|SourceDesc|Version|NormUnit|NumUnits|UnitkW1stBaseline|UnitkWh1stBaseline|UnitTherm1stBaseline|UnitkW2ndBaseline|UnitkWh2ndBaseline|UnitTherm2ndBaseline|UnitMeaCost1stBaseline|UnitMeaCost2ndBaseline|UnitDirectInstallLab|UnitDirectInstallMat|UnitEndUserRebate|UnitIncentiveToOthers|NTG_ID|NTGRkW|NTGRkWh|NTGRTherm|NTGRCost|EUL_ID|EUL_Yrs|RUL_ID|RUL_Yrs|GSIA_ID|RealizationRatekW|RealizationRatekWh|RealizationRateTherm|InstallationRatekW|InstallationRatekWh|InstallationRateTherm|Residential_Flag|Upstream_Flag|PA|MarketEffectsBenefits|MarketEffectsCosts|RateScheduleElec|RateScheduleGas|CombustionType|MeasInflation|Comments"
DEER_NonRes = [
    "DEER:HVAC_Chillers",
    "DEER:HVAC_Split-Package_AC",
    "DEER:HVAC_Split-Package_HP",
    "DEER:Indoor_Non-CFL_Ltg",
]
discount_rate = 0.0766


class CET_Scan:
    def __init__(
        self,
        directory=cwd,
        scan_name="Test",
        program_year="2021",
        acc_version="2020",
        program_admin="PGE",
        climate_zone=["4"],
        mwh=[0.1],
        therms=[0],
        units=[1],
        ntg=[0.6],
        eul=[1],
        sector=["Res"],
        deer_load_shape=["DEER:HVAC_Eff_AC"],
        gas_sector=["Residential"],
        gas_savings_profile=["Annual"],
        admin_cost=[10],
        measure_cost=[50],
        incentive=[30],
    ):
        self.directory = directory
        self.scan_name = scan_name
        self.program_year = program_year
        self.acc_version = acc_version
        self.program_admin = program_admin
        self.climate_zone = climate_zone
        self.mwh = [m * u for m, u in zip(list(i for i in mwh), units)]
        self.therms = [t * u for t, u in zip(list(i for i in therms), units)]
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
        self.path = directory + "/" + scan_name
        self.kwh = [m * u * 1000 for m, u in zip(list(i for i in mwh), units)]

    def generate_cet_input_file(self, files="both"):

        # Create Folders and Path
        os.mkdir(self.path)

        if files == "both" or files == "cet_only":
            os.mkdir(self.path + "/cet")
            # Create ProgramCost.csv file for CET and write columns
            fname_costs = "ProgramCost.csv"
            fhand_costs = open(fname_costs, "w")
            print(programcost_columns, file=fhand_costs)

            # Create Measure.csv file for CET and write columns
            fname_measure = "Measure.csv"
            fhand_measure = open(fname_measure, "w")
            print(measure_columns, file=fhand_measure)

            # Add lines to CET ProgramCost and Measure files, scanning over variable

            for ind in range(len(self.mwh)):

                print(
                    "%s0%g|%s|%sQ1|%g|0|0|0|0|0|0|0|0|0|0|%s"
                    % (
                        self.program_admin,
                        self.index[ind],
                        self.program_year,
                        self.program_year,
                        self.admin_cost[ind],
                        self.program_admin,
                    ),
                    file=fhand_costs,
                )

                print(
                    "%s-%s-0%g|%s0%g|%sQ1|Commercial|CustIncentDown|Com|%s|%s|%s|%s|%s|AR||NMEC|Cust-NMEC|0||Pilot||Testing|||||Each|1|0|%g|%g|0|0|0|%g|0|0|0|%g|0|NonRes-sAll-NMEC|%g|%g|%g|%g||%g||0||1|1|1|1|1|1|0|0|%s|||||||"
                    % (
                        self.program_admin,
                        self.program_year,
                        self.index[ind],
                        self.program_admin,
                        self.index[ind],
                        self.program_year,
                        self.climate_zone[ind],
                        self.gas_savings_profile[ind],
                        self.gas_sector[ind],
                        self.deer_load_shape[ind],
                        "Non_Res"
                        if self.deer_load_shape[ind] in DEER_NonRes
                        else self.sector[ind],
                        self.kwh[ind],
                        self.therms[ind],
                        self.measure_cost[ind],
                        self.incentive[ind],
                        self.ntg[ind],
                        self.ntg[ind],
                        self.ntg[ind],
                        self.ntg[ind],
                        self.eul[ind],
                        self.program_admin,
                    ),
                    file=fhand_measure,
                )

                if (
                    self.sector[ind] == "Res"
                    and self.deer_load_shape[ind] in DEER_NonRes
                ):
                    print(
                        self.sector[ind]
                        + "/"
                        + self.deer_load_shape[ind]
                        + " Pairing Not Allowed in CET. Switching to Non_Res"
                    )

            # Close ProgramCost and Measure files
            fhand_costs.close()
            fhand_measure.close()

            zipObj = ZipFile(self.scan_name + ".zip", "w")
            zipObj.write(fname_measure)
            zipObj.write(fname_costs)
            zipObj.close()
            shutil.move(cwd + "/" + self.scan_name + ".zip", self.path + "/cet")

            print(
                "Your CET input file is at "
                + self.path
                + "/cet/"
                + self.scan_name
                + ".zip"
            )

        if files == "both" or files == "flexvalue_only":
            os.mkdir(self.path + "/flexvalue")
            input_row_dfs = []

            for ind in range(len(self.mwh)):
                flexvalue_inputs_dict = {
                    "ID": self.index[ind],
                    "start_year": self.program_year,
                    "start_quarter": 1,
                    "utility": self.program_admin,
                    "climate_zone": self.climate_zone[ind],
                    "mwh_savings": self.mwh[ind],
                    "load_shape": "NONRES_"
                    + self.deer_load_shape[ind][5:].upper().replace("-", "_")
                    if self.deer_load_shape[ind] in DEER_NonRes
                    else self.sector[ind].upper()
                    + "_"
                    + self.deer_load_shape[ind][5:].upper().replace("-", "_"),
                    "therms_savings": self.therms[ind],
                    "therms_profile": self.gas_savings_profile[ind].split(" ")[0],
                    "units": self.units[ind] / self.units[ind],
                    "eul": self.eul[ind],
                    "ntg": self.ntg[ind],
                    "discount_rate": discount_rate,
                    "admin": self.admin_cost[ind],
                    "measure": self.measure_cost[ind],
                    "incentive": self.incentive[ind],
                }

                flexvalue_inputs_df = pd.DataFrame.from_dict(
                    flexvalue_inputs_dict, orient="index", columns=[self.index[ind]]
                )

                input_row_dfs.append(flexvalue_inputs_df)

            user_inputs = pd.concat(input_row_dfs, axis=1).T

            user_inputs.to_csv(
                self.path
                + "/flexvalue/"
                + self.scan_name
                + "_flexvalue_user_inputs.csv"
            )

            print(
                "Your FLEXvalue input file is at "
                + self.path
                + "/flexvalue/"
                + self.scan_name
                + "_flexvalue_user_inputs.csv"
            )

    def parse_cet_output(self, cet_output_folder=cwd):

        # Create file to store key results
        fname_w = "Results_%s.csv" % (self.scan_name)
        fhand_w = open(fname_w, "w")
        print(
            "ACC_Version"
            + ",Program_Year"
            + ",PA"
            + ",Climate_Zone"
            + ",MWh_Savings"
            + ",Therms_Savings"
            + ",NTG"
            + ",Sector"
            + ",Admin"
            + ",Measure_Cost"
            + ",Incentive"
            + ",DEER_Load_Shape"
            + ",Elec_Benefits"
            + ",Gas_Benefits"
            + ",PAC_Costs"
            + ",TRC_Costs"
            + ",PAC"
            + ",TRC",
            file=fhand_w,
        )

        # Find and transfer output .zip file from folder with CET output file - can set cet_output_folder to download folder but default is current directory
        glob_search_str = os.path.join(cet_output_folder, self.scan_name + "_*.zip")

        loc_search = glob.glob(glob_search_str)
        if loc_search:
            loc = loc_search[0]
        else:
            raise ValueError(f"Can not find CET output zip file in {glob_search_str}")
        fname = os.path.basename(loc)
        fnum = re.findall(".*cet_ui_run_([0-9]+)", fname)[0]
        if loc != self.path + "/":
            shutil.move(loc, self.path + "/cet/")

        # Unzip output files
        with ZipFile(self.path + "/cet/" + fname, "r") as zip_ref:
            zip_ref.extractall(self.path + "/cet/")

        # Extract and print key results to file
        fhand_r = open(self.path + "/cet/" + fnum + "_outputs.csv")
        contents = fhand_r.read()
        parse = contents.split("|")
        offset = 117
        rows = len(self.mwh)

        for ind in range(len(self.mwh)):
            print(
                self.acc_version,
                ",",
                self.program_year,
                ",",
                self.program_admin,
                ",",
                self.climate_zone[ind],
                ",",
                self.mwh[ind],
                ",",
                self.therms[ind],
                ",",
                self.ntg[ind],
                ",",
                "Non_Res"
                if self.deer_load_shape[ind] in DEER_NonRes
                else self.sector[ind],
                ",",
                self.admin_cost[ind],
                ",",
                self.measure_cost[ind],
                ",",
                self.incentive[ind],
                ",",
                self.deer_load_shape[ind],
                ",",
                parse[(rows - ind - 1) * offset + 141],
                ",",
                parse[(rows - ind - 1) * offset + 142],
                ",",
                parse[(rows - ind - 1) * offset + 146],
                ",",
                parse[(rows - ind - 1) * offset + 145],
                ",",
                parse[(rows - ind - 1) * offset + 151],
                ",",
                parse[(rows - ind - 1) * offset + 150],
                file=fhand_w,
            )

        fhand_r.close()
        fhand_w.close()
        shutil.move(cwd + "/" + fname_w, self.path + "/cet/")
