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
import pandas as pd
import numpy as np

from .db import get_filtered_acc_elec, get_filtered_acc_gas, get_deer_load_shape
from .settings import (
    ACC_COMPONENTS_ELECTRICITY,
    ACC_COMPONENTS_GAS,
    THERMS_PROFILE_ADJUSTMENT,
)

__all__ = (
    "get_quarterly_discount_df",
    "calculate_trc_costs",
    "calculate_pac_costs",
    "FlexValueProject",
    "FlexValueRun",
)


def get_quarterly_discount_df(eul, discount_rate):
    """Calculates the quarterly discount factor for the duration of the EUL

    .. math::
        discount = 1/((1+(discountRate/4))^{quarter - 1})

    Parameters
    ----------
    eul: int
        Effective Useful Life (EUL) means the average time over which an energy efficiency measure results in energy savings, including the effects of equipment failure, removal, and cessation of use.
    discount_rate: float
        The quarterly discount rate to be applied to the net present value calculation

    Returns
    -------
    pd.DataFrame
        A dataframe containing a discount to be applied to every quarter for every year of the EUL.

    """
    qd = pd.DataFrame(np.arange(1, eul * 4 + 1)).rename({0: "quarter"}, axis=1)
    qd["discount"] = 1 / ((1 + (discount_rate / 4)) ** (qd["quarter"] - 1))
    return qd


def calculate_trc_costs(admin, measure, incentive, discount_rate, ntg):
    """Calculates the TRC costs

    .. math::
        admin + ((1 - ntg) * incentive + ntg * measure) / (1 + (discountRate / 4))

    Parameters
    ----------
    admin: float
        The administrative costs assigned to a given measure, project, or portfolio
    measure: float
        The measure costs assigned to given measure, project, or portfolio
    incentive: float
        The incentive costs assigned to given measure, project, or portfolio
    discount_rate: float
        The quarterly discount rate to be applied to the net present value calculation
    ntg: float
        Net to gross ratio

    Returns
    -------
    float
    """
    return admin + ((1 - ntg) * incentive + ntg * measure) / (1 + (discount_rate / 4))


def calculate_pac_costs(admin, incentive, discount_rate, ntg):
    """Calculate PAC costs

    .. math::
        admin + incentive / (1 + (discountRate / 4))

    Parameters
    ----------
    admin: float
        The administrative costs assigned to a given measure, project, or portfolio
    incentive: float
        The incentive costs assigned to given measure, project, or portfolio
    discount_rate: float
        The quarterly discount rate to be applied to the net present value calculation
    ntg: float
        Net to gross ratio

    Returns
    -------
    float
    """
    return admin + incentive / (1 + (discount_rate / 4))


class FlexValueProject:
    """Parameters and calculations for a given measure, project, or portfolio

    Parameters
    ----------
    identifier: str
        A unique identifier used to reference this measure, project, or portfolio
    start_year: int
        The year to start with when using avoided costs data
    start_quarter: int
        The quarter to start with when using avoided costs data
    utility: str
        Which uility to filter by when loading avoided costs data
    climate_zone: str
        Which climate zone to filter by when loading avoided costs data
    mwh_savings:
        The first year electricity gross savings in MWh
    load_shape: str
        Either the name of a DEER loadshape or a reference to a meter id in the
        `metered_load_shapes` dataframe.
    load_shape_df: pd.DataFrame
        A dataframe containing up to 8760 rows that contains the load shape for
        the given `load_shape`.
    therms_savings: float
        The first year gas gross savings in Therms
    therms_profile: float
        Indicates what sort of adjustment to make on the therms savings,
        can be one of ['annual', 'summer', 'winter']
    units: int
        Multiplier of the therms_savings and mwh_savings
    eul: int
        Effective Useful Life (EUL) means the average time over which an energy
        efficiency measure results in energy savings, including the effects of
        equipment failure, removal, and cessation of use.
    incentive: float
        The incentive costs assigned to given measure, project, or portfolio
    discount_rate: float
        The quarterly discount rate to be applied to the net present value calculation
    ntg: float
        Net to gross ratio
    admin: float
        The administrative costs assigned to a given measure, project, or portfolio
    measure: float
        The measure costs assigned to given measure, project, or portfolio
    database_version: str
        The version corresponding to the database that contains the avoided costs data.
        Requires that version's database to have already been downloaded
        using the `flexvalue downloaded-avoided-costs-data-db --version 20XX` command.

    Returns
    -------
    FlexValueProject
    """

    def __init__(
        self,
        identifier,
        start_year,
        start_quarter,
        utility,
        climate_zone,
        mwh_savings,
        load_shape,
        load_shape_df,
        therms_savings,
        therms_profile,
        units,
        eul,
        ntg,
        discount_rate,
        admin,
        measure,
        incentive,
        database_version="2020",
    ):
        self.identifier = identifier
        self.start_year = start_year
        self.start_quarter = start_quarter
        self.utility = utility
        self.climate_zone = climate_zone
        self.mwh_savings = mwh_savings
        self.load_shape = load_shape
        self.load_shape_df = load_shape_df
        self.therms_savings = therms_savings
        self.therms_profile = therms_profile
        self.units = units
        self.eul = eul
        self.ntg = ntg
        self.discount_rate = discount_rate
        self.admin = admin
        self.measure = measure
        self.incentive = incentive
        self.database_version = database_version

    def calculate_trc_electricity_benefits(self):
        """Calculate electric TRC benefits

        Returns
        -------
        pd.DataFrame
            hourly benefits for every quarter of every year from the start year through
            the EUL
        """

        load_shape_df = self.load_shape_df.copy(deep=True)
        load_shape_df["hourly_savings"] = (
            self.mwh_savings * self.units * self.ntg * load_shape_df
        )

        def _get_avoided_costs_electricity_df():

            end_year_adjustment = 0 if self.start_quarter == 1 else 1
            acc_unbounded = get_filtered_acc_elec(
                self.database_version,
                self.utility,
                self.climate_zone,
                self.start_year,
                end_year=self.start_year + self.eul + end_year_adjustment,
            )
            return self._add_quarter_col_to_avoided_costs(acc_unbounded)

        avoided_costs_electricity_df = _get_avoided_costs_electricity_df()

        non_discounted = pd.merge(
            load_shape_df,
            avoided_costs_electricity_df,
            how="left",
            left_on="hour_of_year",
            right_on="hour_of_year",
        )
        discounted = pd.merge(
            non_discounted,
            get_quarterly_discount_df(self.eul, self.discount_rate),
            how="left",
            left_on="quarter",
            right_on="quarter",
        )
        discounted["marginal_ghg"] = (
            discounted["hourly_savings"] * discounted["marginal_ghg"]
        )

        for component in ACC_COMPONENTS_ELECTRICITY:
            discounted[component] = (
                discounted["hourly_savings"]
                * discounted["discount"]
                * discounted[component]
            )
        discounted["av_csts_levelized"] = (
            non_discounted["total"] * discounted["discount"]
        )
        discounted["identifier"] = self.identifier
        return discounted

    def calculate_trc_gas_benefits(self):
        """Calculate gas TRC benefits

        Returns
        -------
        pd.DataFrame
            quarterly benefits for every year from the start year through
            the EUL

        """

        def _get_avoided_costs_gas_df():
            """Assemble non-discounted monthly gas avoided costs for the duration of the EUL"""
            end_year_adjustment = 0 if self.start_quarter == 1 else 1

            acc_gas_unbounded = get_filtered_acc_gas(
                self.database_version,
                self.start_year,
                self.start_year + self.eul + end_year_adjustment,
            )
            return self._add_quarter_col_to_avoided_costs(acc_gas_unbounded)

        non_discounted_gas = (
            _get_avoided_costs_gas_df()
            .groupby(["year", "quarter"])
            .mean()
            .reset_index()
            .drop("month", axis=1)
        )
        non_discounted_gas["therms_savings"] = self.therms_savings / 4

        # TODO: These factors are emperically derived and provide a close match to the CET.
        # Further open-source development would be beneficial.
        therms_profile_adjustment = THERMS_PROFILE_ADJUSTMENT.get(self.utility, {}).get(
            self.therms_profile
        )
        if not therms_profile_adjustment:
            raise ValueError(
                "Must supply a therms_profile that is one of: "
                "['annual', 'summer', 'winter']"
            )

        non_discounted_gas["therms_profile_adjustment"] = therms_profile_adjustment

        discounted_gas = pd.merge(
            non_discounted_gas,
            get_quarterly_discount_df(self.eul, self.discount_rate),
            how="left",
            left_on="quarter",
            right_on="quarter",
        )

        for component in ACC_COMPONENTS_GAS:
            discounted_gas[component] = (
                self.ntg
                * self.units
                * discounted_gas["therms_savings"]
                * discounted_gas["discount"]
                * discounted_gas[component]
                * discounted_gas["therms_profile_adjustment"]
            )

        discounted_gas["av_csts_levelized"] = (
            non_discounted_gas["total"] * discounted_gas["discount"]
        )
        discounted_gas["identifier"] = self.identifier
        return discounted_gas

    def _add_quarter_col_to_avoided_costs(self, avoided_costs_df):
        avoided_costs_df["quarter"] = (
            (avoided_costs_df["year"] - self.start_year) * 4
            - self.start_quarter
            + np.ceil(avoided_costs_df["month"] / 3)
            + 1
        )

        return avoided_costs_df[
            (avoided_costs_df["quarter"] >= 1)
            & (avoided_costs_df["quarter"] <= self.eul * 4)
        ]

    def get_output_table(self):
        """Aggregate benefits and calculate TRC and PAC

        Returns
        -------
        pd.DataFrame
            A table with summarized outputs including TRC and PAC, total costs,
            and GHG impacts

        """

        trc_electricity_benefits = self.calculate_trc_electricity_benefits()
        trc_gas_benefits = self.calculate_trc_gas_benefits()

        # Calculating Sums

        ## Therms
        gas_benefits_total = trc_gas_benefits["total"].sum()
        first_year_gas_savings_therms = self.ntg * self.therms_savings
        lifecycle_gas_savings_therms = first_year_gas_savings_therms * self.eul
        lifecycle_gas_savings_ghg = lifecycle_gas_savings_therms * self.units * 0.006

        ## MWH
        elec_benefits_total = trc_electricity_benefits["total"].sum()
        lifecycle_elec_savings_mwh = trc_electricity_benefits["hourly_savings"].sum()
        first_year_elec_savings_mwh = lifecycle_elec_savings_mwh / self.eul
        lifecycle_elec_savings_ghg = trc_electricity_benefits["marginal_ghg"].sum()

        ## Totals
        total_benefits_total = elec_benefits_total + gas_benefits_total
        lifecycle_total_savings_ghg = (
            lifecycle_elec_savings_ghg + lifecycle_gas_savings_ghg
        )

        ## Costs
        trc_costs = calculate_trc_costs(
            self.admin, self.measure, self.incentive, self.discount_rate, self.ntg
        )
        pac_costs = calculate_pac_costs(
            self.admin, self.incentive, self.discount_rate, self.ntg
        )

        outputs_df = pd.DataFrame(
            {
                "TRC": total_benefits_total / trc_costs,
                "PAC": total_benefits_total / pac_costs,
                "TRC (and PAC) Electric Benefits ($)": elec_benefits_total,
                "TRC (and PAC) Gas Benefits ($)": gas_benefits_total,
                "TRC (and PAC) Total Benefits ($)": gas_benefits_total
                + elec_benefits_total,
                "TRC Costs ($)": trc_costs,
                "PAC Costs ($)": pac_costs,
                "Electricity First Year Net Savings (MWh)": first_year_elec_savings_mwh,
                "Electricity Lifecycle Net Savings (MWh)": lifecycle_elec_savings_mwh,
                "Gas First Year Net Savings (Therms)": first_year_gas_savings_therms,
                "Gas Lifecycle Net Savings (Therms)": lifecycle_gas_savings_therms,
                "Electricity Lifecycle GHG Savings (Tons)": lifecycle_elec_savings_ghg,
                "Gas Lifecycle GHG Savings (Tons)": lifecycle_gas_savings_ghg,
                "Total Lifecycle GHG Savings (Tons)": lifecycle_total_savings_ghg,
            },
            index=[self.identifier],
        )

        outputs_df = outputs_df.round(
            {c: 2 if "($)" in c else 3 for c in outputs_df.columns}
        )
        outputs_df.index.name = "Outputs"
        return outputs_df


class FlexValueRun:
    """Representation of a single calculation for a set of projects

    Parameters
    ----------
    metered_load_shape: pd.DataFrame
        Optionally a dataframe containing up to 8760 rows with each column
        representing the savings attributed to a single electricity meter.
        This dataframe is joined with the built-in 8760 DEER
        load shapes to provide additional available load shapes when
        later constructing a user_inputs table.
    database_version: str
        The version corresponding to the database that contains the avoided costs data.
        Requires that version's database to have already been downloaded
        using the `flexvalue downloaded-avoided-costs-data-db --version 20XX` command.

    Returns
    -------
    FlexValueRun
    """

    def __init__(self, metered_load_shape=None, database_version="2020"):
        self.database_version = database_version

        # Get all load_shape options
        self.deer_load_shape = get_deer_load_shape(self.database_version)
        self.all_load_shapes_df = (
            pd.concat([metered_load_shape, self.deer_load_shape], axis=1)
            if metered_load_shape is not None
            else self.deer_load_shape
        )
        # Make all columns upper-cased so matching can be case insensitive
        self.all_load_shapes_df.rename(
            columns={col: col.upper() for col in self.all_load_shapes_df.columns},
            inplace=True,
        )

    def get_flexvalue_projects(self, user_inputs_df):
        """Translate the user inputs dataframe into a dictionary of FlexValueProject objects

        Parameters
        ----------
        user_inputs_df: pd.DataFrame
            A dataframe containing all of the inputs for each measure/project/portoflio
            in the FlexValueRun

        Returns
        -------
        dict
            A dictionary keyed on the id of the measure/project/portfolio withe value
            being an instantiation of the FlexValueProject using those inputs
        """

        def _get_load_shape_df(utility, load_shape, mwh_savings):
            # Check that if electricity savings are supplied, a load shape is available
            # For DEER load shapes, its currently prefixed by the utility name
            # TODO (ssuffian): Fix this if db changes where utility is a column
            if load_shape in self.all_load_shapes_df.columns:
                return self.all_load_shapes_df[[load_shape]]
            elif f"{utility}_{load_shape}" in self.all_load_shapes_df.columns:
                return self.all_load_shapes_df[[f"{utility}_{load_shape}"]]
            elif mwh_savings != 0:
                raise ValueError(
                    f"Neither {load_shape} nor {utility}_{load_shape} can be found in\n"
                    f"{self.all_load_shapes_df.columns}"
                )

        # validate user inputs
        for int_col in ["start_year", "start_quarter", "eul"]:
            user_inputs_df[int_col] = user_inputs_df[int_col].astype(int)
        for float_col in [
            "mwh_savings",
            "therms_savings",
            "units",
            "ntg",
            "discount_rate",
            "admin",
            "measure",
            "incentive",
        ]:
            user_inputs_df[float_col] = user_inputs_df[float_col].astype(float)
        return {
            user_input["ID"]: FlexValueProject(
                identifier=user_input["ID"],
                start_year=user_input["start_year"],
                start_quarter=user_input["start_quarter"],
                utility=user_input["utility"].upper(),
                climate_zone=user_input["climate_zone"],
                mwh_savings=user_input["mwh_savings"],
                load_shape=user_input["load_shape"],
                therms_savings=user_input["therms_savings"],
                therms_profile=user_input["therms_profile"].lower(),
                units=user_input["units"],
                eul=user_input["eul"],
                ntg=user_input["ntg"],
                discount_rate=user_input["discount_rate"],
                admin=user_input["admin"],
                measure=user_input["measure"],
                incentive=user_input["incentive"],
                load_shape_df=_get_load_shape_df(
                    user_input["utility"].upper(),
                    user_input["load_shape"].upper(),
                    user_input["mwh_savings"],
                ),
                database_version=self.database_version,
            )
            for user_input in user_inputs_df.reset_index().to_dict("records")
        }

    def get_all_trc_electricity_benefits_df(self, user_inputs):
        """Concatanates the electricity benefits across all FlexValueProjects

        Parameters
        ----------
        user_inputs: pd.DataFrame
            A dataframe containing all of the inputs for each measure/project/portoflio
            in the FlexValueRun

        Returns
        -------
        pd.DataFrame
            A dataframe containing the electricity benefits for all of the measure/
            project/portoflio entries.
        """
        return (
            pd.concat(
                [
                    flx_project.calculate_trc_electricity_benefits()
                    for flx_project in self.get_flexvalue_projects(user_inputs).values()
                ]
            )
            .sort_values(["identifier", "year", "hour_of_year"])
            .reset_index(drop=True)
        )

    def get_all_trc_gas_benefits_df(self, user_inputs):
        return pd.concat(
            [
                flx_project.calculate_trc_gas_benefits()
                for flx_project in self.get_flexvalue_projects(user_inputs).values()
            ]
        )

    def get_total_trc_gas_benefits(self, user_inputs):
        """The total gas benefits across all FlexValueProjects

        Parameters
        ----------
        user_inputs: pd.DataFrame
            A dataframe containing all of the inputs for each measure/project/portoflio
            in the FlexValueRun

        Returns
        -------
        float
            The sum of all gas benefits across all measure/project/portfolio entries.
        """
        return sum(
            [
                flx_project.calculate_trc_gas_benefits()["total"].sum()
                for flx_project in self.get_flexvalue_projects(user_inputs).values()
            ]
        )

    def get_all_output_tables(self, user_inputs):
        """Returns a table containing the aggregated outputs for each project

        Parameters
        ----------
        user_inputs: pd.DataFrame
            A dataframe containing all of the inputs for each measure/project/portoflio
            in the FlexValueRun

        Returns
        -------
        pd.DataFrame
            A table with summarized outputs including TRC and PAC, total costs,
            and GHG impacts summed across all measure/project/portfolio entries.
            The TRC and PAC values are then recalculated based on the summed benefits
            and costs.
        """
        all_output_tables = [
            flx_project.get_output_table()
            for flx_project in self.get_flexvalue_projects(user_inputs).values()
        ]
        outputs_table = pd.concat(all_output_tables)
        outputs_table_totals = outputs_table.sum()

        # special recalculation of TRC and PAC
        outputs_table_totals["TRC"] = (
            outputs_table_totals["TRC (and PAC) Electric Benefits ($)"]
            + outputs_table_totals["TRC (and PAC) Gas Benefits ($)"]
        ) / outputs_table_totals["TRC Costs ($)"]
        outputs_table_totals["PAC"] = (
            outputs_table_totals["TRC (and PAC) Electric Benefits ($)"]
            + outputs_table_totals["TRC (and PAC) Gas Benefits ($)"]
        ) / outputs_table_totals["PAC Costs ($)"]

        outputs_table = outputs_table.round(3)
        outputs_table["TRC Costs ($)"] = outputs_table["TRC Costs ($)"].round(2)
        outputs_table["PAC Costs ($)"] = outputs_table["PAC Costs ($)"].round(2)

        outputs_table_totals = outputs_table_totals.round(3)
        outputs_table_totals["TRC Costs ($)"] = outputs_table_totals[
            "TRC Costs ($)"
        ].round(2)
        outputs_table_totals["PAC Costs ($)"] = outputs_table_totals[
            "PAC Costs ($)"
        ].round(2)

        return outputs_table, outputs_table_totals

    def get_electric_benefits_full_outputs(self, user_inputs):
        """Aggregates the electricity benefits into a year-month average daily loadshape

        Parameters
        ----------
        user_inputs: pd.DataFrame
            A dataframe containing all of the inputs for each measure/project/portoflio
            in the FlexValueRun

        Returns
        -------
        pd.DataFrame
            Returns a year-month average daily load shape for each
            measure/project/portoflio, concatanated into a single dataframe

        """
        # Year-Month average daily loadshape
        return pd.concat(
            [
                flx_project.calculate_trc_electricity_benefits()
                .groupby(["identifier", "hour_of_day", "year", "month"])
                .agg(
                    {
                        **{
                            "hourly_savings": "sum",
                            "marginal_ghg": "sum",
                            "av_csts_levelized": "mean",
                        },
                        **{
                            component: "sum" for component in ACC_COMPONENTS_ELECTRICITY
                        },
                    }
                )
                for flx_project in self.get_flexvalue_projects(user_inputs).values()
            ]
        ).reset_index()

    def get_results(self, user_inputs):
        """Assemble and report tabular project and portfolio-level inputs and outputs

        Parameters
        ----------
        user_inputs: pd.DataFrame
            A dataframe containing all of the inputs for each measure/project/portoflio
            in the FlexValueRun

        Returns
        -------
        outputs_table: pd.DataFrame
            A table with summarized outputs including TRC and PAC, total costs,
            and GHG impacts summed across all measure/project/portfolio entries.
            The TRC and PAC values are then recalculated based on the summed benefits
            and costs.
        elec_benefits: pd.DataFrame
            Returns a year-month average daily load shape for each
            measure/project/portoflio, concatanated into a single dataframe
        gas_benefits: float
            The sum of all gas benefits across all measure/project/portfolio entries.
        """
        outputs_table, outputs_table_totals = self.get_all_output_tables(
            user_inputs=user_inputs
        )
        elec_benefits = self.get_all_trc_electricity_benefits_df(user_inputs)
        gas_benefits = self.get_total_trc_gas_benefits(user_inputs)
        # if index wasn't already set with the ID colum, set it for joining to the output
        if user_inputs.index.name != "ID":
            user_inputs = user_inputs.set_index("ID")
        outputs_table = user_inputs.join(outputs_table).reset_index()
        return outputs_table, outputs_table_totals, elec_benefits, gas_benefits

    def get_time_series_results(self, user_inputs):
        """ Return raw time series electricity and gas benefits data as an iterator.

        Parameters
        ----------
        user_inputs: pd.DataFrame
            A dataframe containing all of the inputs for each measure/project/portoflio
            in the FlexValueRun

        Returns
        -------       
        An iterator in which each item is a tuple with the following elements:
        
        elec_benefits: pd.DataFrame
            Returns an hourly time series load shape for each
            measure/project/portoflio, concatanated into a single dataframe
        gas_benefits: float
            Returns a quarterly time series load shape for each
            measure/project/portoflio, concatanated into a single dataframe
        """
        
        
        # In order to match the output of the get_results method, we need to 
        # append all of the load shape column names to the data frame.  
        load_shape_cols = [i.load_shape_df.columns[0] for i in self.get_flexvalue_projects(user_inputs).values()]

        # Step through all projects in user_inputs and yield the elec and gas benefits 
        for i in range(len(user_inputs)):            
            row = user_inputs.iloc[i:i+1]
            elec_benefits = self.get_all_trc_electricity_benefits_df(row)
            for col in load_shape_cols:
                if col not in elec_benefits.columns:
                    elec_benefits.loc[:,col] = None
            gas_benefits = self.get_all_trc_gas_benefits_df(row)
            yield elec_benefits, gas_benefits
