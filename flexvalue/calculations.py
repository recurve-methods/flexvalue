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
from IPython.display import display, HTML

from .db import get_filtered_acc_elec, get_filtered_acc_gas, get_deer_load_shape
from .settings import ACC_COMPONENTS_ELECTRICITY, ACC_COMPONENTS_GAS

__all__ = (
    "get_quarterly_discount_df",
    "calculate_trc_costs",
    "calculate_pac_costs",
    "FlexValueProject",
    "FlexValueRun",
)


def get_quarterly_discount_df(eul, discount_rate):
    """Calculate quarterly discount factor for the duration of the EUL"""
    qd = pd.DataFrame(np.arange(1, eul * 4 + 1)).rename({0: "quarter"}, axis=1)
    qd["discount"] = 1 / ((1 + (discount_rate / 4)) ** (qd["quarter"] - 1))
    return qd


def calculate_trc_costs(admin, measure, incentive, discount_rate, ntg):
    """Calculate TRC costs"""
    return admin + ((1 - ntg) * incentive + ntg * measure) / (1 + (discount_rate / 4))


def calculate_pac_costs(admin, incentive, discount_rate, ntg):
    """Calculate PAC costs"""
    return admin + incentive / (1 + (discount_rate / 4))


class FlexValueProject:
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
        database_year="2020",
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
        self.database_year = database_year

    def calculate_trc_electricity_benefits(self):
        """Calculate electric TRC benefits"""

        load_shape_df = self.load_shape_df.copy(deep=True)
        load_shape_df["hourly_savings"] = (
            self.mwh_savings * self.units * self.ntg * load_shape_df
        )
        avoided_costs_electricity_df = self.get_avoided_costs_electricity_df()

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
        """Calculate gas TRC benefits"""
        non_discounted_gas = (
            self.get_avoided_costs_gas_df()
            .groupby(["year", "quarter"])
            .mean()
            .reset_index()
            .drop("month", axis=1)
        )
        non_discounted_gas["therms_savings"] = self.therms_savings / 4

        # TODO: These factors are emperically derived and provide a close match to the CET.
        # Further open-source development would be beneficial.
        if self.therms_profile.lower() == "annual":
            therms_profile_adjustment = 0.965
        elif self.therms_profile.lower() == "summer":
            therms_profile_adjustment = 0.853
        elif self.therms_profile.lower() == "winter":
            therms_profile_adjustment = 1.072

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

    def get_avoided_costs_electricity_df(self):
        """Assemble the non-discounted hourly avoided costs and GHG by component for the duration of the EUL"""

        end_year_adjustment = 0 if self.start_quarter == 1 else 1
        acc_unbounded = get_filtered_acc_elec(
            self.database_year,
            self.utility,
            self.climate_zone,
            self.start_year,
            end_year=self.start_year + self.eul + end_year_adjustment,
        )
        return self._add_quarter_col_to_avoided_costs(acc_unbounded)

    def get_avoided_costs_gas_df(self):
        """Assemble non-discounted monthly gas avoided costs for the duration of the EUL"""
        end_year_adjustment = 0 if self.start_quarter == 1 else 1

        acc_gas_unbounded = get_filtered_acc_gas(
            self.database_year,
            self.start_year,
            self.start_year + self.eul + end_year_adjustment,
        )
        return self._add_quarter_col_to_avoided_costs(acc_gas_unbounded)

    def get_output_table(self):
        """Establish project-level outputs dataframe"""

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

        outputs_dict = {
            "TRC": total_benefits_total / trc_costs,
            "PAC": total_benefits_total / pac_costs,
            "TRC (and PAC) Electric Benefits ($)": elec_benefits_total,
            "TRC (and PAC) Gas Benefits ($)": gas_benefits_total,
            "TRC (and PAC) Total Benefits ($)": gas_benefits_total
            + elec_benefits_total,
            "TRC Costs ($)": trc_costs,
            "PAC Costs ($)": pac_costs,
            "First Year Net MWh Savings": first_year_elec_savings_mwh,
            "Lifecycle Net MWh Savings": lifecycle_elec_savings_mwh,
            "First Year Net Therms Savings": first_year_gas_savings_therms,
            "Lifecycle Net Therms Savings": lifecycle_gas_savings_therms,
            "Lifecycle Electric GHG Savings (Tons)": lifecycle_elec_savings_ghg,
            "Lifecycle Gas GHG Savings (Tons)": lifecycle_gas_savings_ghg,
            "Lifecycle Total GHG Savings (Tons)": lifecycle_total_savings_ghg,
        }

        outputs_df = pd.DataFrame.from_dict(
            outputs_dict, orient="index", columns=[self.identifier]
        )
        # Rounding $'s to 2 decimals and GHG to 3
        # TODO (ssuffian): Simplify by including a $ in all 2-round columns
        numeric_cols = [
            "Lifecycle Electric GHG Savingsons)",
            "Lifecycle Gas GHG Savings (Tons)",
            "Lifecycle Total GHG Savings (Tons)",
            "TRC",
            "PAC",
        ]
        outputs_df.round({c: 3 if c in numeric_cols else 2 for c in outputs_df.columns})
        outputs_df.index.name = "Outputs"
        return outputs_df


class FlexValueRun:
    def __init__(self, metered_load_shape=None, database_year="2020"):
        self.database_year = database_year

        # Get all load_shape options
        self.deer_load_shape = get_deer_load_shape(self.database_year)
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

    def get_flexvalue_meters(self, user_inputs):
        def _get_load_shape_df(load_shape, mwh_savings):
            # Check that if electricity savings are supplied, a load shape is available
            if load_shape in self.all_load_shapes_df.columns:
                return self.all_load_shapes_df[[load_shape]]
            elif mwh_savings != 0:
                raise ValueError(
                    f"{load_shape} can not be found in\n"
                    f"{self.all_load_shapes_df.columns}"
                )

        return {
            user_input["ID"]: FlexValueProject(
                identifier=user_input["ID"],
                start_year=user_input["start_year"],
                start_quarter=user_input["start_quarter"],
                utility=user_input["utility"],
                climate_zone=user_input["climate_zone"],
                mwh_savings=user_input["mwh_savings"],
                load_shape=user_input["load_shape"],
                therms_savings=user_input["therms_savings"],
                therms_profile=user_input["therms_profile"],
                units=user_input["units"],
                eul=user_input["eul"],
                ntg=user_input["ntg"],
                discount_rate=user_input["discount_rate"],
                admin=user_input["admin"],
                measure=user_input["measure"],
                incentive=user_input["incentive"],
                load_shape_df=_get_load_shape_df(
                    user_input["load_shape"].upper(), user_input["mwh_savings"]
                ),
                database_year=self.database_year,
            )
            for user_input in user_inputs.reset_index().to_dict("records")
        }

    def get_all_trc_electricity_benefits_df(self, user_inputs, metered_load_shape=None):
        return (
            pd.concat(
                [
                    flx_meter.calculate_trc_electricity_benefits()
                    for flx_meter in self.get_flexvalue_meters(user_inputs).values()
                ]
            )
            .sort_values(["identifier", "year", "hour_of_year"])
            .reset_index(drop=True)
        )

    def get_total_trc_gas_benefits(self, user_inputs, metered_load_shape=None):
        return sum(
            [
                flx_meter.calculate_trc_gas_benefits()["total"].sum()
                for flx_meter in self.get_flexvalue_meters(user_inputs).values()
            ],
        )

    def get_all_output_tables(self, user_inputs, metered_load_shape=None):
        """Returns a table containing the aggregated outputs for each project"""
        all_output_tables = [
            flx_meter.get_output_table()
            for flx_meter in self.get_flexvalue_meters(user_inputs).values()
        ]
        outputs_table = pd.concat(all_output_tables, axis=1)
        outputs_table_totals = outputs_table.sum(axis=1)

        # special recalculation of TRC and PAC
        outputs_table_totals["TRC"] = (
            outputs_table_totals["TRC (and PAC) Electric Benefits ($)"]
            + outputs_table_totals["TRC (and PAC) Gas Benefits ($)"]
        ) / outputs_table_totals["TRC Costs ($)"]
        outputs_table_totals["PAC"] = (
            outputs_table_totals["TRC (and PAC) Electric Benefits ($)"]
            + outputs_table_totals["TRC (and PAC) Gas Benefits ($)"]
        ) / outputs_table_totals["PAC Costs ($)"]
        """
        outputs_table.loc["TRC", "Totals"] = (
            outputs_table["Totals"]["TRC (and PAC) Electric Benefits ($)"]
            + outputs_table["Totals"]["TRC (and PAC) Gas Benefits ($)"]
        ) / outputs_table["Totals"]["TRC Costs ($)"]
        outputs_table.loc["PAC", "Totals"] = (
            outputs_table["Totals"]["TRC (and PAC) Electric Benefits ($)"]
            + outputs_table["Totals"]["TRC (and PAC) Gas Benefits ($)"]
        ) / outputs_table["Totals"]["PAC Costs ($)"]
        """

        # TODO (ssuffian) Simplify rounding after no longer comparing to original script
        outputs_table = outputs_table.round(3)
        outputs_table.loc["TRC Costs ($)"] = outputs_table.loc["TRC Costs ($)"].round(2)
        outputs_table.loc["PAC Costs ($)"] = outputs_table.loc["PAC Costs ($)"].round(2)

        outputs_table_totals = outputs_table_totals.round(3)
        outputs_table_totals["TRC Costs ($)"] = outputs_table_totals[
            "TRC Costs ($)"
        ].round(2)
        outputs_table_totals["PAC Costs ($)"] = outputs_table_totals[
            "PAC Costs ($)"
        ].round(2)

        outputs_table["Totals"] = outputs_table_totals
        return outputs_table.T

    def get_electric_benefits_full_outputs(self, user_inputs):
        """Returns detailed project-level output table that can be used for further analysis"""
        # TODO (ssuffian): Column order is to ensure test results are the same
        # Year-Month average daily loadshape
        return pd.concat(
            [
                flx_meter.calculate_trc_electricity_benefits()
                .groupby(["identifier", "hour_of_day", "year", "month"])
                .agg(
                    {
                        **{
                            "hourly_savings": "sum",
                        },
                        **{
                            component: "sum" for component in ACC_COMPONENTS_ELECTRICITY
                        },
                        **{"marginal_ghg": "sum", "av_csts_levelized": "mean"},
                    }
                )
                for flx_meter in self.get_flexvalue_meters(user_inputs).values()
            ],
        ).reset_index()

    def get_results(self, user_inputs):
        """Assemble and report tabular project and portfolio-level inputs and outputs"""
        outputs_table = self.get_all_output_tables(user_inputs=user_inputs)
        elec_benefits = self.get_all_trc_electricity_benefits_df(user_inputs)
        gas_benefits = self.get_total_trc_gas_benefits(user_inputs)
        return outputs_table, elec_benefits, gas_benefits
