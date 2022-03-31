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
from matplotlib.ticker import AutoMinorLocator
import matplotlib.pyplot as plt
import re
import seaborn as sns
import numpy as np

from .settings import ACC_COMPONENTS_ELECTRICITY

__all__ = ("plot_results",)


def plot_results(outputs_table_totals, elec_benefits, gas_benefits):
    """Generate a series of plots based on the results of the FlexValueRun

    Parameters
    ----------
    outputs_table_totals: pd.DataFrame
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
    summer_months = [6, 7, 8, 9]
    shoulder_months = [3, 4, 5, 10]
    winter_months = [11, 12, 1, 2]
    peak_hours = [16, 17, 18, 19, 20]
    pct_hours_in_summer = 2928 / 8760
    pct_hours_in_shoulder = 2952 / 8760
    pct_hours_in_winter = 2880 / 8760

    trc_costs_record = outputs_table_totals["TRC Costs ($)"]
    pac_costs_record = outputs_table_totals["PAC Costs ($)"]
    trc_record = outputs_table_totals["TRC"]
    pac_record = outputs_table_totals["PAC"]
    lifecycle_net_mwh = outputs_table_totals["Electricity Lifecycle Net Savings (MWh)"]
    lifecycle_net_therms = outputs_table_totals["Gas Lifecycle Net Savings (Therms)"]
    lifecycle_net_ghg = outputs_table_totals["Total Lifecycle GHG Savings (Tons)"]

    # Getting variables for plots
    elec_benefits_cols = (
        ["hourly_savings"] + ACC_COMPONENTS_ELECTRICITY + ["av_csts_levelized"]
    )

    elec_benefits_hour_month_year = (
        elec_benefits.groupby(["hour_of_day", "year", "month"])
        .agg(
            {
                **{component: "sum" for component in ACC_COMPONENTS_ELECTRICITY},
                **{
                    "hourly_savings": "sum",
                    "marginal_ghg": "sum",
                    "av_csts_levelized": "mean",
                },
            }
        )
        .reset_index()
    )

    total_benefits = list(
        elec_benefits_hour_month_year.groupby(["hour_of_day"])["total"].sum()
    )

    summer_benefits = list(
        elec_benefits_hour_month_year[
            (elec_benefits_hour_month_year["month"].isin(summer_months))
        ]
        .groupby(["hour_of_day"])["total"]
        .sum()
    )
    summer_peak_benefits = elec_benefits_hour_month_year["total"][
        (elec_benefits_hour_month_year["month"].isin(summer_months))
        & (elec_benefits_hour_month_year["hour_of_day"].isin(peak_hours))
    ].sum()
    shoulder_benefits = list(
        elec_benefits_hour_month_year[
            (elec_benefits_hour_month_year["month"].isin(shoulder_months))
        ]
        .groupby(["hour_of_day"])["total"]
        .sum()
    )
    winter_benefits = list(
        elec_benefits_hour_month_year[
            (elec_benefits_hour_month_year["month"].isin(winter_months))
        ]
        .groupby(["hour_of_day"])["total"]
        .sum()
    )
    total_savings = list(
        elec_benefits_hour_month_year.groupby(["hour_of_day"])["hourly_savings"].sum()
    )
    summer_savings = list(
        elec_benefits_hour_month_year[
            (elec_benefits_hour_month_year["month"].isin(summer_months))
        ]
        .groupby(["hour_of_day"])["hourly_savings"]
        .sum()
    )
    shoulder_savings = list(
        elec_benefits_hour_month_year[
            ((elec_benefits_hour_month_year["month"].isin(shoulder_months)))
        ]
        .groupby(["hour_of_day"])["hourly_savings"]
        .sum()
    )
    summer_peak_savings = elec_benefits_hour_month_year["hourly_savings"][
        (elec_benefits_hour_month_year["month"].isin(summer_months))
        & (elec_benefits_hour_month_year["hour_of_day"].isin(peak_hours))
    ].sum()
    winter_savings = list(
        elec_benefits_hour_month_year[
            (elec_benefits_hour_month_year["month"].isin(winter_months))
        ]
        .groupby(["hour_of_day"])["hourly_savings"]
        .sum()
    )
    total_av_csts_avg = list(
        elec_benefits_hour_month_year.groupby(["hour_of_day"])[
            "av_csts_levelized"
        ].mean()
    )
    summer_av_csts_avg = list(
        pct_hours_in_summer
        * elec_benefits_hour_month_year[
            (elec_benefits_hour_month_year["month"].isin(summer_months))
        ]
        .groupby(["hour_of_day"])["av_csts_levelized"]
        .mean()
    )
    summer_peak_av_csts_avg = elec_benefits_hour_month_year["av_csts_levelized"][
        (elec_benefits_hour_month_year["month"].isin(summer_months))
        & (elec_benefits_hour_month_year["hour_of_day"].isin(peak_hours))
    ].mean()
    shoulder_av_csts_avg = list(
        pct_hours_in_shoulder
        * elec_benefits_hour_month_year[
            ((elec_benefits_hour_month_year["month"].isin(shoulder_months)))
        ]
        .groupby(["hour_of_day"])["av_csts_levelized"]
        .mean()
    )
    winter_av_csts_avg = list(
        pct_hours_in_winter
        * elec_benefits_hour_month_year[
            (elec_benefits_hour_month_year["month"].isin(winter_months))
        ]
        .groupby(["hour_of_day"])["av_csts_levelized"]
        .mean()
    )

    elec_benefits_sum_by_hod = (
        elec_benefits[elec_benefits_cols].groupby(elec_benefits["hour_of_day"]).sum()
    )
    elec_benefits_hoy = (
        elec_benefits[elec_benefits_cols]
        .groupby(elec_benefits["hour_of_year"])
        .sum()
        .cumsum()
        .reset_index()
    )
    sav_avcsts_288 = (
        elec_benefits.groupby(["hour_of_day", "month"])
        .agg(
            {
                **{component: "sum" for component in ACC_COMPONENTS_ELECTRICITY},
                **{
                    "hourly_savings": "sum",
                    "marginal_ghg": "sum",
                    "av_csts_levelized": "mean",
                },
            }
        )
        .reset_index()
    )
    sav_avcsts_288 = sav_avcsts_288[
        ["hour_of_day", "month", "hourly_savings", "total", "marginal_ghg"]
    ]
    ghgsav = sav_avcsts_288.pivot("hour_of_day", "month", "marginal_ghg")
    sav = sav_avcsts_288.pivot("hour_of_day", "month", "hourly_savings")
    avcsts = sav_avcsts_288.pivot("hour_of_day", "month", "total")

    # savings load shape plot
    fig0, (ax1, ax2, ax3) = plt.subplots(
        1, 3, figsize=(18, 5), sharex=True, sharey=True
    )
    plt.subplots_adjust(wspace=0, hspace=0)
    axs = [ax1, ax2, ax3]
    hod = elec_benefits_sum_by_hod.index
    legend_labels1 = ["Summer"]
    legend_labels2 = ["Shoulder"]
    legend_labels3 = ["Winter"]

    ax1.plot(
        hod,
        summer_savings,
        c="firebrick",
        linewidth=5,
        marker="$\u25EF$",
        markersize=13,
        linestyle="-",
    )
    ax2.plot(
        hod,
        shoulder_savings,
        c="royalblue",
        linewidth=5,
        marker="$\u2206$",
        markersize=13,
        linestyle="-",
    )
    ax3.plot(
        hod,
        winter_savings,
        c="green",
        linewidth=5,
        marker="$\u25A1$",
        markersize=13,
        linestyle="-",
    )
    ax1.axhline(y=0, color="gray", linewidth=1, linestyle="--")
    ax2.axhline(y=0, color="gray", linewidth=1, linestyle="--")
    ax3.axhline(y=0, color="gray", linewidth=1, linestyle="--")
    # Shade peak region
    ax1.axvspan(16, 21, alpha=0.2, color="grey")

    leg1 = ax1.legend(legend_labels1, fontsize=14, loc="upper left", frameon=False)
    for line, text in zip(leg1.get_lines(), leg1.get_texts()):
        text.set_color(line.get_color())
    leg2 = ax2.legend(legend_labels2, fontsize=14, loc="upper left", frameon=False)
    for line, text in zip(leg2.get_lines(), leg2.get_texts()):
        text.set_color(line.get_color())
    leg3 = ax3.legend(legend_labels3, fontsize=14, loc="upper left", frameon=False)
    for line, text in zip(leg3.get_lines(), leg3.get_texts()):
        text.set_color(line.get_color())

    ax1.set_ylabel("Savings (MWh/hr)", size=16)
    ax2.set_xlabel("Hour of Day", size=16)

    if max(summer_savings + shoulder_savings + winter_savings) < 0:
        ymax = 0
    else:
        ymax = max(summer_savings + shoulder_savings + winter_savings)
    if min(summer_savings + shoulder_savings + winter_savings) > 0:
        ymin = 0
    else:
        ymin = min(summer_savings + shoulder_savings + winter_savings)

    # Tick and lebel parameters
    ax1.set_ylim(ymin * 1.08, ymax * 1.08)
    ax1.set_yticks(
        np.arange(
            ymin * 1.08,
            ymax * 1.08,
            step=max(round(ymax - ymin, 3) / 5, int((round(ymax - ymin, 0)) / 4)),
        )
    )
    ax2.set_yticks(
        np.arange(
            ymin * 1.08,
            ymax * 1.08,
            step=max(round(ymax - ymin, 3) / 5, int((round(ymax - ymin, 0)) / 4)),
        )
    )
    ax3.set_yticks(
        np.arange(
            ymin * 1.08,
            ymax * 1.08,
            step=max(round(ymax - ymin, 3) / 5, int((round(ymax - ymin, 0)) / 4)),
        )
    )
    ax1.tick_params(
        which="major", axis="y", direction="out", length=6, width=2, labelsize=14
    )
    ax2.tick_params(
        which="major", axis="y", direction="out", length=6, width=2, labelsize=14
    )
    ax3.tick_params(
        which="major", axis="y", direction="out", length=6, width=2, labelsize=14
    )
    ax1.yaxis.set_minor_locator(AutoMinorLocator())
    ax1.set_xticks(np.arange(0, 24, step=4))
    ax1.tick_params(
        which="major", axis="x", direction="out", length=7, width=2, labelsize=14
    )
    ax1.set_xlim(hod.min() - hod.max() * 0.04, hod.max() * 1.04)
    ax1.xaxis.set_minor_locator(AutoMinorLocator())
    ax2.tick_params(
        which="major", axis="x", direction="out", length=7, width=2, labelsize=14
    )
    ax2.set_xlim(hod.min() - hod.max() * 0.04, hod.max() * 1.04)
    ax2.xaxis.set_minor_locator(AutoMinorLocator())
    ax3.tick_params(
        which="major", axis="x", direction="out", length=7, width=2, labelsize=14
    )
    ax3.set_xlim(hod.min() - hod.max() * 0.04, hod.max() * 1.04)
    ax3.xaxis.set_minor_locator(AutoMinorLocator())

    # Set plot title, size, and position
    ax1.set_title("Seasonal Savings Load Shapes", size=18, loc="left").set_position(
        [0, 1.03]
    )

    # benefits_seasonal_shape_plot
    fig1, (ax1, ax2, ax3) = plt.subplots(
        1, 3, figsize=(18, 5), sharex=True, sharey=True
    )
    plt.subplots_adjust(wspace=0, hspace=0)
    axs = [ax1, ax2, ax3]
    hod = elec_benefits_sum_by_hod.index
    legend_labels1 = ["Summer"]
    legend_labels2 = ["Shoulder"]
    legend_labels3 = ["Winter"]

    ax1.plot(
        hod,
        summer_benefits,
        c="firebrick",
        linewidth=5,
        marker="$\u2B24$",
        markersize=13,
        linestyle=":",
    )
    ax2.plot(
        hod,
        shoulder_benefits,
        c="royalblue",
        linewidth=5,
        marker="$\u25B2$",
        markersize=13,
        linestyle=":",
    )
    ax3.plot(
        hod,
        winter_benefits,
        c="green",
        linewidth=5,
        marker="$\u25A0$",
        markersize=13,
        linestyle=":",
    )
    ax1.axhline(y=0, color="gray", linewidth=1, linestyle="--")
    ax2.axhline(y=0, color="gray", linewidth=1, linestyle="--")
    ax3.axhline(y=0, color="gray", linewidth=1, linestyle="--")
    # Shade peak region
    ax1.axvspan(16, 21, alpha=0.2, color="grey")

    leg1 = ax1.legend(legend_labels1, fontsize=15, loc="upper left", frameon=False)
    for line, text in zip(leg1.get_lines(), leg1.get_texts()):
        text.set_color(line.get_color())
    leg2 = ax2.legend(legend_labels2, fontsize=15, loc="upper left", frameon=False)
    for line, text in zip(leg2.get_lines(), leg2.get_texts()):
        text.set_color(line.get_color())
    leg3 = ax3.legend(legend_labels3, fontsize=15, loc="upper left", frameon=False)
    for line, text in zip(leg3.get_lines(), leg3.get_texts()):
        text.set_color(line.get_color())

    ax1.set_ylabel("TRC Benefits ($/hr)", size=16)
    ax2.set_xlabel("Hour of Day", size=16)

    if max(summer_benefits + shoulder_benefits + winter_benefits) < 0:
        ymax = 0
    else:
        ymax = max(summer_benefits + shoulder_benefits + winter_benefits)
    if min(summer_benefits + shoulder_benefits + winter_benefits) > 0:
        ymin = 0
    else:
        ymin = min(summer_benefits + shoulder_benefits + winter_benefits)

    # Tick and label parameters
    ax1.set_ylim(ymin * 1.08, ymax * 1.08)
    ax1.set_yticks(
        np.arange(
            ymin * 1.08,
            ymax * 1.08,
            step=max(round(ymax - ymin, 3) / 5, int((round(ymax - ymin, 0)) / 4)),
        )
    )
    ax2.set_yticks(
        np.arange(
            ymin * 1.08,
            ymax * 1.08,
            step=max(round(ymax - ymin, 3) / 5, int((round(ymax - ymin, 0)) / 4)),
        )
    )
    ax3.set_yticks(
        np.arange(
            ymin * 1.08,
            ymax * 1.08,
            step=max(round(ymax - ymin, 3) / 5, int((round(ymax - ymin, 0)) / 4)),
        )
    )
    ax1.tick_params(
        which="major", axis="y", direction="out", length=6, width=2, labelsize=14
    )
    ax2.tick_params(
        which="major", axis="y", direction="out", length=6, width=2, labelsize=14
    )
    ax3.tick_params(
        which="major", axis="y", direction="out", length=6, width=2, labelsize=14
    )
    ax1.yaxis.set_minor_locator(AutoMinorLocator())
    ax1.set_xticks(np.arange(0, 24, step=4))
    ax1.tick_params(
        which="major", axis="x", direction="out", length=7, width=2, labelsize=14
    )
    ax1.set_xlim(hod.min() - hod.max() * 0.04, hod.max() * 1.04)
    ax1.xaxis.set_minor_locator(AutoMinorLocator())
    ax2.tick_params(
        which="major", axis="x", direction="out", length=7, width=2, labelsize=14
    )
    ax2.set_xlim(hod.min() - hod.max() * 0.04, hod.max() * 1.04)
    ax2.xaxis.set_minor_locator(AutoMinorLocator())
    ax3.tick_params(
        which="major", axis="x", direction="out", length=7, width=2, labelsize=14
    )
    ax3.set_xlim(hod.min() - hod.max() * 0.04, hod.max() * 1.04)
    ax3.xaxis.set_minor_locator(AutoMinorLocator())

    # Set plot title, size, and position
    ax1.set_title(
        "Seasonal TRC Benefits by Hour ($)", size=18, loc="left"
    ).set_position([0, 1.03])

    # sum_hourly_plot
    fig2 = plt.figure(figsize=(12, 7), dpi=250)
    ax = fig2.gca()
    colors = [
        "royalblue",
        "black",
        "pink",
        "firebrick",
        "gray",
        "darkviolet",
        "darkorange",
        "green",
        "saddlebrown",
    ]
    legend_labels = []
    x = 1
    while x <= len(ACC_COMPONENTS_ELECTRICITY[1:]):
        if x == 1:
            ax.bar(
                hod,
                elec_benefits_sum_by_hod[ACC_COMPONENTS_ELECTRICITY[x]],
                color=colors[x - 1],
            )
            legend_labels.append(
                re.findall(
                    ".*Name: (.*),",
                    str(elec_benefits_sum_by_hod[ACC_COMPONENTS_ELECTRICITY[x]]),
                )[0]
            )
            x += 1
        else:
            ax.bar(
                hod,
                elec_benefits_sum_by_hod[ACC_COMPONENTS_ELECTRICITY[x]],
                bottom=elec_benefits_sum_by_hod.iloc[:, 2 : x + 1].sum(axis=1),
                color=colors[x - 1],
            )
            legend_labels.append(
                re.findall(
                    ".*Name: (.*),",
                    str(elec_benefits_sum_by_hod[ACC_COMPONENTS_ELECTRICITY[x]]),
                )[0]
            )
            x += 1

    # Set x and y limits based on min and max values
    ymax = elec_benefits_sum_by_hod.iloc[:, 2:x].sum(axis=1).max()
    if elec_benefits_sum_by_hod.iloc[:, 2:x].sum(axis=1).min() > 0:
        ymin = 0
    else:
        ymin = elec_benefits_sum_by_hod.iloc[:, 2:x].sum(axis=1).min()

    ax.set_xlim(hod.min() - hod.max() * 0.04, hod.max() * 1.04)
    ax.set_ylim(ymin * 1.1, ymax * 1.08)

    # Set x and y axis labels
    ax.set_xlabel("Hour of Day", size=17, labelpad=5)
    ax.set_ylabel("$ Avoided Costs", size=17)

    # Set plot title, size, and position
    ax.set_title(
        "Sum of Electric Avoided Costs by Component and Hour of Day",
        size=17,
        loc="left",
    )

    # Tick and lebel parameters
    ax.tick_params(bottom=True, top=False, left=True, right=False)
    ax.set_xticks(np.arange(0, 24, step=4))
    ax.set_yticks(
        np.arange(
            int(round(ymin * 1.1, 0)),
            ymax * 1.08,
            step=max(round(ymax - ymin, 2) / 5, int((round(ymax - ymin, 0)) / 4)),
        )
    )
    ax.tick_params(
        which="major", axis="x", direction="out", length=6, width=2, labelsize=14
    )
    ax.tick_params(
        which="major", axis="y", direction="out", length=6, width=2, labelsize=14
    )

    # Minor ticks
    ax.xaxis.set_minor_locator(AutoMinorLocator())
    ax.yaxis.set_minor_locator(AutoMinorLocator())

    # Legend
    plt.legend(
        legend_labels,
        bbox_to_anchor=(1, 1),
        fontsize=12,
        loc="upper left",
        frameon=False,
    )

    # avoided_cost_summary_plot
    fig3, (ax1, ax2, ax3) = plt.subplots(
        3, 1, figsize=(6, 10), sharex=True, sharey=False
    )
    axs = [ax1, ax2, ax3]
    hod = elec_benefits_sum_by_hod.index
    legend_labels = ["Total", "Summer", "Shoulder", "Winter"]

    ax1.plot(
        hod,
        total_benefits,
        c="royalblue",
        marker="$\u25EF$",
        markersize=10,
        linewidth=3,
        linestyle="-",
    )
    ax1.plot(hod, summer_benefits, c="darkorchid", linewidth=1, linestyle="--")
    ax1.plot(hod, shoulder_benefits, c="olivedrab", linewidth=1, linestyle=":")
    ax1.plot(hod, winter_benefits, c="teal", linewidth=1, linestyle="-")
    ax2.plot(
        hod,
        total_savings,
        c="firebrick",
        marker="$\u2206$",
        markersize=10,
        linewidth=3,
        linestyle="-",
    )
    ax2.plot(hod, summer_savings, c="darkorchid", linewidth=1, linestyle="--")
    ax2.plot(hod, shoulder_savings, c="olivedrab", linewidth=1, linestyle=":")
    ax2.plot(hod, winter_savings, c="teal", linewidth=1, linestyle="-")
    ax3.plot(
        hod,
        total_av_csts_avg,
        c="green",
        marker="$\u25A0$",
        markersize=10,
        linewidth=3,
        linestyle="-",
    )
    ax3.plot(hod, summer_av_csts_avg, c="darkorchid", linewidth=1, linestyle="--")
    ax3.plot(hod, shoulder_av_csts_avg, c="olivedrab", linewidth=1, linestyle=":")
    ax3.plot(hod, winter_av_csts_avg, c="teal", linewidth=1, linestyle="-")

    leg1 = ax1.legend(legend_labels, fontsize=11, loc="upper left", frameon=False)
    for line, text in zip(leg1.get_lines(), leg1.get_texts()):
        text.set_color(line.get_color())
    leg2 = ax2.legend(legend_labels, fontsize=11, loc="upper left", frameon=False)
    for line, text in zip(leg2.get_lines(), leg2.get_texts()):
        text.set_color(line.get_color())
    leg3 = ax3.legend(legend_labels, fontsize=11, loc="upper left", frameon=False)
    for line, text in zip(leg3.get_lines(), leg3.get_texts()):
        text.set_color(line.get_color())

    ax3.set_xticks(np.arange(0, 24, step=4))
    ax3.set_xlabel("Hour of Day", size=14, labelpad=5)
    ax3.tick_params(
        which="major", axis="x", direction="out", length=6, width=2, labelsize=12
    )
    ax3.set_xlim(hod.min() - hod.max() * 0.04, hod.max() * 1.04)
    ax3.xaxis.set_minor_locator(AutoMinorLocator())

    ax1.set_ylabel("TRC Benefits ($)", size=14)
    ax2.set_ylabel("Savings (MWh)", size=14)
    ax3.set_ylabel("Av. Cost ($/MWh)", size=14)

    if max(total_benefits + summer_benefits + shoulder_benefits + winter_benefits) < 0:
        ymax1 = 0
    else:
        ymax1 = max(
            total_benefits + summer_benefits + shoulder_benefits + winter_benefits
        )
    if min(total_benefits + summer_benefits + shoulder_benefits + winter_benefits) > 0:
        ymin1 = 0
    else:
        ymin1 = min(
            total_benefits + summer_benefits + shoulder_benefits + winter_benefits
        )
    if max(total_savings + summer_savings + shoulder_savings + winter_savings) < 0:
        ymax2 = 0
    else:
        ymax2 = max(total_savings + summer_savings + shoulder_savings + winter_savings)
    if min(total_savings + summer_savings + shoulder_savings + winter_savings) > 0:
        ymin2 = 0
    else:
        ymin2 = min(total_savings + summer_savings + shoulder_savings + winter_savings)
    if (
        max(
            total_av_csts_avg
            + summer_av_csts_avg
            + shoulder_av_csts_avg
            + winter_av_csts_avg
        )
        < 0
    ):
        ymax3 = 0
    else:
        ymax3 = max(
            total_av_csts_avg
            + summer_av_csts_avg
            + shoulder_av_csts_avg
            + winter_av_csts_avg
        )
    if (
        min(
            total_av_csts_avg
            + summer_av_csts_avg
            + shoulder_av_csts_avg
            + winter_av_csts_avg
        )
        > 0
    ):
        ymin3 = 0
    else:
        ymin3 = min(
            total_av_csts_avg
            + summer_av_csts_avg
            + shoulder_av_csts_avg
            + winter_av_csts_avg
        )

    # Tick and lebel parameters
    ax1.set_ylim(ymin1 * 1.08, ymax1 * 1.08)
    ax2.set_ylim(ymin2 * 1.08, ymax2 * 1.08)
    ax3.set_ylim(ymin3 * 1.08, ymax3 * 1.08)

    ax1.set_yticks(
        np.arange(
            ymin1 * 1.08,
            ymax1 * 1.08,
            step=max(round(ymax1 - ymin1, 3) / 5, int((round(ymax1 - ymin1, 0)) / 4)),
        )
    )
    ax2.set_yticks(
        np.arange(
            ymin2 * 1.08,
            ymax2 * 1.08,
            step=max(round(ymax2 - ymin2, 3) / 5, int((round(ymax2 - ymin2, 0)) / 4)),
        )
    )
    ax3.set_yticks(
        np.arange(
            ymin3 * 1.08,
            ymax3 * 1.08,
            step=max(round(ymax3 - ymin3, 3) / 5, int((round(ymax3 - ymin3, 0)) / 4)),
        )
    )

    ax1.tick_params(
        which="major", axis="y", direction="out", length=6, width=2, labelsize=12
    )
    ax2.tick_params(
        which="major", axis="y", direction="out", length=6, width=2, labelsize=12
    )
    ax3.tick_params(
        which="major", axis="y", direction="out", length=6, width=2, labelsize=12
    )

    # Shade peak region
    ax1.axvspan(16, 21, alpha=0.2, color="grey")
    ax2.axvspan(16, 21, alpha=0.2, color="grey")
    ax3.axvspan(16, 21, alpha=0.2, color="grey")

    # Print key information
    plt.annotate(
        "Electric Benefits = $" + str(round(elec_benefits["total"].sum(), 2)),
        xy=(350, 530),
        xycoords="axes points",
        fontsize=18,
    )
    plt.annotate(
        "Gas Benefits = $" + str(round(gas_benefits, 2)),
        xy=(350, 505),
        xycoords="axes points",
        fontsize=18,
    )
    plt.annotate(
        "Total Benefits = $"
        + str(round(elec_benefits["total"].sum() + gas_benefits, 2)),
        xy=(350, 480),
        xycoords="axes points",
        fontsize=18,
    )
    plt.annotate(
        "TRC Costs = $" + str(trc_costs_record),
        xy=(350, 455),
        xycoords="axes points",
        fontsize=18,
    )
    plt.annotate(
        "PAC Costs = $" + str(pac_costs_record),
        xy=(350, 430),
        xycoords="axes points",
        fontsize=18,
    )
    plt.annotate(
        "TRC = " + str(trc_record), xy=(350, 405), xycoords="axes points", fontsize=18
    )
    plt.annotate(
        "PAC = " + str(pac_record), xy=(350, 380), xycoords="axes points", fontsize=18
    )
    plt.annotate(
        "Net Lifecycle Electric Savings = " + str(lifecycle_net_mwh) + " MWh",
        xy=(350, 335),
        xycoords="axes points",
        fontsize=18,
    )
    plt.annotate(
        "Net Lifecycle Gas Savings = " + str(lifecycle_net_therms) + " Therms",
        xy=(350, 310),
        xycoords="axes points",
        fontsize=18,
    )
    plt.annotate(
        "Net Lifecycle GHG Savings = " + str(lifecycle_net_ghg) + " Tons",
        xy=(350, 285),
        xycoords="axes points",
        fontsize=18,
    )
    plt.annotate(
        str(round(100 * ((summer_peak_savings) / sum(total_savings)), 1))
        + "% MWh savings during summer peak period",
        xy=(350, 260),
        xycoords="axes points",
        fontsize=18,
    )
    plt.annotate(
        str(round(100 * ((summer_peak_benefits) / sum(total_benefits)), 1))
        + "% Electric TRC benefits from summer peak period",
        xy=(350, 235),
        xycoords="axes points",
        fontsize=18,
    )
    plt.annotate(
        "Electric Benefits per MWh = $"
        + str(round(elec_benefits["total"].sum() / lifecycle_net_mwh, 2)),
        xy=(350, 210),
        xycoords="axes points",
        fontsize=18,
    )
    plt.annotate(
        "Typical Avoided Cost per MWh = $"
        + str(round(elec_benefits["av_csts_levelized"].mean(), 2)),
        xy=(350, 145),
        xycoords="axes points",
        fontsize=18,
    )

    # Set plot title, size, and position
    ax1.set_title(
        "Savings and Avoided Cost Profiles", size=16, loc="left"
    ).set_position([0, 1.03])

    # marginal_ghg_savings_plot
    cmp = sns.diverging_palette(16, 260, l=35, n=25, as_cmap=True)

    fig4 = plt.figure(figsize=(8, 6), dpi=100)
    ax1 = fig4.gca()
    y_ticks = [
        0,
        "",
        2,
        "",
        4,
        "",
        6,
        "",
        8,
        "",
        10,
        "",
        12,
        "",
        14,
        "",
        16,
        "",
        18,
        "",
        20,
        "",
        22,
    ]
    hmp = sns.heatmap(ghgsav, cmap=cmp, ax=ax1, yticklabels=y_ticks, center=0.00)
    ax1.set_xlabel("Month", size=15)
    ax1.set_ylabel("Hour of Day", size=15)
    ax1.tick_params(
        which="major", axis="x", direction="out", length=6, width=2, labelsize=13
    )
    ax1.tick_params(
        which="major",
        axis="y",
        direction="out",
        length=6,
        width=2,
        labelsize=13,
        rotation=0,
    )
    ax1.set_title("Electric GHG Savings by Month and Hour", size=15, loc="left", pad=8)
    cbar1 = hmp.collections[0].colorbar
    cbar1.ax.tick_params(labelsize=14)
    plt.annotate("Sum GHG", xy=(370, 352), xycoords="axes points", fontsize=12)
    plt.annotate("Savings (Tons)", xy=(370, 336), xycoords="axes points", fontsize=12)

    # month_hour_savings_benefits_plot
    fig5, (ax1, ax2) = plt.subplots(1, 2, figsize=(21, 10), dpi=200)
    y_ticks = [
        0,
        "",
        2,
        "",
        4,
        "",
        6,
        "",
        8,
        "",
        10,
        "",
        12,
        "",
        14,
        "",
        16,
        "",
        18,
        "",
        20,
        "",
        22,
    ]
    fleft = sns.heatmap(sav, cmap=cmp, ax=ax1, yticklabels=y_ticks, center=0.00)
    fright = sns.heatmap(avcsts, cmap=cmp, ax=ax2, yticklabels=y_ticks, center=0.00)
    ax1.set_xlabel("Month", size=22)
    ax1.set_ylabel("Hour of Day", size=22)
    ax2.set_xlabel("Month", size=22)
    ax2.set_ylabel("Hour of Day", size=22)
    ax1.tick_params(
        which="major", axis="x", direction="out", length=6, width=2, labelsize=18
    )
    ax1.tick_params(
        which="major",
        axis="y",
        direction="out",
        length=6,
        width=2,
        labelsize=18,
        rotation=0,
    )
    ax2.tick_params(
        which="major", axis="x", direction="out", length=6, width=2, labelsize=18
    )
    ax2.tick_params(
        which="major",
        axis="y",
        direction="out",
        length=6,
        width=2,
        labelsize=18,
        rotation=0,
    )
    ax1.set_title(
        "MWh Savings by Month and Hour", size=24, loc="left", pad=15
    ).set_position([0, 1.1])
    ax2.set_title("$ Benefits by Month and Hour", size=24, loc="left", pad=15)
    fig4.tight_layout(pad=2.0)
    cbar1 = fleft.collections[0].colorbar
    cbar1.ax.tick_params(labelsize=18)
    cbar2 = fright.collections[0].colorbar
    cbar2.ax.tick_params(labelsize=18)
    plt.annotate("Sum MWh", xy=(-200, 585), xycoords="axes points", fontsize=20)
    plt.annotate("Savings", xy=(-193, 560), xycoords="axes points", fontsize=20)
    plt.annotate("Sum TRC", xy=(435, 585), xycoords="axes points", fontsize=20)
    plt.annotate("Benefits", xy=(442, 560), xycoords="axes points", fontsize=20)

    # savings_benefits_cumulative_sum_plot
    fig6 = plt.figure(figsize=(12, 7), dpi=250)
    ax1 = fig6.gca()
    ax1.plot(
        elec_benefits_hoy["hour_of_year"],
        elec_benefits_hoy["hourly_savings"],
        color="royalblue",
        linewidth=3,
    )
    ax2 = ax1.twinx()
    ax2.plot(
        elec_benefits_hoy["hour_of_year"],
        elec_benefits_hoy["total"],
        color="firebrick",
        linewidth=3,
        linestyle="--",
    )
    ax2.axhline(y=0, color="gray", linewidth=0.7, linestyle="--")

    # Set x and y limits based on min and max values

    if (
        elec_benefits_hoy["hourly_savings"].max() >= 0
        and elec_benefits_hoy["total"].max() >= 0
    ):
        ymax1 = elec_benefits_hoy["hourly_savings"].max()
        ymax2 = elec_benefits_hoy["total"].max()
    elif (
        elec_benefits_hoy["hourly_savings"].max() < 0
        and elec_benefits_hoy["total"].max() < 0
    ):
        ymax1 = 0
        ymax2 = 0
    elif (
        elec_benefits_hoy["hourly_savings"].max() < 0
        and elec_benefits_hoy["total"].max() > 0
    ):
        ymax1 = (
            -1
            * elec_benefits_hoy["hourly_savings"].min()
            * (
                elec_benefits_hoy["total"].max()
                / (elec_benefits_hoy["total"].max() - elec_benefits_hoy["total"].min())
            )
            / (
                1
                - elec_benefits_hoy["total"].max()
                / (elec_benefits_hoy["total"].max() - elec_benefits_hoy["total"].min())
            )
        )
        ymax2 = elec_benefits_hoy["total"].max()
    else:
        ymax1 = 0
        ymax2 = (
            -1
            * elec_benefits_hoy["total"].min()
            * (
                elec_benefits_hoy["hourly_savings"].max()
                / (
                    elec_benefits_hoy["hourly_savings"].max()
                    - elec_benefits_hoy["hourly_savings"].min()
                )
            )
        )

    if (
        elec_benefits_hoy["hourly_savings"].min() <= 0
        and elec_benefits_hoy["total"].min() <= 0
    ):
        ymin1 = elec_benefits_hoy["hourly_savings"].min()
        ymin2 = elec_benefits_hoy["total"].min()
    elif (
        elec_benefits_hoy["hourly_savings"].min() > 0
        and elec_benefits_hoy["total"].min() > 0
    ):
        ymin1 = 0
        ymin2 = 0
    elif (
        elec_benefits_hoy["hourly_savings"].min() > 0
        and elec_benefits_hoy["total"].min() < 0
    ):
        ymin1 = (
            -1
            * elec_benefits_hoy["hourly_savings"].max()
            * (
                elec_benefits_hoy["total"].min()
                / (elec_benefits_hoy["total"].min() - elec_benefits_hoy["total"].max())
            )
            / (
                1
                - elec_benefits_hoy["total"].min()
                / (elec_benefits_hoy["total"].min() - elec_benefits_hoy["total"].max())
            )
        )
        ymin2 = elec_benefits_hoy["total"].min()
    else:
        ymin1 = 0
        ymin2 = (
            -1
            * elec_benefits_hoy["total"].min()
            * (
                elec_benefits_hoy["hourly_savings"].min()
                / (
                    elec_benefits_hoy["hourly_savings"].min()
                    - elec_benefits_hoy["hourly_savings"].min()
                )
            )
        )

    # Set x and y axis limits
    ax1.set_xlim(-340, 9000)
    ax1.set_ylim(ymin1 * 1.08, ymax1 * 1.08)
    ax2.set_ylim(ymin2 * 1.08, ymax2 * 1.08)

    # Set x and y axis labels
    ax1.set_xlabel("Hour of Year", size=17, labelpad=5)
    ax1.set_ylabel("Net Lifecycle Savings (MWh)", size=17)
    ax2.set_ylabel("$ TRC Benefits", size=17, rotation=-90, labelpad=20)

    # Set plot title, size, and position
    ax1.set_title(
        "Cumulative Savings and TRC Benefits by Hour of Year",
        size=17,
        loc="left",
        pad=8,
    )

    # Tick and lebel parameters
    ax1.set_xticks(np.arange(0, 8760, step=1000))
    ax1.set_yticks(
        np.arange(
            int(round(ymin1 * 1.1, 0)),
            ymax1 * 1.08,
            step=max(round(ymax1 - ymin1, 2) / 5, int((round(ymax1 - ymin1, 0)) / 4)),
        )
    )
    ax1.tick_params(
        which="major", axis="x", direction="out", length=6, width=2, labelsize=14
    )
    ax1.tick_params(
        which="major", axis="y", direction="out", length=6, width=2, labelsize=14
    )

    ax2.set_xticks(np.arange(0, 8760, step=1000))
    ax2.set_yticks(
        np.arange(
            int(round(ymin2 * 1.1, 0)),
            ymax2 * 1.08,
            step=max(round(ymax2 - ymin2, 2) / 5, int((round(ymax2 - ymin2, 0)) / 4)),
        )
    )
    ax2.tick_params(
        which="major", axis="x", direction="out", length=6, width=2, labelsize=14
    )
    ax2.tick_params(
        which="major", axis="y", direction="out", length=6, width=2, labelsize=14
    )

    # Minor ticks
    ax1.xaxis.set_minor_locator(AutoMinorLocator())
    ax1.yaxis.set_minor_locator(AutoMinorLocator())
    ax2.yaxis.set_minor_locator(AutoMinorLocator())

    # Legend
    ax1.legend(
        ["Savings"],
        fontsize=12,
        bbox_to_anchor=(0.02, 1),
        loc="upper left",
        frameon=False,
    )
    ax2.legend(
        ["TRC Beneftis"],
        fontsize=12,
        bbox_to_anchor=(0.02, 0.95),
        loc="upper left",
        frameon=False,
    )

    fig7 = plt.figure(figsize=(12, 7), dpi=250)
    ax = fig7.gca()
    colors1 = [
        "black",
        "royalblue",
        "black",
        "pink",
        "firebrick",
        "gray",
        "darkviolet",
        "darkorange",
        "green",
        "saddlebrown",
    ]
    legend_labels2 = []

    ax.plot(
        elec_benefits_hoy["hour_of_year"],
        elec_benefits_hoy[ACC_COMPONENTS_ELECTRICITY[0]],
        color=colors1[0],
        linewidth=3,
    )
    legend_labels2.append(ACC_COMPONENTS_ELECTRICITY[0])
    x = 1
    while x <= len(ACC_COMPONENTS_ELECTRICITY) - 2:
        ax.plot(
            elec_benefits_hoy["hour_of_year"],
            elec_benefits_hoy[ACC_COMPONENTS_ELECTRICITY[x]],
            color=colors1[x],
        )
        legend_labels2.append(ACC_COMPONENTS_ELECTRICITY[x])
        x += 1

    # Set x and y limits based on min and max values
    if max(elec_benefits_hoy.iloc[:, 2:x].max()) < 0:
        ymax = 0
    else:
        ymax = max(elec_benefits_hoy.iloc[:, 2:x].max())
    if min(elec_benefits_hoy.iloc[:, 2:x].min()) > 0:
        ymin = 0
    else:
        ymin = min(elec_benefits_hoy.iloc[:, 2:x].min())

    ax.set_xlim(-340, 9000)
    ax.set_ylim(ymin * 1.1, ymax * 1.08)

    # Set x and y axis labels
    ax.set_xlabel("Hour of Year", size=17, labelpad=5)
    ax.set_ylabel("$ TRC Benefits", size=17)

    # Set plot title, size, and position
    ax.set_title(
        "Sum of Avoided Costs by Component and Hour of Day", size=17, loc="left"
    )

    # Tick and lebel parameters
    ax.set_xticks(np.arange(0, 8760, step=1000))
    ax.set_yticks(
        np.arange(
            int(round(ymin * 1.1, 0)),
            ymax * 1.08,
            step=max(round(ymax - ymin, 3) / 5, int((round(ymax - ymin, 0)) / 4)),
        )
    )
    ax.tick_params(
        which="major", axis="x", direction="out", length=6, width=2, labelsize=14
    )
    ax.tick_params(
        which="major", axis="y", direction="out", length=6, width=2, labelsize=14
    )

    # Minor ticks
    ax.xaxis.set_minor_locator(AutoMinorLocator())
    ax.yaxis.set_minor_locator(AutoMinorLocator())

    # Legend
    plt.legend(
        legend_labels2,
        bbox_to_anchor=(1, 1),
        fontsize=12,
        loc="upper left",
        frameon=False,
    )
