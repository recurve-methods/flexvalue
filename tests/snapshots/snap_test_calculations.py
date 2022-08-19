# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import GenericRepr, Snapshot


snapshots = Snapshot()

snapshots['test_electric_benefits_full_outputs elec_ben'] = GenericRepr("      identifier  hour_of_day  ...  ghg_adder_rebalancing  methane_leakage
0           id_0            0  ...               0.137734         0.041598
1           id_0            0  ...               0.155136         0.046854
2           id_0            0  ...               0.132449         0.040002
3           id_0            0  ...               0.123463         0.037288
4           id_0            0  ...               0.165034         0.049844
...          ...          ...  ...                    ...              ...
10075  DEER_LS_2           23  ...               0.964215         0.291212
10076  DEER_LS_2           23  ...               1.068513         0.322712
10077  DEER_LS_2           23  ...               1.029674         0.310982
10078  DEER_LS_2           23  ...               0.766763         0.231578
10079  DEER_LS_2           23  ...               0.971170         0.293313

[10080 rows x 17 columns]")

snapshots['test_time_series_outputs time_series_elec_outputs'] = GenericRepr("        hour_of_year      ID_0  hourly_savings  ...  ID_4 DEER_LS_1 DEER_LS_2
0                  0  0.084442        0.084442  ...   NaN       NaN       NaN
1                  1  0.075795        0.075795  ...   NaN       NaN       NaN
2                  2  0.042057        0.042057  ...   NaN       NaN       NaN
3                  3  0.025892        0.025892  ...   NaN       NaN       NaN
4                  4  0.051127        0.051127  ...   NaN       NaN       NaN
...              ...       ...             ...  ...   ...       ...       ...
306595          8755       NaN        0.677359  ...   NaN       NaN  0.677359
306596          8756       NaN        0.989559  ...   NaN       NaN  0.989559
306597          8757       NaN        0.725121  ...   NaN       NaN  0.725121
306598          8758       NaN        0.069761  ...   NaN       NaN  0.069761
306599          8759       NaN        0.860945  ...   NaN       NaN  0.860945

[306600 rows x 30 columns]")

snapshots['test_time_series_outputs time_series_gas_outputs'] = GenericRepr("     year  quarter  index  ...  discount  av_csts_levelized  identifier
0    2021      1.0   32.0  ...  1.000000           0.721540        id_0
1    2021      2.0  125.0  ...  0.981210           0.707982        id_0
2    2021      3.0  218.0  ...  0.962773           0.694679        id_0
3    2021      4.0  311.0  ...  0.944682           0.681626        id_0
4    2022      5.0   33.0  ...  0.926931           0.668818        id_0
..    ...      ...    ...  ...       ...                ...         ...
135  2024     16.0  314.0  ...  0.752365           0.542861   DEER_LS_2
136  2025     17.0   36.0  ...  0.738228           0.532661   DEER_LS_2
137  2025     18.0  129.0  ...  0.724356           0.522652   DEER_LS_2
138  2025     19.0  222.0  ...  0.710745           0.512831   DEER_LS_2
139  2025     20.0  315.0  ...  0.697390           0.503195   DEER_LS_2

[140 rows x 13 columns]")

snapshots['test_user_inputs_basic df_output_table_totals'] = GenericRepr("TRC                                             1.048
PAC                                             1.999
TRC (and PAC) Electric Benefits ($)          6167.700
TRC (and PAC) Gas Benefits ($)               8959.930
TRC (and PAC) Total Benefits ($)            15127.600
TRC Costs ($)                               14436.940
PAC Costs ($)                                7568.470
Electricity First Year Net Savings (MWh)    10928.246
Electricity Lifecycle Net Savings (MWh)     54641.233
Gas First Year Net Savings (Therms)          2800.000
Gas Lifecycle Net Savings (Therms)          14000.000
Electricity Lifecycle GHG Savings (Tons)    45667.236
Gas Lifecycle GHG Savings (Tons)               84.000
Total Lifecycle GHG Savings (Tons)          45751.236
dtype: float64")

snapshots['test_user_inputs_from_example_metered df_output_table_totals'] = GenericRepr("TRC                                             1.048
PAC                                             1.999
TRC (and PAC) Electric Benefits ($)          6167.700
TRC (and PAC) Gas Benefits ($)               8959.930
TRC (and PAC) Total Benefits ($)            15127.600
TRC Costs ($)                               14436.940
PAC Costs ($)                                7568.470
Electricity First Year Net Savings (MWh)    10928.246
Electricity Lifecycle Net Savings (MWh)     54641.233
Gas First Year Net Savings (Therms)          2800.000
Gas Lifecycle Net Savings (Therms)          14000.000
Electricity Lifecycle GHG Savings (Tons)    45667.236
Gas Lifecycle GHG Savings (Tons)               84.000
Total Lifecycle GHG Savings (Tons)          45751.236
dtype: float64")

snapshots['test_user_inputs_full df_output_table_totals'] = GenericRepr("TRC                                             1.048
PAC                                             1.999
TRC (and PAC) Electric Benefits ($)          6167.700
TRC (and PAC) Gas Benefits ($)               8959.930
TRC (and PAC) Total Benefits ($)            15127.600
TRC Costs ($)                               14436.940
PAC Costs ($)                                7568.470
Electricity First Year Net Savings (MWh)    10928.246
Electricity Lifecycle Net Savings (MWh)     54641.233
Gas First Year Net Savings (Therms)          2800.000
Gas Lifecycle Net Savings (Therms)          14000.000
Electricity Lifecycle GHG Savings (Tons)    45667.236
Gas Lifecycle GHG Savings (Tons)               84.000
Total Lifecycle GHG Savings (Tons)          45751.236
dtype: float64")

snapshots['test_user_inputs_single_row df_output_table_totals'] = GenericRepr("TRC                                             1.809
PAC                                             3.450
TRC (and PAC) Electric Benefits ($)          2450.190
TRC (and PAC) Gas Benefits ($)               1279.990
TRC (and PAC) Total Benefits ($)             3730.180
TRC Costs ($)                                2062.420
PAC Costs ($)                                1081.210
Electricity First Year Net Savings (MWh)     4341.309
Electricity Lifecycle Net Savings (MWh)     21706.547
Gas First Year Net Savings (Therms)           400.000
Gas Lifecycle Net Savings (Therms)           2000.000
Electricity Lifecycle GHG Savings (Tons)    18141.574
Gas Lifecycle GHG Savings (Tons)               12.000
Total Lifecycle GHG Savings (Tons)          18153.574
dtype: float64")
