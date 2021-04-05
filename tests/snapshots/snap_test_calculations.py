# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import GenericRepr, Snapshot


snapshots = Snapshot()

snapshots['test_electric_benefits_full_outputs elec_ben'] = GenericRepr('                             identifier  ...  methane_leakage\n0                                     0  ...      2440.456961\n1                                     0  ...      2614.282898\n2                                     0  ...      2114.712496\n3                                     0  ...      1617.374298\n4                                     0  ...      2150.869447\n...                                 ...  ...              ...\n23323  Res_RefgFrzr_Recyc_UnConditioned  ...        10.331944\n23324  Res_RefgFrzr_Recyc_UnConditioned  ...        10.269918\n23325  Res_RefgFrzr_Recyc_UnConditioned  ...         8.232499\n23326  Res_RefgFrzr_Recyc_UnConditioned  ...         6.382734\n23327  Res_RefgFrzr_Recyc_UnConditioned  ...         5.616405\n\n[23328 rows x 17 columns]')

snapshots['test_user_inputs_basic df_output_table'] = GenericRepr('Outputs                                 TRC  ...  Lifecycle Total GHG Savings (Tons)\n0                                 10215.860  ...                          464320.275\n1                                  5418.946  ...                          451182.163\n2                                  3817.229  ...                          462446.939\n3                                  2939.286  ...                          469590.918\n4                                  2279.498  ...                          450237.801\nRes_Indoor_CFL_Ltg                   28.584  ...                            1199.112\nRes_RefgFrzr_HighEff                 25.443  ...                            1116.635\nRes_RefgFrzr_Recyc_Conditioned       25.443  ...                            1116.635\nRes_RefgFrzr_Recyc_UnConditioned     25.620  ...                            1111.443\nTotals                             2922.679  ...                         2302321.921\n\n[10 rows x 14 columns]')

snapshots['test_user_inputs_basic df_output_table_totals'] = GenericRepr('TRC                                         2.922679e+03\nPAC                                         5.186288e+03\nTRC (and PAC) Electric Benefits ($)         6.861880e+08\nTRC (and PAC) Gas Benefits ($)              7.950359e+04\nTRC (and PAC) Total Benefits ($)            6.862675e+08\nTRC Costs ($)                               2.348077e+05\nPAC Costs ($)                               1.323234e+05\nElectricity First Year Net Savings (MWh)    9.086439e+05\nElectricity Lifecycle Net Savings (MWh)     8.177795e+06\nGas First Year Net Savings (Therms)         9.329000e+03\nGas Lifecycle Net Savings (Therms)          8.396100e+04\nElectricity Lifecycle GHG Savings (Tons)    2.301818e+06\nGas Lifecycle GHG Savings (Tons)            5.037660e+02\nTotal Lifecycle GHG Savings (Tons)          2.302322e+06\ndtype: float64')

snapshots['test_user_inputs_from_example_metered df_output_table'] = GenericRepr('Outputs        TRC  ...  Lifecycle Total GHG Savings (Tons)\nid_0     11300.838  ...                          464274.105\nid_1      5596.145  ...                          451146.253\nid_2      3848.054  ...                          462308.429\nid_3      2927.549  ...                          469611.438\nid_4      2198.086  ...                          445386.153\nTotals    3782.528  ...                         2292726.378\n\n[6 rows x 14 columns]')

snapshots['test_user_inputs_from_example_metered df_output_table_totals'] = GenericRepr('TRC                                            2.465\nPAC                                            4.703\nTRC (and PAC) Electric Benefits ($)         -626.290\nTRC (and PAC) Gas Benefits ($)              5711.070\nTRC (and PAC) Total Benefits ($)            5084.780\nTRC Costs ($)                               2062.420\nPAC Costs ($)                               1081.210\nElectricity First Year Net Savings (MWh)      -0.932\nElectricity Lifecycle Net Savings (MWh)      -13.984\nGas First Year Net Savings (Therms)          400.000\nGas Lifecycle Net Savings (Therms)          6000.000\nElectricity Lifecycle GHG Savings (Tons)      -4.935\nGas Lifecycle GHG Savings (Tons)              36.000\nTotal Lifecycle GHG Savings (Tons)            31.065\ndtype: float64')

snapshots['test_user_inputs_full df_output_table'] = GenericRepr('Outputs                                 TRC  ...  Lifecycle Total GHG Savings (Tons)\n0                                 10215.860  ...                          464320.275\n1                                  5418.946  ...                          451182.163\n2                                  3817.229  ...                          462446.939\n3                                  2939.286  ...                          469590.918\n4                                  2279.498  ...                          450237.801\nRes_Indoor_CFL_Ltg                   28.584  ...                            1199.112\nRes_RefgFrzr_HighEff                 25.443  ...                            1116.635\nRes_RefgFrzr_Recyc_Conditioned       25.443  ...                            1116.635\nRes_RefgFrzr_Recyc_UnConditioned     25.620  ...                            1111.443\nTotals                             2922.679  ...                         2302321.921\n\n[10 rows x 14 columns]')

snapshots['test_user_inputs_full df_output_table_totals'] = GenericRepr('TRC                                         2.922679e+03\nPAC                                         5.186288e+03\nTRC (and PAC) Electric Benefits ($)         6.861880e+08\nTRC (and PAC) Gas Benefits ($)              7.950359e+04\nTRC (and PAC) Total Benefits ($)            6.862675e+08\nTRC Costs ($)                               2.348077e+05\nPAC Costs ($)                               1.323234e+05\nElectricity First Year Net Savings (MWh)    9.086439e+05\nElectricity Lifecycle Net Savings (MWh)     8.177795e+06\nGas First Year Net Savings (Therms)         9.329000e+03\nGas Lifecycle Net Savings (Therms)          8.396100e+04\nElectricity Lifecycle GHG Savings (Tons)    2.301818e+06\nGas Lifecycle GHG Savings (Tons)            5.037660e+02\nTotal Lifecycle GHG Savings (Tons)          2.302322e+06\ndtype: float64')

snapshots['test_user_inputs_single_row df_output_table'] = GenericRepr('Outputs                             TRC  ...  Lifecycle Total GHG Savings (Tons)\nRes_RefgFrzr_Recyc_UnConditioned  25.62  ...                            1111.443\nTotals                            25.62  ...                            1111.443\n\n[2 rows x 14 columns]')

snapshots['test_user_inputs_single_row df_output_table_totals'] = GenericRepr('TRC                                             25.620\nPAC                                             27.775\nTRC (and PAC) Electric Benefits ($)         332420.390\nTRC (and PAC) Gas Benefits ($)                8020.010\nTRC (and PAC) Total Benefits ($)            340440.400\nTRC Costs ($)                                13287.870\nPAC Costs ($)                                12256.910\nElectricity First Year Net Savings (MWh)       419.100\nElectricity Lifecycle Net Savings (MWh)       3771.900\nGas First Year Net Savings (Therms)            950.000\nGas Lifecycle Net Savings (Therms)            8550.000\nElectricity Lifecycle GHG Savings (Tons)      1060.143\nGas Lifecycle GHG Savings (Tons)                51.300\nTotal Lifecycle GHG Savings (Tons)            1111.443\ndtype: float64')
