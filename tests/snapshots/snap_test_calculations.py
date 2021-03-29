# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import GenericRepr, Snapshot


snapshots = Snapshot()

snapshots["test_electric_benefits_full_outputs elec_ben"] = GenericRepr(
    "                             identifier  ...  methane_leakage\n0                                     0  ...      2440.456961\n1                                     0  ...      2614.282898\n2                                     0  ...      2114.712496\n3                                     0  ...      1617.374298\n4                                     0  ...      2150.869447\n...                                 ...  ...              ...\n23323  Res_RefgFrzr_Recyc_UnConditioned  ...        10.331944\n23324  Res_RefgFrzr_Recyc_UnConditioned  ...        10.269918\n23325  Res_RefgFrzr_Recyc_UnConditioned  ...         8.232499\n23326  Res_RefgFrzr_Recyc_UnConditioned  ...         6.382734\n23327  Res_RefgFrzr_Recyc_UnConditioned  ...         5.616405\n\n[23328 rows x 17 columns]"
)

snapshots["test_user_inputs_basic df_output_table"] = GenericRepr(
    "Outputs                                 TRC  ...  Lifecycle Total GHG Savings (Tons)\n0                                 10215.860  ...                          464320.275\n1                                  5418.946  ...                          451182.163\n2                                  3817.229  ...                          462446.939\n3                                  2939.286  ...                          469590.918\n4                                  2279.498  ...                          450237.801\nRes_Indoor_CFL_Ltg                   28.584  ...                            1199.112\nRes_RefgFrzr_HighEff                 25.443  ...                            1116.635\nRes_RefgFrzr_Recyc_Conditioned       25.443  ...                            1116.635\nRes_RefgFrzr_Recyc_UnConditioned     25.620  ...                            1111.443\nTotals                             2922.679  ...                         2302321.921\n\n[10 rows x 14 columns]"
)

snapshots["test_user_inputs_from_example_metered df_output_table"] = GenericRepr(
    "Outputs        TRC  ...  Lifecycle Total GHG Savings (Tons)\nid_0     11300.838  ...                          464274.105\nid_1      5596.145  ...                          451146.253\nid_2      3848.054  ...                          462308.429\nid_3      2927.549  ...                          469611.438\nid_4      2198.086  ...                          445386.153\nTotals    3782.528  ...                         2292726.378\n\n[6 rows x 14 columns]"
)

snapshots["test_user_inputs_full df_output_table"] = GenericRepr(
    "Outputs                                 TRC  ...  Lifecycle Total GHG Savings (Tons)\n0                                 10215.860  ...                          464320.275\n1                                  5418.946  ...                          451182.163\n2                                  3817.229  ...                          462446.939\n3                                  2939.286  ...                          469590.918\n4                                  2279.498  ...                          450237.801\nRes_Indoor_CFL_Ltg                   28.584  ...                            1199.112\nRes_RefgFrzr_HighEff                 25.443  ...                            1116.635\nRes_RefgFrzr_Recyc_Conditioned       25.443  ...                            1116.635\nRes_RefgFrzr_Recyc_UnConditioned     25.620  ...                            1111.443\nTotals                             2922.679  ...                         2302321.921\n\n[10 rows x 14 columns]"
)

snapshots["test_user_inputs_single_row df_output_table"] = GenericRepr(
    "Outputs                             TRC  ...  Lifecycle Total GHG Savings (Tons)\nRes_RefgFrzr_Recyc_UnConditioned  25.62  ...                            1111.443\nTotals                            25.62  ...                            1111.443\n\n[2 rows x 14 columns]"
)
