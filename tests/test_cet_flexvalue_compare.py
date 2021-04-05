import os
import pytest

from flexvalue.cet_flexvalue_compare import CET_Scan


@pytest.fixture
def cet_scan():
    return CET_Scan(
        directory=os.getcwd(),
        scan_name="Test_Run",
        program_year="2021",
        acc_version="2020",
        program_admin="PGE",
        climate_zone=["12", "3A", "4"],
        mwh=[5000, 10, 2],
        therms=[200, 3, 114],
        units=[1, 1, 10],
        ntg=[0.95, 0.4, 0.9],
        eul=[7, 1, 1],
        sector=["NonRes", "Res", "Res"],
        deer_load_shape=[
            "DEER:HVAC_Chillers",
            "DEER:HVAC_Chillers",
            "DEER:Indoor_CFL_Ltg",
        ],
        gas_sector=["Residential", "Residential", "Residential"],
        gas_savings_profile=["Annual", "Winter Only", "Summer Only"],
        admin_cost=[5000, 165, 299],
        measure_cost=[220000, 310, 4044],
        incentive=[200000, 66, 800],
    )

def test_generate_cet_input_file(cet_scan):
    cet_scan.generate_cet_input_file()

def test_parse_cet_output(cet_scan):
    pass
    #cet_scan.parse_cet_output()

    # Results -> Results_test_run.csv compare to Flexvalue
