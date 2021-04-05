import os
import pandas as pd
import pytest
import tempfile
import zipfile

from flexvalue.cet_flexvalue_compare import CET_Scan


@pytest.fixture
def cet_scan():
    return CET_Scan(
        directory=tempfile.mkdtemp(),
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
    user_inputs = cet_scan.generate_cet_input_file()
    assert user_inputs['load_shape'].str.startswith(cet_scan.program_admin).all()


def test_parse_cet_output(cet_scan, monkeypatch):
    csv_filepath = os.path.join(cet_scan.cet_path, "outputs.csv")
    run_id = '1234'
    zip_filename = f'{cet_scan.scan_name}_for_cet_ui_run_{run_id}.zip'
    zip_filepath = os.path.join(cet_scan.cet_path, zip_filename)
    pd.DataFrame([{"abc": "def"}]).to_csv(csv_filepath)
    with zipfile.ZipFile(zip_filepath, "w") as f:
        f.write(csv_filepath, arcname=f"{run_id}_outputs.csv")
    cet_scan.parse_cet_output()
    cet_scan.parse_cet_output(zip_filepath)
