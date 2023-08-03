import pytest
import random
import requests
import os


@pytest.fixture
def test_ids():
    return [f"{i}" for i in range(5)]

def pytest_addoption(parser):
    parser.addoption("--database-version", action="store", default="")


@pytest.fixture
def deer_ls_options():
    return [
        "Res_Indoor_CFL_Ltg",
        "Res_RefgFrzr_HighEff",
        "Res_RefgFrzr_Recyc_Conditioned",
        "Res_RefgFrzr_Recyc_UnConditioned",
    ]


@pytest.fixture
def user_inputs(metered_load_shape, deer_ls_options):
    user_inputs = metered_load_shape.sum()
    user_inputs.name = "mwh_savings"
    user_inputs.index.name = "ID"
    user_inputs = user_inputs.to_frame().reset_index()
    user_inputs["load_shape"] = user_inputs["ID"]  # so it points to metered_ls
    user_inputs["start_year"] = 2021
    user_inputs["start_quarter"] = [1, 2, 3, 4, 2]
    user_inputs["utility"] = "PGE"
    user_inputs["climate_zone"] = "3A"
    user_inputs["therms_savings"] = [1000, 900, 3000, 0, 920]
    user_inputs["therms_profile"] = "annual"
    user_inputs["units"] = 1
    user_inputs["eul"] = 9
    user_inputs["ntg"] = 0.95
    user_inputs["discount_rate"] = 0.0766
    user_inputs["admin"] = [1234, 2345, 3456, 4567, 5678]
    user_inputs["measure"] = [12340, 23450, 34560, 45670, 56780]
    user_inputs["incentive"] = [11234, 12345, 13456, 14567, 15678]
    user_inputs = user_inputs[
        [
            "ID",
            "start_year",
            "start_quarter",
            "utility",
            "climate_zone",
            "mwh_savings",
            "load_shape",
            "therms_savings",
            "therms_profile",
            "units",
            "eul",
            "ntg",
            "discount_rate",
            "admin",
            "measure",
            "incentive",
        ]
    ]
    deer_user_input = user_inputs.iloc[0].copy(deep=True)
    for deer in deer_ls_options:
        deer_user_input["load_shape"] = deer
        deer_user_input["ID"] = deer
        user_inputs = user_inputs.append(deer_user_input, ignore_index=True)
    return user_inputs

@pytest.fixture(scope="session", autouse=True)
def download_test_data():
    """
    Downloads missing test data files from a remote server and saves them to a local directory.

    The function checks for a directory called "tests/test_data" and creates it if it doesn't exist.
    It then compares the list of expected test data files to the list of files in the local directory.
    Any missing files are downloaded from a remote server and saved to the local directory.

    Returns:
        None
    """
    TEST_FILE_NAMES = set((
        "ca_hourly_electric_load_shapes.csv",
        "ca_monthly_therms_load_profiles.csv",
        "example_metered_load_shape.csv",
        "example_two_metered_load_shapes.csv",
        "example_user_inputs_380.csv",
        "example_user_inputs_cz12_37.csv",
        "example_user_inputs_no_header.csv",
        "example_user_inputs_two_metered.csv",
        "example_user_inputs.csv",
        "full_ca_avoided_costs_2020acc_gas.csv",
        "full_ca_avoided_costs_2020acc.csv",
        "test_value_curve_join_elec_acc.csv",
        "test_value_curve_join_gas_acc.csv",
        "value_curve_join_inputs_2.csv"
    ))
    
    if not os.path.exists("tests/test_data"):
        os.makedirs("tests/test_data")
    current_test_files = set(os.listdir("tests/test_data"))
    missing_test_files = TEST_FILE_NAMES - current_test_files
    
    for missing_file in missing_test_files:
        url = f"https://storage.googleapis.com/oee-avdcosts-platform/flexvalue_test_data/{missing_file}"
        response = requests.get(url)
        if response.status_code == 200:
            local_path = f"tests/test_data/{missing_file}"
            with open(local_path, "wb") as f:
                f.write(response.content)
            print(f"Downloaded {missing_file} to {local_path}")
        else:
            print(f"Failed to download {missing_file} at {url}")
        


