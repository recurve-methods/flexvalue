import pytest
import random


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
