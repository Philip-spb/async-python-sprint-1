import json

import pytest


@pytest.fixture
def data_fixture():
    with open('./examples/response.json') as f:
        data = json.load(f)

    return data


@pytest.fixture
def raw_data_fixture(data_fixture):
    return data_fixture['forecasts']
