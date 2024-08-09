
import pytest
import download_manager as dm


@pytest.fixture
def simple_url():

    return  dm._data._module_data('url')
