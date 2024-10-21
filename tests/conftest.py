import pytest
import download_manager as dm


@pytest.fixture
def simple_url():

    return  dm._data._module_data('url')


@pytest.fixture
def http_url():

    return 'http://eu.httpbin.org/'


@pytest.fixture
def https_url():

    return 'http://omnipathdb.org/'


@pytest.fixture(scope='session')
def download_dir(tmpdir_factory):

    fn = tmpdir_factory.mktemp('test_downloads')

    return fn


@pytest.fixture(params=[dm.CurlDownloader, dm.RequestsDownloader])
def downloader(request):

    return request.param


@pytest.fixture(params=['curl', 'requests'])
def d_config(request):

    return {'backend': request.param}

