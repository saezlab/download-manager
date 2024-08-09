import download_manager as dm


def test_builtin_examples(simple_url):

    assert isinstance(simple_url, dict)
    assert len(simple_url) == 1
    assert simple_url['url'] == 'https://www.google.com'


def test_descriptor_simple_init_kwargs(simple_url):

    desc = dm.Descriptor(**simple_url)

    assert desc._param['url'] == 'https://www.google.com'
    assert desc.url == 'https://www.google.com'


def test_descriptor_simple_init_args(simple_url):

    desc = dm.Descriptor(simple_url['url'])

    assert desc._param['url'] == 'https://www.google.com'
    assert desc.url == 'https://www.google.com'
