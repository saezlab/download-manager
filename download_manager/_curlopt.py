
import pycurl


SYNONYMS =
    {
        'http_version': {
            'http_version_1_1': 1,
        }
    }


def name_to_int(name: str) -> int | None:

    name = name.upper()

    for n in (name, f'CURL_{name}'):
        if (value := getattr(pycurl, n, None)) is not None:
            return value

    return None
