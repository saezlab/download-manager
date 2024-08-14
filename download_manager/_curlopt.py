import re

import pycurl


SYNONYMS = {
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

def version_to_int(ver: str):
    ver = str(ver)
    
    if not re.match(r'^(?:curl_)?http_version', ver, re.IGNORECASE):
        ver = ver.replace('.', '_')
        ver = f'curl_http_version_{ver}'
    
    return name_to_int(ver)
