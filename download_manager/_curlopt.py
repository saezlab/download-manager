import re

import pycurl


SYNONYMS = {
}


def process(key: str, value: Any) -> Any:

    if (proc := SYNONYMS.get(key, globals().get(key, None))):

        if isinstance(proc, dict):

            value = proc.get(value, value)

        elif callable(proc):

            value = proc(value)

    return name_to_int(value)


def name_to_int(name: str) -> int | None:


    if isinstance(name, int):

        return name

    name = name.upper()

    for n in (name, f'CURL_{name}'):
        if (value := getattr(pycurl, n, None)) is not None:
            return value

    return None


def http_version(ver: str):

    ver = str(ver)

    if not re.match(r'^(?:curl_)?http_version', ver, re.IGNORECASE):
        ver = ver.replace('.', '_')
        ver = f'curl_http_version_{ver}'

    return ver
