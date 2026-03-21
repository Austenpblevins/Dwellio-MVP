import re

_TOKEN_REPLACEMENTS = {
    "APARTMENT": "",
    "APT": "",
    "AVENUE": "AVE",
    "BOULEVARD": "BLVD",
    "CIRCLE": "CIR",
    "COURT": "CT",
    "DRIVE": "DR",
    "EAST": "E",
    "HIGHWAY": "HWY",
    "LANE": "LN",
    "NORTH": "N",
    "PARKWAY": "PKWY",
    "PLACE": "PL",
    "ROAD": "RD",
    "SOUTH": "S",
    "STREET": "ST",
    "SUITE": "",
    "TEXAS": "TX",
    "UNIT": "",
    "WEST": "W",
}


def normalize_address_query(value: str) -> str:
    value = value.upper().strip()
    value = re.sub(r"[^A-Z0-9 ]", " ", value)
    tokens = [_TOKEN_REPLACEMENTS.get(token, token) for token in value.split()]
    value = " ".join(token for token in tokens if token)
    value = re.sub(r"\s+", " ", value)
    return value
