from __future__ import annotations
import re

def normalize_address_query(value: str) -> str:
    value = value.upper().strip()
    value = re.sub(r'[^A-Z0-9 ]', ' ', value)
    value = re.sub(r'\s+', ' ', value)
    return value
