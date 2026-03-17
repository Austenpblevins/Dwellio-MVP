def build_storage_path(*parts: str) -> str:
    return '/'.join(part.strip('/') for part in parts if part)
