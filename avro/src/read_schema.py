def get_schema(file: str) -> str:
    with open(file) as f:
        schema = f.read()
    return schema
