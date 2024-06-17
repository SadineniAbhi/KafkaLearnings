import json


def get_schema(file: str) -> dict:
    with open(file) as f:
        schema = f.read()
    schema = json.loads(schema)
    return schema
