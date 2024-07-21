import avro.schema


def get_schema(file: str) -> dict:
    schema = avro.schema.parse(open(file).read())
    return schema
