from io import BytesIO
from fastavro import writer, reader


class AvroSerializer:

    def __init__(self, schema):
        self.schema = schema

    def serialize(self, record):
        buffer = BytesIO()
        writer(buffer, self.schema, [record])
        return buffer.getvalue()

    def deserialize(self, serialized_avro_msg):
        buffer = BytesIO(serialized_avro_msg)
        return [record for record in reader(buffer, self.schema)]
