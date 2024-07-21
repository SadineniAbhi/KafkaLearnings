import avro.schema
import avro.io
import io


class AvroSerializer:

    def __init__(self, schema):
        self.schema = schema

    def serialize(self, record):
        writer = avro.io.DatumWriter(self.schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(record, encoder)
        raw_bytes = bytes_writer.getvalue()
        return raw_bytes

    def deserialize(self, serialized_avro_msg):
        bytes_reader = io.BytesIO(serialized_avro_msg)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(self.schema)
        record = reader.read(decoder)
        return record

