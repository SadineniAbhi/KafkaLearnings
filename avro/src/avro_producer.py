from producer_factory import LocalProducer
from confluent_kafka import Producer
from config import get_config
from serialize_avro import AvroSerializer
from read_schema import get_schema


def main(topic: str, serializer: AvroSerializer, producer: Producer) -> None:
    records = [{"name": "abhi", "age": 19},
               {"name": "mock", "age": 20},
               {"name": "big", "age": 21},
               {"name": "small", "age": 22}]

    for record in records:
        serialized_record = serializer.serialize(record)
        producer.produce(topic, serialized_record)
    producer.flush()


if __name__ == "__main__":
    conf = get_config('config.ini')
    prod_obj = LocalProducer().get_producer()
    schema = get_schema('user.avsc')
    avro_serializer = AvroSerializer(schema=schema)
    main(topic=conf['topic']['topic_one'], serializer=avro_serializer, producer=prod_obj)
