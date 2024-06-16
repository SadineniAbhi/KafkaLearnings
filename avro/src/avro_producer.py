from producer_factory import ProducerFactory
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry.avro import AvroSerializer
from config import get_config


def main(topic: str, avro_serializer: AvroSerializer, producer: Producer) -> None:
    ...


if __name__ == "__main__":
    conf = get_config('config.ini')
    prod_obj = ProducerFactory().get_producer()


