from consumer_factory import ConsumerFactory
from confluent_kafka import Consumer, KafkaException
from config import get_config
from serialize_avro import AvroSerializer
from read_schema import get_schema


def main(topic: str, serializer: AvroSerializer, consumer: Consumer) -> None:
    consumer.subscribe([topic])
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(msg.error())
        else:
            msg = msg.value()
            print(serializer.deserialize(msg))


if __name__ == "__main__":
    conf = get_config('config.ini')
    con_obj = ConsumerFactory(conf['consumer']['group_id']).get_consumer()
    schema = get_schema('user.avsc')
    avro_serializer = AvroSerializer(schema=schema)
    main(topic=conf['topic']['topic_one'], serializer=avro_serializer, consumer=con_obj)
