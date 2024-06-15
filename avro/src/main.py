from consumer import ConsumerFactory
from producer import ProducerFactory
from confluent_kafka import Producer, Consumer


def main(consumer_obj: Consumer, producer_obj: Producer) -> None:
    ...


if __name__ == "__main__":
    prod_obj = ProducerFactory().get_producer()
    con_obj = ConsumerFactory('xyz').get_consumer()

    main(con_obj, prod_obj)
