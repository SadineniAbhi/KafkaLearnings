from kafka import KafkaConsumer, KafkaProducer


class Replicator:
    def __init__(self, consumer_obj: KafkaConsumer, producer_obj: KafkaProducer) -> None:
        self.consumer = consumer_obj
        self.producer = producer_obj

    def replicate(self, replicate_from: str, replicate_to: str) -> None:
        self.consumer.subscribe([replicate_from])
        for msg in self.consumer:
            msg = msg.value
            self.producer.send(replicate_to, msg)
