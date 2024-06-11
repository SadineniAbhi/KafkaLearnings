from replicator import Replicator

from kafka import KafkaConsumer, KafkaProducer


def main(replicator: Replicator):
    replicator.replicate('a', 'b')


if __name__ == '__main__':
    rep_obj = Replicator(consumer_obj=KafkaConsumer(bootstrap_servers='localhost:9092'),
                         producer_obj=KafkaProducer(bootstrap_servers='localhost:9092'))

    main(replicator=rep_obj)
