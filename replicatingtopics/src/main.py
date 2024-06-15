from replicator import Replicator
import configparser
from kafka import KafkaConsumer, KafkaProducer

config = configparser.ConfigParser()
config.read('config.ini')


def main(replicator: Replicator):
    replicator.replicate(config['topics']['from_topic'], config['topics']['to_topic'])


if __name__ == '__main__':
    rep_obj = Replicator(consumer_obj=KafkaConsumer(bootstrap_servers=config['consumer']['bootstrap_servers']),
                         producer_obj=KafkaProducer(bootstrap_servers=config['producer']['bootstrap_servers']))

    main(replicator=rep_obj)
