from confluent_kafka import Producer
from config import get_config
from typing import Dict


class ProducerFactory:
    def __init__(self):
        self.conf = get_config('config.ini')

    # helper function which returns dictionary
    def _create_conf_obj(self) -> Dict[str, str]:
        return {
            'bootstrap.servers': self.conf['kafka']['bootstrap_servers'],
            'sasl.mechanisms': self.conf['kafka']['sasl_mechanisms'],
            'security.protocol': self.conf['kafka']['security_protocol'],
            'sasl.username': self.conf['kafka']['sasl_username'],
            'sasl.password': self.conf['kafka']['sasl_password'],
        }

    def get_producer(self) -> Producer:
        return Producer(self._create_conf_obj())
