from confluent_kafka import Consumer
from config import get_config
from typing import Dict


class ConsumerFactory:
    def __init__(self, group_id: str):
        self.group_id = group_id
        self.conf = get_config('config.ini')

    # helper function to set up the config dictionary
    def _create_conf_obj(self) -> Dict[str, str]:
        return {
            'bootstrap.servers': self.conf['kafka']['bootstrap_servers'],
            'sasl.mechanisms': self.conf['kafka']['sasl_mechanisms'],
            'security.protocol': self.conf['kafka']['security_protocol'],
            'sasl.username': self.conf['kafka']['sasl_username'],
            'sasl.password': self.conf['kafka']['sasl_password'],
            'group.id': self.group_id,
            'auto.offset.reset': self.conf['consumer']['auto_offset_reset'],
        }

    def get_consumer(self) -> Consumer:
        return Consumer(self._create_conf_obj())


