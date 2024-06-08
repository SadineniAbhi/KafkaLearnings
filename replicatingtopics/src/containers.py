from dependency_injector import containers, providers
from confluent_kafka import Producer, Consumer

class Container(containers.DeclarativeContainer):

    # Objects 
    
    producer_obj = providers.Singleton(
        Producer,
        {'bootstrap.servers': 'localhost:9092',
        'client.id': 'replicator_producer'}
       
    )


    consumer_obj = providers.Singleton(
        Consumer,
         {'bootstrap.servers': 'localhost:9092',
                 'group.id': 'replicator_group',
                 'auto.offset.reset': 'smallest'}

    )

