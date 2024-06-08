import logging 
import os
from dependency_injector.wiring import inject, Provide
from containers import Container
from confluent_kafka import Consumer,Producer


#logger configuration
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) #get root directory
file_path = os.path.join(root_dir, "logs.log")#get logs.log file path
logger = logging.getLogger(__name__) #get logger object
file_handler = logging.FileHandler(file_path) #create file handler
logger.addHandler(file_handler) #add file handler to logger



@inject
def replicate(from_topics: list ,
              to_topic: str,
              consumer_obj :Consumer = Provide[Container.consumer_obj], 
              producer_obj:Producer = Provide[Container.producer_obj]) -> None:
    try:
        consumer_obj.subscribe(from_topics)

        while True:
            msg = consumer_obj.poll(timeout=1)
            if msg is None:
                break #stops repication after 1 second of no messages
            elif msg.error():
                logger.error("Error: %s" % msg.error())
            else:
                producer_obj.produce(to_topic, value=msg.value().decode('utf-8'))
                producer_obj.poll(1)

    except KeyboardInterrupt:
        print("stopped replicating")

    finally:
        consumer_obj.close()


if __name__ == '__main__':
    container = Container()
    container.init_resources()
    container.wire(modules=[__name__])
    # replicate take list of topics to replicate from and a topic to replicate to
    replicate(['a'],'b')
 