import pytest
import threading
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from replicator import Replicator


@pytest.fixture(scope="module")
def admin_client():
    admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
    yield admin
    topics_to_delete = ["topic1", "topic2"]
    admin.delete_topics(topics=topics_to_delete, timeout_ms=5000)
    admin.close()


@pytest.fixture(scope="module")
def setup_topics(admin_client):
    topics = [
        NewTopic(name="topic1", num_partitions=1, replication_factor=1),
        NewTopic(name="topic2", num_partitions=1, replication_factor=1)
    ]
    admin_client.create_topics(new_topics=topics, validate_only=False)


@pytest.fixture(scope="module")
def replicator():
    return Replicator(KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='earliest'),
                      KafkaProducer(bootstrap_servers='localhost:9092'))


@pytest.fixture(scope="module")
def new_consumer():
    return KafkaConsumer(bootstrap_servers='localhost:9092', group_id="newgroup", auto_offset_reset='earliest')


def test_replication(setup_topics, replicator, new_consumer):
    messages = [b'hello world', b'hello', b'abhi is a genius']
    for message in messages:
        replicator.producer.send('topic1', message)
        replicator.producer.flush()

    replicator_thread = threading.Thread(target=lambda: replicator.replicate('topic1', 'topic2'), daemon=True)
    replicator_thread.start()

    count = 0
    data = []
    new_consumer.subscribe(['topic2'])
    for message in new_consumer:
        count += 1
        data.append(message.value)
        if count == len(messages) :
            break

    assert data == messages, "Data received does not match the sent messages."
