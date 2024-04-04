# PLEASE COMPLETE THE TODO ITEMS IN THIS PYTHON CODE

import asyncio

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic


BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "my-first-python-topic"


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
   
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#producer
    p = Producer({"bootstrap.servers": BROKER_URL})

    curr_iteration = 0
    while True:
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Producer.produce
        p.produce(topic_name, f"iteration {curr_iteration}".encode("utf-8"))

        curr_iteration += 1
        await asyncio.sleep(1)


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#consumer
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id" : "My-First-Consumer-Group-Id"} )

    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Consumer.subscribe
    c.subscribe([TOPIC_NAME])

    while True:
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Consumer.poll
        message = c.poll(1.0)
        if message is None:
            print("No Messges Recieved!")
        elif message.error() is not None:
            print(f"Message Error : {message.error}")
        else:
            print(f"Message Key : {message.key()}, Message Value : {message.value()}")

        await asyncio.sleep(1)


async def produce_consume():
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(TOPIC_NAME))
    t2 = asyncio.create_task(consume(TOPIC_NAME))
    await t1
    await t2


def main():
    """Runs the exercise"""

    try:
        asyncio.run(produce_consume())
    except KeyboardInterrupt as e:
        print("shutting down")
    finally:
        pass


if __name__ == "__main__":
    main()
