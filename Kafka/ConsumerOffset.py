# Please complete the TODO items in the code

import asyncio

from confluent_kafka import Consumer, Producer, OFFSET_BEGINNING
from confluent_kafka.admin import AdminClient, NewTopic


BROKER_URL = "PLAINTEXT://localhost:9092"


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    # Sleep for a few seconds to give the producer time to create some data
    await asyncio.sleep(2.5)

    #  Set the auto offset reset to earliest
    
    c = Consumer(
        {
            "bootstrap.servers": BROKER_URL,
            "group.id": "0",
            "auto.offset.reset": "earliest"
        }
    )

    #  Configure the on_assign callback
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=partition#confluent_kafka.Consumer.subscribe
    c.subscribe([topic_name], on_assign=on_assign)

    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            print(f"consumed message {message.key()}: {message.value()}")
        await asyncio.sleep(0.1)


def on_assign(consumer, partitions):
    """Callback for when topic assignment takes place"""
    #  Set the partition offset to the beginning on every boot.
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=partition#confluent_kafka.Consumer.on_assign
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=partition#confluent_kafka.TopicPartition
    for partition in partitions:
        partition.offset = OFFSET_BEGINNING

    #  Assign the consumer the partitions
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=partition#confluent_kafka.Consumer.assign
    consumer.assign(partitions)


def main():
    """Runs the exercise"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    try:
        asyncio.run(produce_consume("com.udacity.lesson2.exercise5.iterations"))
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})

    curr_iteration = 0
    while True:
        p.produce(topic_name, f"iteration {curr_iteration}".encode("utf-8"))
        curr_iteration += 1
        await asyncio.sleep(0.1)


async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    await t1
    await t2


if __name__ == "__main__":
    main()





## OUTPUT : 
# root@2aca0b1f17ee1be103cd28f3f8c7748a75091206-644b89b5cf-2tp4p:/workspace/home# python exercise2.5.py
# no message received by consumer
# no message received by consumer
# no message received by consumer
# consumed message None: b'iteration 0'
# consumed message None: b'iteration 1'
# consumed message None: b'iteration 2'
# consumed message None: b'iteration 3'
# consumed message None: b'iteration 4'
# consumed message None: b'iteration 5'
# consumed message None: b'iteration 6'
# consumed message None: b'iteration 7'
# consumed message None: b'iteration 8'
# consumed message None: b'iteration 9'
# consumed message None: b'iteration 10'
# consumed message None: b'iteration 11'
# consumed message None: b'iteration 12'
# consumed message None: b'iteration 13'
# consumed message None: b'iteration 14'
# consumed message None: b'iteration 15'
# consumed message None: b'iteration 16'
# consumed message None: b'iteration 17'
# consumed message None: b'iteration 18'
# consumed message None: b'iteration 19'
# consumed message None: b'iteration 20'
# consumed message None: b'iteration 21'
# consumed message None: b'iteration 22'
# consumed message None: b'iteration 23'
# consumed message None: b'iteration 24'
# consumed message None: b'iteration 25'
# consumed message None: b'iteration 26'
# consumed message None: b'iteration 27'
# consumed message None: b'iteration 28'
# consumed message None: b'iteration 29'
