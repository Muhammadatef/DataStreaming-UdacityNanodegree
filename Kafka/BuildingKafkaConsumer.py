# Please complete the TODO items in the code

import asyncio
from dataclasses import dataclass, field
import json
import random

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker


faker = Faker()

BROKER_URL = "PLAINTEXT://localhost:9092"


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0"})
    c.subscribe([topic_name])

    while True:
        messages = c.consume(5, timeout=1.0)
        print(f"consumed {len(messages)} messages")

        for message in messages:
                print(f"data recieved : {message.key()} : {message.value()}")

       

        # Do not delete this!
        await asyncio.sleep(0.01)


def main():
    """Checks for topic and creates the topic if it does not exist"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})

    try:
        asyncio.run(produce_consume("com.udacity.lesson2.exercise6.purchases"))
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})
    while True:
        for _ in range(10):
            p.produce(topic_name, Purchase().serialize())
        await asyncio.sleep(0.01)


async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    await t1
    await t2


@dataclass
class Purchase:
    username: str = field(default_factory=faker.user_name)
    currency: str = field(default_factory=faker.currency_code)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))

    def serialize(self):
        return json.dumps(
            {
                "username": self.username,
                "currency": self.currency,
                "amount": self.amount,
            }
        )


if __name__ == "__main__":
    main()




## OUTPUT 

# consumed 5 messages
# data recieved : None : b'{"username": "joseph92", "currency": "ARS", "amount": 46299}'
# data recieved : None : b'{"username": "flemingdylan", "currency": "JOD", "amount": 148880}'
# data recieved : None : b'{"username": "qfoley", "currency": "TND", "amount": 126845}'
# data recieved : None : b'{"username": "mbuckley", "currency": "LAK", "amount": 151691}'
# data recieved : None : b'{"username": "mccarthyjulie", "currency": "BOB", "amount": 4105}'
