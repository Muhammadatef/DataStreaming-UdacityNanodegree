import asyncio
from dataclasses import asdict, dataclass, field
import json
import random

from confluent_kafka import avro, Consumer, Producer
from confluent_kafka.avro import AvroConsumer, AvroProducer, CachedSchemaRegistryClient
from faker import Faker


faker = Faker()

SCHEMA_REGISTRY_URL = "http://localhost:8081"
BROKER_URL = "PLAINTEXT://localhost:9092"


@dataclass
class ClickAttribute:
    element: str = field(default_factory=lambda: random.choice(["div", "a", "button"]))
    content: str = field(default_factory=faker.bs)

    @classmethod
    def attributes(self):
        return {faker.uri_page(): ClickAttribute() for _ in range(random.randint(1, 5))}


@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))
    attributes: dict = field(default_factory=ClickAttribute.attributes)

    #
    # TODO: Load the schema using the Confluent avro loader
    #       See: https://github.com/confluentinc/confluent-kafka-python/blob/master/confluent_kafka/avro/load.py#L23
    #
    schema = avro.loads(
        """{
        "type": "record",
        "name": "click_event",
        "namespace": "com.udacity.lesson3.solution4",
        "fields": [
            {"name": "email", "type": "string"},
            {"name": "timestamp", "type": "string"},
            {"name": "uri", "type": "string"},
            {"name": "number", "type": "int"},
            {
                "name": "attributes",
                "type": {
                    "type": "map",
                    "values": {
                        "type": "record",
                        "name": "attribute",
                        "fields": [
                            {"name": "element", "type": "string"},
                            {"name": "content", "type": "string"}
                        ]
                    }
                }
            }
        ]
    }"""
    )


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    #
    # TODO: Create a CachedSchemaRegistryClient. Use SCHEMA_REGISTRY_URL.
    #       See: https://github.com/confluentinc/confluent-kafka-python/blob/master/confluent_kafka/avro/cached_schema_registry_client.py#L47
    #
    schema_registry = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    #
    # TODO: Replace with an AvroProducer.
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=loads#confluent_kafka.avro.AvroProducer
    #
    p = AvroProducer({"bootstrap.servers": BROKER_URL}, schema_registry=schema_registry)
    while True:
        #
        # TODO: Replace with an AvroProducer produce. Make sure to specify the schema!
        #       Tip: Make sure to serialize the ClickEvent with `asdict(ClickEvent())`
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=loads#confluent_kafka.avro.AvroProducer
        #
        p.produce(
            topic=topic_name, value=asdict(ClickEvent()), value_schema=ClickEvent.schema
        )
        await asyncio.sleep(1.0)


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    #
    # TODO: Create a CachedSchemaRegistryClient
    #
    schema_registry = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    #
    # TODO: Use the Avro Consumer
    #
    c = AvroConsumer(
        {"bootstrap.servers": BROKER_URL, "group.id": "0"},
        schema_registry=schema_registry,
    )
    c.subscribe([topic_name])
    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            try:
                print(message.value())
            except KeyError as e:
                print(f"Failed to unpack message {e}")
        await asyncio.sleep(1.0)


def main():
    """Checks for topic and creates the topic if it does not exist"""
    try:
        asyncio.run(produce_consume("com.udacity.lesson3.solution4.clicks"))
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    await t1
    await t2


if __name__ == "__main__":
    main()

#OUTPUT Without Schema Registry with a regular Consumer  :

# root@690b89aac913a6edaf627a5ad22e781bfdb1aeee-cc9c676d8-2j2fj:/workspace/home# python exercise3.4.py
# no message received by consumer
# no message received by consumer
# no message received by consumer
# b'\x00\x00\x00\x00\x01(steven25@hotmail.com&2023-05-16T09:45:32Phttps://www.allen.info/category/home.htm\x86\x03\n\x10category\x02aBtransition ubiquitous initiatives\nindex\x0cbutton2orchestrate B2C e-tailers\nterms\x0cbutton.unleash dot-com metrics\x0eprivacy\x02aBincentivize extensible e-commerce\x10homepage\x06div8incubate robust partnerships\x00'
# b'\x00\x00\x00\x00\x01&ytownsend@gmail.com&1989-08-30T05:59:40lhttp://www.goodwin-nolan.com/main/category/privacy.php\xc8\n\x04\x08main\x06div6incubate virtual interfaces\x10category\x0cbutton>leverage end-to-end initiatives\x00'
# b'\x00\x00\x00\x00\x01$marylamb@clark.com&2019-12-07T11:01:51Dhttps://reed.org/blog/tag/home.asp\x98\x04\x02\x0eprivacy\x0cbutton2enable magnetic mindshare\x00'
# b'\x00\x00\x00\x00\x01&daniel29@brown.info&2014-01-27T05:13:05Bhttps://www.howell-king.com/home/\xf6\n\x04\x10homepage\x06div0seize one-to-one content\x06faq\x02a0engage robust e-services\x00'
# b'\x00\x00\x00\x00\x01*katherine32@allen.biz&2008-03-20T16:17:17Vhttp://www.johns.org/categories/search.html\xaa\x05\x04\x08main\x02aBdisintermediate efficient systems\nindex\x06div.mesh dynamic e-services\x00'
# b'\x00\x00\x00\x00\x014williamsjennifer@gmail.com&1978-10-26T02:09:26Dhttp://www.medina.biz/category.htm\xf0\x06\x04\x08main\x06div<visualize turn-key communities\nlogin\x02a<drive cross-platform synergies\x00'
# b'\x00\x00\x00\x00\x01&pjones@thompson.com&2002-10-02T10:42:00$http://wagner.com/\xec\x0b\x04\nabout\x0cbutton:extend mission-critical users\x0csearch\x06div.leverage 24/7 solutions\x00'
# b'\x00\x00\x00\x00\x01"dwagner@gmail.com&1973-01-26T08:11:52Bhttp://www.ramos.biz/homepage.htm\xde\x0c\x02\x0eprivacy\x0cbutton0evolve impactful markets\x00'
# b'\x00\x00\x00\x00\x012justinmanning@hotmail.com&2015-12-14T22:58:026http://santiago.net/search/\xb6\x04\x06\nindex\x06divDdisintermediate out-of-the-box ROI\x10register\x02a4benchmark vertical systems\x08post\x02aFsyndicate distributed architectures\x00'
# b'\x00\x00\x00\x00\x012swansonkristine@gmail.com&1999-08-10T14:18:37>https://www.lewis.com/login.php\xc0\x02\x02\x06faq\x0cbutton8transform user-centric users\x00'
# b'\x00\x00\x00\x00\x01*katherine20@gmail.com&2004-11-27T03:10:168http://hughes.com/tags/main/\xde\t\x06\x08main\x06div<leverage open-source solutions\nterms\x06div.matrix global bandwidth\nindex\x02aVre-contextualize user-centric architectures\x00'

## OUTPUT WITH SCHEMA REGISTRY AND AVROConsumer:

# root@690b89aac913a6edaf627a5ad22e781bfdb1aeee-cc9c676d8-2j2fj:/workspace/home# python exercise3.4.py
# no message received by consumer
# no message received by consumer
# {'email': 'fjohnston@schwartz.net', 'timestamp': '1974-03-30T09:04:45', 'uri': 'http://mitchell-hoffman.com/search/index.php', 'number': 168, 'attributes': {'main': {'element': 'div', 'content': 'leverage customized systems'}, 'author': {'element': 'div', 'content': 'generate virtual platforms'}, 'search': {'element': 'a', 'content': 'deploy plug-and-play mindshare'}}}
# {'email': 'champton@gmail.com', 'timestamp': '2004-10-02T01:13:53', 'uri': 'https://www.barber-martin.com/app/search/category/homepage.htm', 'number': 911, 'attributes': {'category': {'element': 'div', 'content': 'matrix real-time infrastructures'}, 'author': {'element': 'a', 'content': 're-intermediate intuitive eyeballs'}, 'privacy': {'element': 'button', 'content': 'seize innovative platforms'}}}
# {'email': 'jennifer55@yahoo.com', 'timestamp': '1989-01-28T03:10:46', 'uri': 'https://www.vang.com/home.htm', 'number': 37, 'attributes': {'index': {'element': 'a', 'content': 'strategize scalable web services'}, 'author': {'element': 'div', 'content': 'orchestrate killer e-tailers'}, 'homepage': {'element': 'div', 'content': 'transition cross-platform web services'}, 'login': {'element': 'button', 'content': 'evolve frictionless portals'}}}
# {'email': 'cperkins@gmail.com', 'timestamp': '2006-08-19T14:51:21', 'uri': 'http://www.smith-chavez.biz/app/main/posts/search/', 'number': 291, 'attributes': {'terms': {'element': 'div', 'content': 'harness killer platforms'}}}
# {'email': 'carolynmullins@rhodes.biz', 'timestamp': '2007-05-19T20:43:13', 'uri': 'https://www.stewart-nelson.net/', 'number': 243, 'attributes': {'search': {'element': 'a', 'content': 'envisioneer extensible web-readiness'}, 'faq': {'element': 'button', 'content': 'disintermediate cross-media functionalities'}, 'about': {'element': 'div', 'content': 'empower intuitive convergence'}}}
# {'email': 'michael08@hotmail.com', 'timestamp': '1980-11-07T06:29:04', 'uri': 'https://www.le.info/home/', 'number': 27, 'attributes': {'author': {'element': 'a', 'content': 'morph interactive applications'}, 'index': {'element': 'button', 'content': 'e-enable transparent ROI'}, 'post': {'element': 'button', 'content': 'empower robust metrics'}, 'terms': {'element': 'button', 'content': 'optimize world-class markets'}}}
# {'email': 'anthony22@warren.com', 'timestamp': '1974-07-01T15:47:56', 'uri': 'https://www.sullivan.info/main.php', 'number': 249, 'attributes': {'home': {'element': 
