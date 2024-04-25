
import asyncio
from dataclasses import dataclass, field
import json
import random

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker


faker = Faker()

BROKER_URL = "PLAINTEXT://localhost:9092"


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})
    while True:
        p.produce(topic_name, ClickEvent().serialize())
        await asyncio.sleep(1.0)


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0"})
    c.subscribe([topic_name])
    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            #
            # Load the value as JSON and then create a ClickEvent object. The web team has
            #       told us to expect the keys "email", "uri", and "timestamp".
            clickevent_json = json.loads(message.value())
            try:
                print(
                    ClickEvent(
                        email=clickevent_json["email"],
                        timestamp=clickevent_json["timestamp"],
                        uri=clickevent_json["uri"],
                    )
                )
            except KeyError as e:
                print(f"Failed to unpack message {e}")
        await asyncio.sleep(1.0)
            

def main():
    """Checks for topic and creates the topic if it does not exist"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})

    try:
        asyncio.run(produce_consume("com.udacity.lesson3.exercise1.clicks"))
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    await t1
    await t2


@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)

    num_calls = 0

    def serialize(self):
        email_key = "email" if ClickEvent.num_calls < 10 else "user_email"
        ClickEvent.num_calls += 1
        return json.dumps(
            {"uri": self.uri, "timestamp": self.timestamp, email_key: self.email}
        )

    @classmethod
    def deserialize(self, json_data):
        purchase_json = json.loads(json_data)
        return Purchase(
            username=purchase_json["username"],
            currency=purchase_json["currency"],
            amount=purchase_json["amount"],
        )


if __name__ == "__main__":
    main()



# Output

# root@3d69799aa072825783b75e321a5047d8f7da8e2b-6c7585ffd7-rmlw8:/workspace/home# python exercise3.1.py
# no message received by consumer
# no message received by consumer
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# ^Cshutting down
# root@3d69799aa072825783b75e321a5047d8f7da8e2b-6c7585ffd7-rmlw8:/workspace/home# python exercise3.1.py
# no message received by consumer
# no message received by consumer
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# ^Cshutting down
# root@3d69799aa072825783b75e321a5047d8f7da8e2b-6c7585ffd7-rmlw8:/workspace/home# python exercise3.1.py
# no message received by consumer
# no message received by consumer
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# Failed to unpack message 'email'
# ClickEvent(email='davidthomas@watson-howell.com', timestamp='2001-10-05T22:54:02', uri='http://diaz.com/')
# ClickEvent(email='jimmy52@yahoo.com', timestamp='2023-12-11T11:00:57', uri='https://www.blevins.com/search/post.html')
# ClickEvent(email='jeffreyvalencia@collins.com', timestamp='2002-07-29T01:42:16', uri='http://shaw.com/tag/wp-content/list/index.php')
# ClickEvent(email='karen64@williams-wheeler.com', timestamp='1986-12-11T08:18:42', uri='http://www.barrett-patterson.com/faq/')
# ClickEvent(email='ntaylor@hotmail.com', timestamp='1996-09-23T23:42:32', uri='http://www.pratt.com/home.html')
# ClickEvent(email='francisco52@gmail.com', timestamp='2016-10-11T16:44:51', uri='https://www.harmon-jimenez.net/post/')
# ClickEvent(email='jenkinskevin@hill-cunningham.com', timestamp='2017-11-05T01:35:15', uri='http://boyd.com/main.htm')
# ClickEvent(email='justinpatel@hotmail.com', timestamp='1981-08-26T02:06:45', uri='https://nguyen.com/search/explore/index/')
# ClickEvent(email='walkerjean@yahoo.com', timestamp='2023-11-07T18:00:07', uri='https://ramirez-lopez.biz/explore/search.htm')
# ClickEvent(email='molly03@miller.com', timestamp='1997-10-29T10:30:50', uri='https://www.ballard-pittman.com/posts/explore/category/privacy.jsp')
# Failed to unpack message 'email'
# ClickEvent(email='karen66@gray.org', timestamp='1997-08-31T09:23:33', uri='http://www.martinez.net/privacy.php')
# ClickEvent(email='martinmelissa@turner.com', timestamp='1996-01-26T00:38:10', uri='http://smith.biz/search/')
# ClickEvent(email='toddbennett@patel.biz', timestamp='1976-11-04T05:18:40', uri='https://www.weaver-black.net/wp-content/categories/faq/')
# ClickEvent(email='matthewstyler@yahoo.com', timestamp='2021-12-31T21:15:53', uri='https://martin.info/search/post.html')
# ClickEvent(email='colemantammy@gmail.com', timestamp='1988-08-12T11:40:25', uri='http://sweeney.net/post.php')
# ClickEvent(email='anawhitney@hotmail.com', timestamp='1977-01-18T15:29:42', uri='https://campbell.com/')
# ClickEvent(email='nicole58@rowland.org', timestamp='2007-07-25T22:03:18', uri='https://bridges-garner.com/terms.html')
# ClickEvent(email='taylormiller@tanner-stevenson.net', timestamp='1988-10-17T06:58:19', uri='https://www.burch-hebert.com/terms.php')
# ClickEvent(email='tclark@frey-singleton.com', timestamp='1997-06-15T11:29:13', uri='http://www.stevenson.org/tags/explore/category/category.htm')
# ClickEvent(email='smithdanny@smith.com', timestamp='1971-03-30T23:09:07', uri='https://www.burke.com/register/')
# root@3d69799aa072825783b75e321a5047d8f7da8e2b-6c7585ffd7-rmlw8:/workspace/home#  ttps://www.hayes.com/wp-content/wp-conten
