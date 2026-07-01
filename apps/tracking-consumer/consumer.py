import os
import json
import time
from datetime import datetime, timezone
import certifi
from confluent_kafka import Consumer, KafkaError

BOOTSTRAP_SERVERS = os.environ['BOOTSTRAP_SERVERS']
API_KEY = os.environ['API_KEY']
API_SECRET = os.environ['API_SECRET']
TOPIC = os.environ.get('TOPIC', 'shipments.events')
CONSUMER_GROUP = os.environ.get('CONSUMER_GROUP', 'tracking-consumer')
# Deliberately slow — builds consumer lag so KEDA demonstrates scaling
PROCESS_DELAY = float(os.environ.get('PROCESS_DELAY_SEC', '1.0'))

conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': API_KEY,
    'sasl.password': API_SECRET,
    'ssl.ca.location': certifi.where(),
    'group.id': CONSUMER_GROUP,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
}

c = Consumer(conf)
c.subscribe([TOPIC])
print(f'Tracking consumer started — group={CONSUMER_GROUP}, topic={TOPIC}, delay={PROCESS_DELAY}s/msg')

STATUS_EMOJI = {
    'CREATED': '📦',
    'PICKED_UP': '🚚',
    'IN_TRANSIT': '🛣️ ',
    'OUT_FOR_DELIVERY': '🏠',
    'DELIVERED': '✅',
    'FAILED': '❌',
}

while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        print(f'Consumer error: {msg.error()}')
        continue

    event = json.loads(msg.value().decode('utf-8'))
    time.sleep(PROCESS_DELAY)

    now = datetime.now(timezone.utc).strftime('%H:%M:%S')
    emoji = STATUS_EMOJI.get(event.get('status', ''), '❓')
    print(
        f'[{now}] {emoji} {event["shipment_id"]} '
        f'→ {event["status"]} '
        f'({event["origin"]} → {event["destination"]}) '
        f'via {event["carrier"]}'
    )
