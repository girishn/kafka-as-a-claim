import os
import json
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError

BOOTSTRAP_SERVERS = os.environ['BOOTSTRAP_SERVERS']
API_KEY = os.environ['API_KEY']
API_SECRET = os.environ['API_SECRET']
TOPIC = os.environ.get('TOPIC', 'delivery.alerts')
CONSUMER_GROUP = os.environ.get('CONSUMER_GROUP', 'alerts-consumer')

conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': API_KEY,
    'sasl.password': API_SECRET,
    'group.id': CONSUMER_GROUP,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
}

c = Consumer(conf)
c.subscribe([TOPIC])
print(f'Alerts consumer started — group={CONSUMER_GROUP}, topic={TOPIC}')

SEVERITY_EMOJI = {'HIGH': '🚨', 'MEDIUM': '⚠️ ', 'LOW': 'ℹ️ '}

while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        print(f'Consumer error: {msg.error()}')
        continue

    alert = json.loads(msg.value().decode('utf-8'))
    now = datetime.now(timezone.utc).strftime('%H:%M:%S')
    severity = alert.get('severity', 'UNKNOWN')
    emoji = SEVERITY_EMOJI.get(severity, '❓')
    print(f'[{now}] {emoji} [{severity}] {alert["shipment_id"]} — {alert.get("message", "")}')
