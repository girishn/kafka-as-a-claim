import os
import json
import time
import random
import string
from datetime import datetime, timezone
import certifi
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = os.environ['BOOTSTRAP_SERVERS']
API_KEY = os.environ['API_KEY']
API_SECRET = os.environ['API_SECRET']
SHIPMENTS_TOPIC = os.environ.get('SHIPMENTS_TOPIC', 'shipments.events')
ALERTS_TOPIC = os.environ.get('ALERTS_TOPIC', 'delivery.alerts')
RATE_PER_SEC = int(os.environ.get('RATE_PER_SEC', '10'))

conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': API_KEY,
    'sasl.password': API_SECRET,
    'ssl.ca.location': certifi.where(),
}

CITIES = [
    'New York NY', 'Los Angeles CA', 'Chicago IL', 'Houston TX',
    'Phoenix AZ', 'Austin TX', 'Seattle WA', 'Denver CO', 'Miami FL', 'Boston MA',
]
CARRIERS = ['FastShip', 'QuickDeliver', 'SpeedEx', 'RapidFreight']
STATUSES = ['CREATED', 'PICKED_UP', 'IN_TRANSIT', 'IN_TRANSIT', 'IN_TRANSIT',
            'OUT_FOR_DELIVERY', 'DELIVERED', 'DELIVERED', 'FAILED']


def rand_shipment_id():
    return 'SHP-' + ''.join(random.choices(string.digits, k=6))


def delivery_callback(err, msg):
    if err:
        print(f'Delivery error: {err}')


p = Producer(conf)
print(f'Producer started → {SHIPMENTS_TOPIC} / {ALERTS_TOPIC} at {RATE_PER_SEC} msg/sec')

interval = 1.0 / RATE_PER_SEC
while True:
    status = random.choice(STATUSES)
    origin, dest = random.sample(CITIES, 2)
    event = {
        'shipment_id': rand_shipment_id(),
        'status': status,
        'origin': origin,
        'destination': dest,
        'carrier': random.choice(CARRIERS),
        'timestamp': int(datetime.now(timezone.utc).timestamp() * 1000),
    }
    p.produce(SHIPMENTS_TOPIC, json.dumps(event).encode('utf-8'), callback=delivery_callback)

    if status == 'FAILED':
        alert = {
            'shipment_id': event['shipment_id'],
            'alert_type': 'DELIVERY_FAILED',
            'severity': 'HIGH',
            'message': f"Delivery failed: {origin} → {dest} via {event['carrier']}",
            'timestamp': event['timestamp'],
        }
        p.produce(ALERTS_TOPIC, json.dumps(alert).encode('utf-8'), callback=delivery_callback)

    p.poll(0)
    time.sleep(interval)
