import uuid
import json
import time
import random
from confluent_kafka import Producer

# --- CONFIGURATION ---
conf = {'bootstrap.servers': 'localhost:9092'}
PRODUCER = Producer(conf)
TOPIC = 'transactions'

def send_to_kafka(key, value):
    PRODUCER.produce(
        TOPIC, 
        key=key, 
        value=json.dumps(value).encode('utf-8')
    )
    PRODUCER.poll(0)

# --- GEOGRAPHY: ROUGH LAND BOUNDING BOXES ---
# Defining rough rectangular boundaries for major landmasses to avoid deep oceans
LAND_REGIONS = [
    {"name": "Europe", "lat": (35.0, 70.0), "lon": (-10.0, 40.0)},
    {"name": "North America", "lat": (15.0, 70.0), "lon": (-130.0, -60.0)},
    {"name": "South America", "lat": (-55.0, 15.0), "lon": (-80.0, -35.0)},
    {"name": "Africa", "lat": (-35.0, 35.0), "lon": (-15.0, 50.0)},
    {"name": "Asia", "lat": (5.0, 70.0), "lon": (40.0, 130.0)},
    {"name": "Australia", "lat": (-40.0, -10.0), "lon": (110.0, 155.0)}
]

def get_random_land_gps():
    """Picks a random continent box, then generates coordinates within it."""
    region = random.choice(LAND_REGIONS)
    return {
        "lat": round(random.uniform(region["lat"][0], region["lat"][1]), 4),
        "lon": round(random.uniform(region["lon"][0], region["lon"][1]), 4)
    }

# --- DATA GENERATION SETUP ---
# Pre-generate user profiles with home locations strictly on rough landmasses
USER_PROFILES = {f"USER_{i}": get_random_land_gps() for i in range(1, 6001)}

print("Generating 10,000 payment cards...")
CARDS = []
for i in range(1, 10001):
    CARDS.append({
        "card_id": f"CARD_{i:05d}",
        "user_id": f"USER_{random.randint(1, 6000)}",
        "credit_limit": float(random.choice([3000, 5000, 10000, 15000]))
    })

def generate_gps(user_id, is_anomaly=False):
    """Generates GPS coordinates based on user home or global land for anomalies."""
    home = USER_PROFILES[user_id]
    if is_anomaly:
        # Jump to a random LAND location for anomaly simulation
        return get_random_land_gps()
    else:
        # Keep transaction close to home (simulated radius ~100km)
        return {
            "lat": round(home["lat"] + random.uniform(-1, 1), 4),
            "lon": round(home["lon"] + random.uniform(-1, 1), 4)
        }

def create_tx(card, anomaly_type="NONE"):
    return {
        "transaction_id": str(uuid.uuid4()),
        "card_id": card["card_id"],
        "user_id": card["user_id"],
        "amount": round(random.uniform(10.0, 300.0), 2),
        "gps": generate_gps(card["user_id"], is_anomaly=(anomaly_type != "NONE")),
        "timestamp": int(time.time()),
        "anomaly_type": anomaly_type
    }

print("Advanced Simulator (Land-only) running...")

try:
    while True:
        card = random.choice(CARDS)
        rand = random.random()

        if rand > 0.10: 
            tx = create_tx(card)
            send_to_kafka(card["card_id"], tx)
        
        else:
            anomaly_type = random.choice(["high_amount", "impossible_travel", "night_owl", "carding"])
            
            if anomaly_type == "high_amount":
                tx = create_tx(card, anomaly_type="high_amount")
                tx["amount"] = round(card["credit_limit"] * random.uniform(0.85, 0.99), 2)
                send_to_kafka(card["card_id"], tx)

            elif anomaly_type == "impossible_travel":
                t1 = create_tx(card)
                send_to_kafka(card["card_id"], t1)
                t2 = create_tx(card, anomaly_type="impossible_travel")
                send_to_kafka(card["card_id"], t2)
            
            elif anomaly_type == "night_owl":
                tx = create_tx(card, anomaly_type="night_owl")
                tx["timestamp"] = int(time.time()) - random.randint(10000, 20000)
                send_to_kafka(card["card_id"], tx)

            elif anomaly_type == "carding":
                for _ in range(5):
                    tx = create_tx(card, anomaly_type="carding")
                    tx["amount"] = round(random.uniform(1.0, 5.0), 2)
                    send_to_kafka(card["card_id"], tx)
                    time.sleep(0.01)

        time.sleep(0.1)

except KeyboardInterrupt:
    print("\nShutting down...")
finally:
    PRODUCER.flush()
