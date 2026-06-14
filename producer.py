import uuid
import json
import time
import random
from confluent_kafka import Producer

# --- CONFIGURATION ---
# Using confluent-kafka for better stability and production standards
conf = {'bootstrap.servers': 'localhost:9092'}
PRODUCER = Producer(conf)
TOPIC = 'transactions'

# Helper function to send data to Kafka
def send_to_kafka(key, value):
    PRODUCER.produce(
        TOPIC, 
        key=key, 
        value=json.dumps(value).encode('utf-8')
    )
    # Serve delivery callbacks and keep the producer responsive
    PRODUCER.poll(0)

# --- DATA GENERATION SETUP ---
# Pre-generate user profiles (Home locations) to simulate geography
USER_PROFILES = {f"USER_{i}": {
    "lat": random.uniform(-60, 60), 
    "lon": random.uniform(-180, 180)
} for i in range(1, 6001)}

# Generate a pool of payment cards
print("Generating 10,000 payment cards...")
CARDS = []
for i in range(1, 10001):
    CARDS.append({
        "card_id": f"CARD_{i:05d}",
        "user_id": f"USER_{random.randint(1, 6000)}",
        "credit_limit": float(random.choice([3000, 5000, 10000, 15000]))
    })

def generate_gps(user_id, is_anomaly=False):
    """Generates GPS coordinates based on user home or global random for anomalies."""
    home = USER_PROFILES[user_id]
    if is_anomaly:
        # Jump to a random global location for anomaly simulation
        return {"lat": round(random.uniform(-90, 90), 4), "lon": round(random.uniform(-180, 180), 4)}
    else:
        # Keep transaction close to home (simulated radius)
        return {
            "lat": round(home["lat"] + random.uniform(-1, 1), 4),
            "lon": round(home["lon"] + random.uniform(-1, 1), 4)
        }

def create_tx(card, anomaly_type="NONE"):
    """Helper to structure the transaction dictionary."""
    return {
        "transaction_id": str(uuid.uuid4()),
        "card_id": card["card_id"],
        "user_id": card["user_id"],
        "amount": round(random.uniform(10.0, 300.0), 2),
        "gps": generate_gps(card["user_id"], is_anomaly=(anomaly_type != "NONE")),
        "timestamp": int(time.time()),
        "anomaly_type": anomaly_type
    }

print("Advanced Simulator running...")

try:
    while True:
        card = random.choice(CARDS)
        rand = random.random()

        # 90% chance for a normal transaction
        if rand > 0.10: 
            tx = create_tx(card)
            send_to_kafka(card["card_id"], tx)
        
        # 10% chance for an anomaly
        else:
            anomaly_type = random.choice(["high_amount", "impossible_travel", "night_owl", "carding"])
            
            if anomaly_type == "high_amount":
                tx = create_tx(card, anomaly_type="high_amount")
                tx["amount"] = round(card["credit_limit"] * random.uniform(0.85, 0.99), 2)
                send_to_kafka(card["card_id"], tx)

            elif anomaly_type == "impossible_travel":
                # Transaction 1: Home location
                t1 = create_tx(card)
                send_to_kafka(card["card_id"], t1)
                # Transaction 2: Distant location (Impossible travel)
                t2 = create_tx(card, anomaly_type="impossible_travel")
                send_to_kafka(card["card_id"], t2)
            
            elif anomaly_type == "night_owl":
                tx = create_tx(card, anomaly_type="night_owl")
                # Simulate transaction during night hours (01:00-04:00)
                tx["timestamp"] = int(time.time()) - random.randint(10000, 20000)
                send_to_kafka(card["card_id"], tx)

            elif anomaly_type == "carding":
                # Series of rapid micro-transactions
                for _ in range(5):
                    tx = create_tx(card, anomaly_type="carding")
                    tx["amount"] = round(random.uniform(1.0, 5.0), 2)
                    send_to_kafka(card["card_id"], tx)
                    time.sleep(0.01)

        # Small delay between event iterations
        time.sleep(0.1)

except KeyboardInterrupt:
    print("\nShutting down...")
finally:
    # Ensure all pending messages are delivered
    PRODUCER.flush()