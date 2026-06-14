import uuid
import json
import time
import random
from kafka import KafkaProducer

PRODUCER = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
TOPIC = 'transactions'

print("Generating a database of 10,000 payment cards...")
CARDS = []
for i in range(1, 10001):
    card_id = f"CARD_{i:05d}"
    user_id = f"USER_{random.randint(1, 6000)}"  
    credit_limit = float(random.choice([3000, 5000, 10000, 15000]))
    CARDS.append({
        "card_id": card_id,
        "user_id": user_id,
        "credit_limit": credit_limit
    })

def get_gaussian_location_poland():
    """
    Generates a GPS coordinate using a 2D Gaussian (Normal) distribution.
    The density peaks at the geographic center of Poland and fades out towards borders.
    """
    # 1. Center of the distribution (approx. geometry center of Poland)
    center_lat = 52.20
    center_lon = 19.40
    
    # 2. Standard deviation (spread). 
    # In a normal distribution, ~99.7% of data falls within 3 standard deviations (3*sigma).
    # Poland spans roughly 5 degrees in lat and 10 degrees in lon.
    sigma_lat = 0.85  # 3 * 0.85 = ~2.5 degrees north and south from center
    sigma_lon = 1.40  # 3 * 1.40 = ~4.2 degrees east and west from center
    
    # Generate coordinates based on the Gaussian bell curve
    lat = round(random.gauss(center_lat, sigma_lat), 4)
    lon = round(random.gauss(center_lon, sigma_lon), 4)
    
    return lat, lon

def generate_normal_data(card):
    """Generates a valid, standard transaction using non-uniform distributions for both amounts and space."""
    
    # Gaussian distribution for transaction amounts (already non-uniform)
    amount = random.gauss(60.0, 25.0)
    final_amount = max(2.0, round(amount, 2))

    # NEW: Gaussian distribution for spatial data (No more rectangle!)
    lat, lon = get_gaussian_location_poland()

    return {
        "transaction_id": str(uuid.uuid4()),
        "card_id": card["card_id"],
        "user_id": card["user_id"],
        "gps": {
            "lat": lat,
            "lon": lon
        },
        "amount": final_amount,
        "credit_limit": card["credit_limit"],
        "timestamp": int(time.time())
    }

print("Simulator running. Transactions are being sent to Kafka.")

try:
    while True:
        card = random.choice(CARDS)
        rand_action = random.random()

        if rand_action > 0.03:
            tx = generate_normal_data(card)
            PRODUCER.send(TOPIC, key=tx["card_id"].encode('utf-8'), value=tx)
        
        else:
            anomaly_type = random.choice(["high_amount", "impossible_travel", "carding"])
            
            if anomaly_type == "high_amount":
                # Anomaly 1: Large amount close to the card limit
                tx = generate_normal_data(card)
                tx["amount"] = round(card["credit_limit"] * random.uniform(0.85, 0.95), 2)
                PRODUCER.send(TOPIC, key=tx["card_id"].encode('utf-8'), value=tx)
                print(f"[INJECTED ANOMALY: AMOUNT] Card: {tx['card_id']}, Amount: {tx['amount']} PLN")
                
            elif anomaly_type == "impossible_travel":
                # Anomaly 2: Rapid location change (Impossible travel)
                tx1 = generate_normal_data(card)
                # FIX: Explicitly passing key and value to ensure correct partition assignment
                PRODUCER.send(TOPIC, key=tx1["card_id"].encode('utf-8'), value=tx1)
                
                tx2 = generate_normal_data(card)
                tx2["gps"] = {"lat": 40.7128, "lon": -74.0060} 
                PRODUCER.send(TOPIC, key=tx2["card_id"].encode('utf-8'), value=tx2)
                print(f"[INJECTED ANOMALY: LOCATION] Card: {tx2['card_id']} jumped to USA")
                
            elif anomaly_type == "carding":
                # Anomaly 3: Frequency anomaly (Series of rapid small payments)
                print(f"[INJECTED ANOMALY: FREQUENCY] Card under attack: {card['card_id']}")
                for _ in range(6):
                    tx = generate_normal_data(card)
                    tx["amount"] = round(random.uniform(5.0, 15.0), 2)
                    PRODUCER.send(TOPIC, key=tx["card_id"].encode('utf-8'), value=tx)
                    time.sleep(0.05)  # Very short time interval between transactions

        time.sleep(0.1)  # Delay between consecutive users in the simulation

except KeyboardInterrupt:
    print("\nShutting down...")
finally:
    PRODUCER.flush()