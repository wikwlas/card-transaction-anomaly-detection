import json
import time
import random
from kafka import KafkaProducer

PRODUCER = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
TOPIC = 'transactions'

print("Generowanie bazy 10 000 kart płatniczych...")
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

def generate_normal_data(card):
    """Generuje poprawną, standardową transakcję z terenu Polski."""
    return {
        "card_id": card["card_id"],
        "user_id": card["user_id"],
        "gps": {
            "lat": round(random.uniform(50.0, 54.0), 4),
            "lon": round(random.uniform(14.0, 24.0), 4)
        },
        "amount": round(random.uniform(10.0, 300.0), 2),
        "credit_limit": card["credit_limit"],
        "timestamp": int(time.time())
    }

print("Symulator uruchomiony. Transakcje trafiają do Kafki.")

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
                # Anomalia 1: ogromna kwota blisko limitu karty
                tx = generate_normal_data(card)
                tx["amount"] = round(card["credit_limit"] * random.uniform(0.85, 0.95), 2)
                PRODUCER.send(TOPIC, key=tx["card_id"].encode('utf-8'), value=tx)
                print(f"[WSTRZYKNIĘTO ANOMALIĘ: KWOTA] Karta: {tx['card_id']}, Kwota: {tx['amount']} PLN")
                
            elif anomaly_type == "impossible_travel":
                # Anomalia 2: szybka zmiana lokalizacji
                tx1 = generate_normal_data(card)
                PRODUCER.send(TOPIC, tx1)
                
                tx2 = generate_normal_data(card)
                tx2["gps"] = {"lat": 40.7128, "lon": -74.0060} 
                PRODUCER.send(TOPIC, tx2)
                print(f"[WSTRZYKNIĘTO ANOMALIĘ: LOKALIZACJA] Karta: {tx2['card_id']} przeskoczyła do USA")
                
            elif anomaly_type == "carding":
                # Anomalia 3: częstotliwość
                print(f"[WSTRZYKNIĘTO ANOMALIĘ: CZĘSTOTLIWOŚĆ] Atak na kartę: {card['card_id']}")
                for _ in range(6):
                    tx = generate_normal_data(card)
                    tx["amount"] = round(random.uniform(5.0, 15.0), 2)
                    PRODUCER.send(TOPIC, key=tx["card_id"].encode('utf-8'), value=tx)
                    time.sleep(0.05)  # Bardzo mały odstęp czasu

        time.sleep(0.1)  # Odstęp między kolejnymi użytkownikami w symulacji

except KeyboardInterrupt:
    print("\nZamykanie...")
finally:
    PRODUCER.flush()