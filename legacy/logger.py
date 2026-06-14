import json
from kafka import KafkaConsumer
from pymongo import MongoClient

print("Łączenie z MongoDB...")
mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['fraud_detection_system']
collection = db['all_transactions']

print("Inicjalizacja konsumenta Kafki...")
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest', 
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Konsument uruchomiony. Oczekiwanie na transakcje z Kafki i zapis do bazy...")

try:
    for message in consumer:
        transaction_data = message.value
        
        collection.insert_one(transaction_data)
        
        print(f"[ZAPISANO DO DB] Karta: {transaction_data['card_id']}, Kwota: {transaction_data['amount']} PLN")

except KeyboardInterrupt:
    print("\nZamykanie konsumenta...")
finally:
    mongo_client.close()