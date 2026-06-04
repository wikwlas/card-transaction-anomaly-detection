import json
from kafka import KafkaConsumer
from pymongo import MongoClient

# 1. Połączenie z bazą danych MongoDB (uruchomioną w Dockerze)
print("Łączenie z MongoDB...")
mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['fraud_detection_system']
collection = db['all_transactions']

# 2. Inicjalizacja Konsumenta Kafki
print("Inicjalizacja konsumenta Kafki...")
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',  # Czytaj najnowsze wiadomości na bieżąco
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Konsument uruchomiony. Oczekiwanie na transakcje z Kafki i zapis do bazy...")

# 3. Pętla odbierająca dane i zapisująca je do MongoDB
try:
    for message in consumer:
        transaction_data = message.value
        
        # Zapis dokumentu do bazy MongoDB
        collection.insert_one(transaction_data)
        
        # Wyświetlenie krótkiego potwierdzenia w konsoli
        print(f"[ZAPISANO DO DB] Karta: {transaction_data['card_id']}, Kwota: {transaction_data['amount']} PLN")

except KeyboardInterrupt:
    print("\nZamykanie konsumenta...")
finally:
    mongo_client.close()