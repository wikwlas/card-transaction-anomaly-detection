# Real-Time Fraud Detection System

System wykrywania oszustw na transakcjach płatniczych w czasie rzeczywistym. Architektura opiera się na przetwarzaniu strumieniowym danych (Stream Processing) przy użyciu narzędzi Apache Flink, Apache Kafka, MongoDB oraz Python.

## Architektura systemu

1. **Generator (Python):** Skrypt symuluje ruch transakcyjny i wysyła JSON-y do Kafki (`transactions`).
2. **Silnik Detekcji (Flink - FraudDetector):** Analizuje strumień pod kątem reguł (częstotliwość, limity, odległości GPS) i generuje alarmy do Kafki (`alerts`).
3. **Inżestor Bazy (Flink - MongoIngestionJob):** Konsumuje oba topici z Kafki i zapisuje/aktualizuje dokumenty w MongoDB, dbając o spójność poprzez operacje typu Upsert.
4. **Analityka (Metabase):** Łączy się bezpośrednio z MongoDB, pozwalając na wizualizację incydentów na żywo.

---

## Wymagania wstępne

* **Docker** oraz **Docker Compose**
* **uv** (menedżer pakietów Pythona):
  * **Linux/macOS:** `curl -LsSf https://astral.sh/uv/install.sh | sh`
  * **Windows:** `powershell -c "irm https://astral.sh/uv/install.ps1 | iex"`

---

## Instrukcja uruchomienia

### Krok 1: Uruchomienie infrastruktury
W głównym folderze projektu (gdzie znajduje się plik `docker-compose.yml`) uruchom środowisko w tle:

```bash
docker compose up -d
```

*Kontenery, które wystartują: kafka, flink_jobmanager, flink_taskmanager, fraud_mongodb, metabase.*

### Krok 2: Przygotowanie środowiska Python
Zainstaluj wymagane zależności dla generatora danych:

```bash
uv sync
```

### Krok 3: Wdrożenie aplikacji Flink (Deploy)
Skrypt `deploy.sh` buduje aplikację Javową w kontenerze, przygotowuje Kafkę, anuluje stare procesy i uruchamia nową logikę analityczną:

```bash
chmod +x deploy.sh
./deploy.sh
```

*Działające procesy możesz podejrzeć w panelu: http://localhost:8081*

### Krok 4: Uruchomienie strumienia danych
W nowym oknie terminala uruchom generator symulujący transakcje i zachowania oszustów:

```bash
uv run producer.py
```

---

## Weryfikacja przepływu danych

### 1. Podgląd surowych danych (Kafka)
Sprawdzenie zwykłych transakcji:

```bash
docker exec fraud_kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic transactions --from-beginning
```

Sprawdzenie wygenerowanych alarmów:

```bash
docker exec fraud_kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic alerts --from-beginning
```

### 2. Sprawdzenie zapisu (MongoDB)
Wejdź do bazy danych:

```bash
docker exec -it fraud_mongodb mongosh
```

Wewnątrz konsoli `mongosh` wykonaj:

```javascript
use fraud_db;

// Pokaż 5 najnowszych transakcji (pełne struktury)
db.transactions.find().sort({ timestamp: -1 }).limit(5).toArray();

// Pokaż tylko wykryte oszustwa
db.transactions.find({ is_fraud: true }).toArray();

exit;
```

---

## Konfiguracja analityki w Metabase

Panel logowania znajduje się pod adresem: **http://localhost:3000**

### Podłączenie bazy danych
* **Database type:** MongoDB
* **Name:** System Detekcji
* **Host:** `fraud_mongodb`
* **Port:** `27017`
* **Database name:** `fraud_db`
* *Pola Username, Password i Authentication Database należy zostawić puste.*

### Ważne: Odświeżanie schematu
Metabase nie aktualizuje schematu dokumentów w czasie rzeczywistym. Jeśli Twoja logika we Flinku dodała nowe kolumny (np. `anomaly_type`), musisz wymusić synchronizację:
1. Przejdź do: **Ustawienia admina -> Databases -> System Detekcji**.
2. Kliknij na dole przycisk **Sync database schema now**.

### Tworzenie widoku rodzajów oszustw (Wykres Kołowy)
1. Wybierz z górnego paska **+ New -> Question**.
2. Wybierz bazę `System Detekcji` i tabelę `transactions`.
3. Kliknij przycisk **Summarize** i wybierz metrykę **Count of rows**.
4. W sekcji **Group by** wybierz pole `anomaly_type`.
5. Dodaj filtrowanie (**Filter**), aby ukryć czyste transakcje: `anomaly_type is not equal to NONE`.
6. Kliknij **Visualize**, a następnie w lewym dolnym rogu zmień typ wykresu z tabeli na **Pie**.