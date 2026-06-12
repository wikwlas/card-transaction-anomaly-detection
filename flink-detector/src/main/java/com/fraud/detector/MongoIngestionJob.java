package com.fraud.detector;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;

public class MongoIngestionJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. Source for normal transactions
        KafkaSource<String> txSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("transactions")
                .setGroupId("mongo-transactions-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 2. Source for anomaly alerts
        KafkaSource<String> alertSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("alerts")
                .setGroupId("mongo-alerts-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> txStream = env.fromSource(txSource, WatermarkStrategy.noWatermarks(), "Kafka Transactions");
        DataStream<String> alertStream = env.fromSource(alertSource, WatermarkStrategy.noWatermarks(), "Kafka Alerts");

        // Save normal transactions to Mongo (is_fraud = false)
        txStream.addSink(new MongoSinkFunction(false));

        // Save alerts to Mongo (upsert/update is_fraud = true)
        alertStream.addSink(new MongoSinkFunction(true));

        env.execute("Flink MongoDB Ingestion Engine");
    }

    public static class MongoSinkFunction extends RichSinkFunction<String> {
        private final boolean isAlertStream;
        private transient MongoClient mongoClient;
        private transient MongoCollection<Document> collection;
        private final ObjectMapper mapper = new ObjectMapper();

        public MongoSinkFunction(boolean isAlertStream) {
            this.isAlertStream = isAlertStream;
        }

        @Override
        public void open(Configuration parameters) {
            // Connecting to the fraud_mongodb container configured in docker-compose
            mongoClient = MongoClients.create("mongodb://fraud_mongodb:27017");
            MongoDatabase database = mongoClient.getDatabase("fraud_db");
            collection = database.getCollection("transactions");
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            JsonNode node = mapper.readTree(value);
            String txId = node.path("transaction_id").asText("UNKNOWN");

            if ("UNKNOWN".equals(txId)) {
                return; // Ignore records without a valid transaction_id
            }

            Document query = new Document("_id", txId);

            if (!isAlertStream) {
                // Normal transaction stream -> Extract ALL fields
                Document setFields = new Document()
                        .append("card_id", node.path("card_id").asText("UNKNOWN"))
                        // Added UserID extraction
                        .append("user_id", node.path("user_id").asText("UNKNOWN"))
                        .append("amount", node.path("amount").asDouble(0.0))
                        .append("timestamp", node.path("timestamp").asLong(0L))
                        // Added credit_limit
                        .append("credit_limit", node.path("credit_limit").asDouble(0.0));

                // Added safe extraction of nested GPS coordinates
                if (node.has("gps") && !node.path("gps").isNull()) {
                    setFields.append("gps", new Document()
                            .append("lat", node.path("gps").path("lat").asDouble(0.0))
                            .append("lon", node.path("gps").path("lon").asDouble(0.0)));
                }

                // $set updates the data fields. 
                // $setOnInsert ONLY sets is_fraud to false if the document is completely new.
                // This prevents overwriting an alert if the alert arrived a millisecond earlier.
                Document update = new Document("$set", setFields)
                        .append("$setOnInsert", new Document("is_fraud", false));
                
                collection.updateOne(query, update, new UpdateOptions().upsert(true));
                System.out.println("[MONGO] Saved full transaction: " + txId);
                
            } else {
                // Alert stream -> Update the existing transaction with fraud details
                Document update = new Document("$set", new Document()
                        .append("is_fraud", true)
                        .append("anomaly_type", node.path("anomaly_type").asText("UNKNOWN"))
                        .append("details", node.path("details").asText("UNKNOWN")));

                collection.updateOne(query, update, new UpdateOptions().upsert(true));
                System.out.println("[MONGO ALERT!!!] Flagged transaction as fraud: " + txId);
            }
        }

        @Override
        public void close() {
            if (mongoClient != null) {
                mongoClient.close();
            }
        }
    }
}