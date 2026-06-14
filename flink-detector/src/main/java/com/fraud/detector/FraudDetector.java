package com.fraud.detector;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

// New imports for Time handling
import java.time.Instant;
import java.time.ZoneOffset;

public class FraudDetector {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("transactions")
                .setGroupId("flink-detector-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("alerts")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                ).build();

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        stream
            .keyBy(jsonStr -> {
                try {
                    return new ObjectMapper().readTree(jsonStr).path("card_id").asText("UNKNOWN");
                } catch (Exception e) { return "UNKNOWN"; }
            })
            .process(new AnomalyDetectionEngine())
            .sinkTo(sink);

        env.execute("Flink Fraud Detection Engine");
    }

    public static class AnomalyDetectionEngine extends KeyedProcessFunction<String, String, String> {
        private transient ValueState<String> lastTransactionState;
        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>(
                    "last-transaction", 
                    TypeInformation.of(String.class)
            );
            lastTransactionState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            JsonNode currentTx = mapper.readTree(value);
            
            // Extracting fields
            double amount = currentTx.path("amount").asDouble();
            double creditLimit = currentTx.path("credit_limit").asDouble(5000.0); // Default if missing
            double currentLat = currentTx.path("gps").path("lat").asDouble();
            double currentLon = currentTx.path("gps").path("lon").asDouble();
            long currentTimestamp = currentTx.path("timestamp").asLong();

            // 1. LIMIT EXCEEDED ANOMALY
            if (amount >= creditLimit * 0.90) {
                out.collect(createAlertJson(currentTx, "LIMIT_EXCEEDED_ANOMALY", 
                        String.format("Amount %.2f PLN is near credit limit.", amount)));
            }

            // 2. NIGHT OWL ANOMALY (01:00 - 04:00 UTC)
            int hour = Instant.ofEpochSecond(currentTimestamp).atZone(ZoneOffset.UTC).getHour();
            if (hour >= 1 && hour <= 4) {
                out.collect(createAlertJson(currentTx, "NIGHT_OWL_ANOMALY", 
                        "Transaction detected during suspicious night hours."));
            }

            // State management for relative anomalies
            String lastTxStr = lastTransactionState.value();
            if (lastTxStr != null) {
                JsonNode lastTx = mapper.readTree(lastTxStr);
                
                double lastLat = lastTx.path("gps").path("lat").asDouble();
                double lastLon = lastTx.path("gps").path("lon").asDouble();
                long lastTimestamp = lastTx.path("timestamp").asLong();

                // 3. FREQUENCY ANOMALY
                long timeDiffSec = currentTimestamp - lastTimestamp;
                if (timeDiffSec >= 0 && timeDiffSec < 2) {
                    out.collect(createAlertJson(currentTx, "FREQUENCY_ANOMALY", 
                            "Rapid payment burst detected."));
                }

                // 4. LOCATION ANOMALY (Impossible Travel)
                double distance = haversine(lastLat, lastLon, currentLat, currentLon);
                double timeDiffHours = (currentTimestamp - lastTimestamp) / 3600.0;
                
                if (timeDiffHours > 0) {
                    double speed = distance / timeDiffHours;
                    if (speed > 800.0) { // Commercial jet speed
                        out.collect(createAlertJson(currentTx, "LOCATION_ANOMALY", 
                                String.format("Impossible travel! Speed: %.2f km/h", speed)));
                    }
                }
            }

            lastTransactionState.update(value);
        }

        private String createAlertJson(JsonNode tx, String type, String details) throws Exception {
            ObjectNode alert = mapper.createObjectNode();
            alert.put("transaction_id", tx.path("transaction_id").asText());
            alert.put("card_id", tx.path("card_id").asText());
            alert.put("anomaly_type", type);
            alert.put("details", details);
            alert.put("timestamp", tx.path("timestamp").asLong());
            return mapper.writeValueAsString(alert);
        }

        private double haversine(double lat1, double lon1, double lat2, double lon2) {
            double R = 6371; // Earth radius in km
            double dLat = Math.toRadians(lat2 - lat1);
            double dLon = Math.toRadians(lon2 - lon1);
            double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                       Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                       Math.sin(dLon / 2) * Math.sin(dLon / 2);
            double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
            return R * c;
        }
    }
}