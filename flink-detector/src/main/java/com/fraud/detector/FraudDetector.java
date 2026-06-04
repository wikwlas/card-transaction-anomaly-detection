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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

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
            .keyBy(new KeySelector<String, String>() {
                @Override
                public String getKey(String jsonStr) throws Exception {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode node = mapper.readTree(jsonStr);
                    return node.path("card_id").asText("UNKNOWN");
                }
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
            System.out.println("[FLINK INCOMING] Odebrano transakcje: " + value);
            
            JsonNode currentTx = mapper.readTree(value);
            
            JsonNode amountNode = currentTx.path("amount");
            JsonNode creditLimitNode = currentTx.path("credit_limit");
            JsonNode gpsLatNode = currentTx.path("gps_lat");
            JsonNode gpsLonNode = currentTx.path("gps_lon");
            JsonNode timestampNode = currentTx.path("timestamp");

            if (amountNode.isMissingNode() || creditLimitNode.isMissingNode() || 
                gpsLatNode.isMissingNode() || gpsLonNode.isMissingNode() || timestampNode.isMissingNode()) {
                System.err.println("[FLINK OSTRZEŻENIE] Pominięto rekord! Brakuje wymaganych pól strukturalnych.");
                return; 
            }

            double amount = amountNode.asDouble();
            double creditLimit = creditLimitNode.asDouble();
            double currentLat = gpsLatNode.asDouble();
            double currentLon = gpsLonNode.asDouble();
            long currentTimestamp = timestampNode.asLong();

            if (amount >= creditLimit * 0.90) {
                out.collect(createAlertJson(currentTx, "LIMIT_EXCEEDED_ANOMALY", 
                        String.format("Kwota %s PLN przekracza 90%% limitu karty (%s PLN)", amount, creditLimit)));
            }

            String lastTxStr = lastTransactionState.value();
            if (lastTxStr != null) {
                JsonNode lastTx = mapper.readTree(lastTxStr);
                
                double lastLat = lastTx.path("gps_lat").asDouble();
                double lastLon = lastTx.path("gps_lon").asDouble();
                long lastTimestamp = lastTx.path("timestamp").asLong();

                long timeDiffSec = currentTimestamp - lastTimestamp;
                if (timeDiffSec >= 0 && timeDiffSec < 2) {
                    out.collect(createAlertJson(currentTx, "FREQUENCY_ANOMALY", 
                            String.format("Wykryto serie szybkich płatności. Odstęp: %s sek.", timeDiffSec)));
                }

                double distance = haversine(lastLat, lastLon, currentLat, currentLon);
                double timeDiffHours = (currentTimestamp - lastTimestamp) / 3600.0;
                
                if (timeDiffHours > 0) {
                    double speed = distance / timeDiffHours;
                    if (speed > 800.0) {
                        out.collect(createAlertJson(currentTx, "LOCATION_ANOMALY", 
                                String.format("Niemożliwa podróż! Karta pokonała %s km z prędkością %.2f km/h", (int)distance, speed)));
                    }
                }
            }

            lastTransactionState.update(value);
        }

        private String createAlertJson(JsonNode tx, String type, String details) throws Exception {
            ObjectNode alert = mapper.createObjectNode();
            alert.put("card_id", tx.path("card_id").asText("UNKNOWN"));
            alert.put("anomaly_type", type);
            alert.put("details", details);
            alert.put("timestamp", tx.path("timestamp").asLong());
            alert.put("amount", tx.path("amount").asDouble());
            return mapper.writeValueAsString(alert);
        }

        private double haversine(double lat1, double lon1, double lat2, double lon2) {
            double R = 6371;
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