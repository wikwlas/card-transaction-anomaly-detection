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
            JsonNode currentTx = mapper.readTree(value);
            
            JsonNode amountNode = currentTx.path("amount");
            JsonNode creditLimitNode = currentTx.path("credit_limit");
            // FIX: Dig deeper into the nested "gps" object
            JsonNode gpsLatNode = currentTx.path("gps").path("lat");
            JsonNode gpsLonNode = currentTx.path("gps").path("lon");
            JsonNode timestampNode = currentTx.path("timestamp");

            if (amountNode.isMissingNode() || creditLimitNode.isMissingNode() || 
                gpsLatNode.isMissingNode() || gpsLonNode.isMissingNode() || timestampNode.isMissingNode()) {
                System.err.println("[FLINK WARNING] Record skipped! Missing required structural fields.");
                return; 
            }

            // If validation passes, log it as a successfully received record
            System.out.println("[FLINK INCOMING] Received transaction: " + value);

            double amount = amountNode.asDouble();
            double creditLimit = creditLimitNode.asDouble();
            double currentLat = gpsLatNode.asDouble();
            double currentLon = gpsLonNode.asDouble();
            long currentTimestamp = timestampNode.asLong();

            if (amount >= creditLimit * 0.90) {
                out.collect(createAlertJson(currentTx, "LIMIT_EXCEEDED_ANOMALY", 
                        String.format("Amount %s PLN exceeds 90%% of the card limit (%s PLN)", amount, creditLimit)));
            }

            String lastTxStr = lastTransactionState.value();
            if (lastTxStr != null) {
                JsonNode lastTx = mapper.readTree(lastTxStr);
                
                // FIX: Also extract coordinates from the nested "gps" object for historical data
                double lastLat = lastTx.path("gps").path("lat").asDouble();
                double lastLon = lastTx.path("gps").path("lon").asDouble();
                long lastTimestamp = lastTx.path("timestamp").asLong();

                long timeDiffSec = currentTimestamp - lastTimestamp;
                if (timeDiffSec >= 0 && timeDiffSec < 2) {
                    out.collect(createAlertJson(currentTx, "FREQUENCY_ANOMALY", 
                            String.format("Detected a series of rapid payments. Interval: %s sec.", timeDiffSec)));
                }

                double distance = haversine(lastLat, lastLon, currentLat, currentLon);
                double timeDiffHours = (currentTimestamp - lastTimestamp) / 3600.0;
                
                if (timeDiffHours > 0) {
                    double speed = distance / timeDiffHours;
                    if (speed > 800.0) {
                        out.collect(createAlertJson(currentTx, "LOCATION_ANOMALY", 
                                String.format("Impossible travel! Card covered %s km at a speed of %.2f km/h", (int)distance, speed)));
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