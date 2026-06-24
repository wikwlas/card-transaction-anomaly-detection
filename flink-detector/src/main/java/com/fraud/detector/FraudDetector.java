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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

public class FraudDetector {
    private static final ObjectMapper MAIN_MAPPER = new ObjectMapper();

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

        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((jsonStr, recordTimestamp) -> extractTransactionTimestampMillis(jsonStr));

        DataStream<String> stream = env.fromSource(source, watermarkStrategy, "Kafka Source");

        stream
            .keyBy(FraudDetector::extractCardId)
            .process(new AnomalyDetectionEngine())
            .sinkTo(sink);

        stream
            .keyBy(FraudDetector::extractCardId)
            .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
            .process(new TransactionBurstWindowFunction())
            .sinkTo(sink);

        env.execute("Flink Fraud Detection Engine");
    }

    private static String extractCardId(String jsonStr) {
        try {
            return MAIN_MAPPER.readTree(jsonStr).path("card_id").asText("UNKNOWN");
        } catch (Exception e) {
            return "UNKNOWN";
        }
    }

    private static long extractTransactionTimestampMillis(String jsonStr) {
        try {
            long timestampSeconds = MAIN_MAPPER.readTree(jsonStr).path("timestamp").asLong(0L);
            if (timestampSeconds > 0L) {
                return timestampSeconds * 1000L;
            }
        } catch (Exception ignored) {
            // Fall back to ingestion time when the event timestamp is malformed.
        }
        return System.currentTimeMillis();
    }

    public static class AnomalyDetectionEngine extends KeyedProcessFunction<String, String, String> {
        private static final double DEFAULT_CREDIT_LIMIT = 5000.0;

        private static final double HOEFFDING_DELTA = 1.0e-7;
        private static final int HOEFFDING_GRACE_PERIOD = 20;
        private static final double HOEFFDING_TIE_THRESHOLD = 0.05;
        private static final int HOEFFDING_MAX_DEPTH = 8;
        private static final int HOEFFDING_MIN_SAMPLES_LEAF = 10;
        private static final double HOEFFDING_ALERT_THRESHOLD = 0.62;

        private transient ValueState<String> lastTransactionState;
        private transient ValueState<HoeffdingTreeModel> treeState;
        private transient ValueState<AdaptiveThresholds> thresholdState;
        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>(
                    "last-transaction", 
                    TypeInformation.of(String.class)
            );
            lastTransactionState = getRuntimeContext().getState(descriptor);

            ValueStateDescriptor<HoeffdingTreeModel> treeDescriptor = new ValueStateDescriptor<>(
                    "hoeffding-tree",
                    TypeInformation.of(HoeffdingTreeModel.class)
            );
            treeState = getRuntimeContext().getState(treeDescriptor);

            ValueStateDescriptor<AdaptiveThresholds> thresholdDescriptor = new ValueStateDescriptor<>(
                    "adaptive-thresholds",
                    TypeInformation.of(AdaptiveThresholds.class)
            );
            thresholdState = getRuntimeContext().getState(thresholdDescriptor);
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            JsonNode currentTx = mapper.readTree(value);
            
            // Extracting fields
            double amount = currentTx.path("amount").asDouble();
            double creditLimit = currentTx.path("credit_limit").asDouble(DEFAULT_CREDIT_LIMIT); // Default if missing
            double currentLat = currentTx.path("gps").path("lat").asDouble();
            double currentLon = currentTx.path("gps").path("lon").asDouble();
            long currentTimestamp = currentTx.path("timestamp").asLong();
            double amountToLimitRatio = creditLimit > 0.0 ? amount / creditLimit : 0.0;
            int hour = Instant.ofEpochSecond(currentTimestamp).atZone(ZoneOffset.UTC).getHour();

            AdaptiveThresholds thresholds = thresholdState.value();
            if (thresholds == null) {
                thresholds = new AdaptiveThresholds();
            }

            double speed = 0.0;
            double distance = 0.0;
            double timeDiffSec = AdaptiveThresholds.UNKNOWN_TIME_DIFF_SEC;

            // 1. LIMIT EXCEEDED ANOMALY
            boolean ruleTriggered = false;
            if (amountToLimitRatio >= thresholds.limitRatioThreshold()) {
                ruleTriggered = true;
                out.collect(createAlertJson(currentTx, "LIMIT_EXCEEDED_ANOMALY", 
                        String.format("Amount %.2f PLN is near adaptive credit limit threshold %.2f%%.",
                                amount, thresholds.limitRatioThreshold() * 100.0)));
            }

            // 2. NIGHT OWL ANOMALY (01:00 - 04:00 UTC)
            if (hour >= 1 && hour <= 4) {
                ruleTriggered = true;
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
                timeDiffSec = currentTimestamp - lastTimestamp;
                if (timeDiffSec >= 0 && timeDiffSec < thresholds.frequencySecondsThreshold()) {
                    ruleTriggered = true;
                    out.collect(createAlertJson(currentTx, "FREQUENCY_ANOMALY", 
                            String.format("Rapid payment burst detected below adaptive %.2fs threshold.",
                                    thresholds.frequencySecondsThreshold())));
                }

                // 4. LOCATION ANOMALY (Impossible Travel)
                distance = haversine(lastLat, lastLon, currentLat, currentLon);
                double timeDiffHours = (currentTimestamp - lastTimestamp) / 3600.0;
                
                if (timeDiffHours > 0) {
                    speed = distance / timeDiffHours;
                    if (speed > thresholds.speedKmhThreshold()) {
                        ruleTriggered = true;
                        out.collect(createAlertJson(currentTx, "LOCATION_ANOMALY", 
                                String.format("Impossible travel! Speed: %.2f km/h, adaptive threshold: %.2f km/h",
                                        speed, thresholds.speedKmhThreshold())));
                    }
                }
            }

            TransactionFeatures features = new TransactionFeatures(
                    amountToLimitRatio,
                    amount,
                    hour,
                    timeDiffSec,
                    distance,
                    speed
            );

            HoeffdingTreeModel model = treeState.value();
            if (model == null) {
                model = new HoeffdingTreeModel(
                        HOEFFDING_DELTA,
                        HOEFFDING_GRACE_PERIOD,
                        HOEFFDING_TIE_THRESHOLD,
                        HOEFFDING_MAX_DEPTH,
                        HOEFFDING_MIN_SAMPLES_LEAF
                );
            }

            double fraudProbability = model.predictFraudProbability(features);
            if (fraudProbability >= HOEFFDING_ALERT_THRESHOLD) {
                out.collect(createAlertJson(currentTx, "HOEFFDING_TREE_ANOMALY",
                        String.format("Adaptive Hoeffding tree fraud probability: %.3f", fraudProbability)));
            }

            boolean label = extractFraudLabel(currentTx, ruleTriggered);
            model.learn(features, label);
            thresholds.observe(features, label);
            treeState.update(model);
            thresholdState.update(thresholds);

            lastTransactionState.update(value);
        }

        private boolean extractFraudLabel(JsonNode tx, boolean fallbackLabel) {
            JsonNode explicitBoolean = tx.get("is_fraud");
            if (explicitBoolean != null && explicitBoolean.isBoolean()) {
                return explicitBoolean.asBoolean();
            }

            String anomalyType = tx.path("anomaly_type").asText("");
            if (!anomalyType.isEmpty()) {
                return !"NONE".equalsIgnoreCase(anomalyType);
            }

            return fallbackLabel;
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

    public static class TransactionBurstWindowFunction
            extends ProcessWindowFunction<String, String, String, TimeWindow> {
        private static final long serialVersionUID = 1L;

        private static final int BURST_COUNT_THRESHOLD = 5;
        private static final int SUSPICIOUS_COUNT_THRESHOLD = 3;
        private static final double SPENDING_LIMIT_RATIO_THRESHOLD = 0.75;

        private transient ObjectMapper mapper;

        @Override
        public void process(String cardId, Context context, Iterable<String> transactions, Collector<String> out)
                throws Exception {
            ObjectMapper objectMapper = mapper();

            int count = 0;
            double totalAmount = 0.0;
            double creditLimit = AnomalyDetectionEngine.DEFAULT_CREDIT_LIMIT;
            long lastTimestamp = 0L;
            String lastTransactionId = "UNKNOWN";

            for (String txStr : transactions) {
                JsonNode tx = objectMapper.readTree(txStr);
                double amount = tx.path("amount").asDouble(0.0);

                count++;
                totalAmount += amount;
                creditLimit = tx.path("credit_limit").asDouble(creditLimit);

                long timestamp = tx.path("timestamp").asLong(0L);
                if (timestamp >= lastTimestamp) {
                    lastTimestamp = timestamp;
                    lastTransactionId = tx.path("transaction_id").asText("UNKNOWN");
                }
            }

            if (count >= BURST_COUNT_THRESHOLD) {
                out.collect(createWindowAlertJson(
                        objectMapper,
                        cardId,
                        lastTransactionId,
                        "WINDOW_BURST_ANOMALY",
                        String.format("%d transactions in a 10s sliding event-time window.", count),
                        context.window()
                ));
            }

            if (count >= SUSPICIOUS_COUNT_THRESHOLD
                    && creditLimit > 0.0
                    && totalAmount >= creditLimit * SPENDING_LIMIT_RATIO_THRESHOLD) {
                out.collect(createWindowAlertJson(
                        objectMapper,
                        cardId,
                        lastTransactionId,
                        "WINDOW_SPENDING_ANOMALY",
                        String.format(
                                "%d transactions total %.2f PLN in window, %.2f%% of credit limit.",
                                count,
                                totalAmount,
                                (totalAmount / creditLimit) * 100.0
                        ),
                        context.window()
                ));
            }
        }

        private ObjectMapper mapper() {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            return mapper;
        }

        private String createWindowAlertJson(ObjectMapper objectMapper, String cardId, String transactionId,
                                             String type, String details, TimeWindow window) throws Exception {
            ObjectNode alert = objectMapper.createObjectNode();
            alert.put("transaction_id", transactionId);
            alert.put("card_id", cardId);
            alert.put("anomaly_type", type);
            alert.put("details", details);
            alert.put("timestamp", window.getEnd() / 1000L);
            alert.put("window_start", window.getStart() / 1000L);
            alert.put("window_end", window.getEnd() / 1000L);
            return objectMapper.writeValueAsString(alert);
        }
    }

    public static class TransactionFeatures implements Serializable {
        private static final long serialVersionUID = 1L;

        private final double amountToLimitRatio;
        private final double amount;
        private final double hour;
        private final double timeDiffSec;
        private final double distanceKm;
        private final double speedKmh;

        public TransactionFeatures() {
            this(0.0, 0.0, 0.0, AdaptiveThresholds.UNKNOWN_TIME_DIFF_SEC, 0.0, 0.0);
        }

        public TransactionFeatures(double amountToLimitRatio, double amount, double hour,
                                   double timeDiffSec, double distanceKm, double speedKmh) {
            this.amountToLimitRatio = sanitize(amountToLimitRatio);
            this.amount = sanitize(amount);
            this.hour = sanitize(hour);
            this.timeDiffSec = sanitize(timeDiffSec);
            this.distanceKm = sanitize(distanceKm);
            this.speedKmh = sanitize(speedKmh);
        }

        public double value(int featureIndex) {
            switch (featureIndex) {
                case 0:
                    return amountToLimitRatio;
                case 1:
                    return amount;
                case 2:
                    return hour;
                case 3:
                    return timeDiffSec;
                case 4:
                    return distanceKm;
                case 5:
                    return speedKmh;
                default:
                    throw new IllegalArgumentException("Unknown feature index: " + featureIndex);
            }
        }

        public double amountToLimitRatio() {
            return amountToLimitRatio;
        }

        public double timeDiffSec() {
            return timeDiffSec;
        }

        public double speedKmh() {
            return speedKmh;
        }

        private static double sanitize(double value) {
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                return 0.0;
            }
            return value;
        }
    }

    public static class AdaptiveThresholds implements Serializable {
        private static final long serialVersionUID = 1L;

        public static final double UNKNOWN_TIME_DIFF_SEC = 3600.0;

        private final OnlineStats normalAmountRatioStats = new OnlineStats();
        private final OnlineStats normalTimeDiffStats = new OnlineStats();
        private final OnlineStats normalSpeedStats = new OnlineStats();

        public void observe(TransactionFeatures features, boolean fraud) {
            if (fraud) {
                return;
            }

            normalAmountRatioStats.observe(features.amountToLimitRatio());

            if (features.timeDiffSec() >= 0.0 && features.timeDiffSec() < UNKNOWN_TIME_DIFF_SEC) {
                normalTimeDiffStats.observe(features.timeDiffSec());
            }

            if (features.speedKmh() > 0.0) {
                normalSpeedStats.observe(features.speedKmh());
            }
        }

        public double limitRatioThreshold() {
            if (normalAmountRatioStats.count() < 30) {
                return 0.90;
            }
            return clamp(normalAmountRatioStats.mean() + 4.0 * normalAmountRatioStats.stdDev(), 0.65, 0.98);
        }

        public double frequencySecondsThreshold() {
            if (normalTimeDiffStats.count() < 20) {
                return 2.0;
            }
            return clamp(normalTimeDiffStats.mean() - 2.5 * normalTimeDiffStats.stdDev(), 1.0, 10.0);
        }

        public double speedKmhThreshold() {
            if (normalSpeedStats.count() < 20) {
                return 800.0;
            }
            return clamp(normalSpeedStats.mean() + 3.0 * normalSpeedStats.stdDev(), 500.0, 1600.0);
        }

        private static double clamp(double value, double min, double max) {
            return Math.max(min, Math.min(max, value));
        }
    }

    public static class OnlineStats implements Serializable {
        private static final long serialVersionUID = 1L;

        private long count;
        private double mean;
        private double m2;

        public void observe(double value) {
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                return;
            }

            count++;
            double delta = value - mean;
            mean += delta / count;
            double delta2 = value - mean;
            m2 += delta * delta2;
        }

        public long count() {
            return count;
        }

        public double mean() {
            return mean;
        }

        public double stdDev() {
            if (count < 2) {
                return 0.0;
            }
            return Math.sqrt(m2 / (count - 1));
        }
    }

    public static class HoeffdingTreeModel implements Serializable {
        private static final long serialVersionUID = 1L;

        private static final double[][] SPLIT_THRESHOLDS = new double[][] {
                {0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 1.00},
                {5.0, 20.0, 100.0, 300.0, 1000.0, 3000.0, 7000.0},
                {1.0, 4.0, 7.0, 12.0, 18.0, 22.0},
                {1.0, 2.0, 5.0, 30.0, 300.0, 1800.0},
                {1.0, 10.0, 100.0, 500.0, 1500.0, 5000.0},
                {50.0, 300.0, 800.0, 1200.0, 2500.0, 5000.0}
        };

        private final double delta;
        private final int gracePeriod;
        private final double tieThreshold;
        private final int maxDepth;
        private final int minSamplesLeaf;
        private final TreeNode root = new TreeNode(0);

        public HoeffdingTreeModel() {
            this(1.0e-7, 20, 0.05, 8, 10);
        }

        public HoeffdingTreeModel(double delta, int gracePeriod, double tieThreshold, int maxDepth, int minSamplesLeaf) {
            this.delta = delta;
            this.gracePeriod = gracePeriod;
            this.tieThreshold = tieThreshold;
            this.maxDepth = maxDepth;
            this.minSamplesLeaf = minSamplesLeaf;
        }

        public double predictFraudProbability(TransactionFeatures features) {
            TreeNode leaf = root.leafFor(features);
            return leaf.stats.fraudProbability();
        }

        public void learn(TransactionFeatures features, boolean fraud) {
            TreeNode leaf = root.leafFor(features);
            leaf.stats.observe(features, fraud);

            if (leaf.depth < maxDepth
                    && leaf.stats.total() >= minSamplesLeaf
                    && leaf.stats.total() % gracePeriod == 0) {
                trySplit(leaf);
            }
        }

        private void trySplit(TreeNode leaf) {
            SplitCandidate best = null;
            SplitCandidate secondBest = null;

            for (int featureIndex = 0; featureIndex < SPLIT_THRESHOLDS.length; featureIndex++) {
                for (int thresholdIndex = 0; thresholdIndex < SPLIT_THRESHOLDS[featureIndex].length; thresholdIndex++) {
                    SplitCandidate candidate = leaf.stats.candidate(featureIndex, thresholdIndex);
                    if (best == null || candidate.gain > best.gain) {
                        secondBest = best;
                        best = candidate;
                    } else if (secondBest == null || candidate.gain > secondBest.gain) {
                        secondBest = candidate;
                    }
                }
            }

            if (best == null || best.gain <= 0.0 || secondBest == null) {
                return;
            }

            double epsilon = Math.sqrt(Math.log(1.0 / delta) / (2.0 * leaf.stats.total()));
            if (best.gain - secondBest.gain > epsilon || epsilon < tieThreshold) {
                leaf.split(best);
            }
        }

        private static class TreeNode implements Serializable {
            private static final long serialVersionUID = 1L;

            private final int depth;
            private boolean leaf = true;
            private int splitFeatureIndex;
            private double splitThreshold;
            private TreeNode left;
            private TreeNode right;
            private LeafStats stats = new LeafStats();

            private TreeNode(int depth) {
                this.depth = depth;
            }

            private TreeNode leafFor(TransactionFeatures features) {
                if (leaf) {
                    return this;
                }
                if (features.value(splitFeatureIndex) <= splitThreshold) {
                    return left.leafFor(features);
                }
                return right.leafFor(features);
            }

            private void split(SplitCandidate candidate) {
                leaf = false;
                splitFeatureIndex = candidate.featureIndex;
                splitThreshold = SPLIT_THRESHOLDS[candidate.featureIndex][candidate.thresholdIndex];
                left = new TreeNode(depth + 1);
                right = new TreeNode(depth + 1);
                left.stats.seedPredictionPrior(candidate.leftLegit, candidate.leftFraud);
                right.stats.seedPredictionPrior(candidate.rightLegit, candidate.rightFraud);
                stats = null;
            }
        }

        private static class LeafStats implements Serializable {
            private static final long serialVersionUID = 1L;

            private int legitCount;
            private int fraudCount;
            private int priorLegitCount;
            private int priorFraudCount;
            private int[][] leftLegit = new int[SPLIT_THRESHOLDS.length][];
            private int[][] leftFraud = new int[SPLIT_THRESHOLDS.length][];

            private LeafStats() {
                for (int i = 0; i < SPLIT_THRESHOLDS.length; i++) {
                    leftLegit[i] = new int[SPLIT_THRESHOLDS[i].length];
                    leftFraud[i] = new int[SPLIT_THRESHOLDS[i].length];
                }
            }

            private void seedPredictionPrior(int legit, int fraud) {
                priorLegitCount = Math.max(0, legit);
                priorFraudCount = Math.max(0, fraud);
            }

            private void observe(TransactionFeatures features, boolean fraud) {
                if (fraud) {
                    fraudCount++;
                } else {
                    legitCount++;
                }

                for (int featureIndex = 0; featureIndex < SPLIT_THRESHOLDS.length; featureIndex++) {
                    double value = features.value(featureIndex);
                    for (int thresholdIndex = 0; thresholdIndex < SPLIT_THRESHOLDS[featureIndex].length; thresholdIndex++) {
                        if (value <= SPLIT_THRESHOLDS[featureIndex][thresholdIndex]) {
                            if (fraud) {
                                leftFraud[featureIndex][thresholdIndex]++;
                            } else {
                                leftLegit[featureIndex][thresholdIndex]++;
                            }
                        }
                    }
                }
            }

            private int total() {
                return legitCount + fraudCount;
            }

            private double fraudProbability() {
                return (fraudCount + priorFraudCount + 1.0)
                        / (total() + priorLegitCount + priorFraudCount + 2.0);
            }

            private SplitCandidate candidate(int featureIndex, int thresholdIndex) {
                int lLegit = leftLegit[featureIndex][thresholdIndex];
                int lFraud = leftFraud[featureIndex][thresholdIndex];
                int rLegit = legitCount - lLegit;
                int rFraud = fraudCount - lFraud;

                double gain = gini(legitCount, fraudCount)
                        - weightedGini(lLegit, lFraud, rLegit, rFraud);

                return new SplitCandidate(featureIndex, thresholdIndex, gain, lLegit, lFraud, rLegit, rFraud);
            }

            private double weightedGini(int lLegit, int lFraud, int rLegit, int rFraud) {
                int leftTotal = lLegit + lFraud;
                int rightTotal = rLegit + rFraud;
                int all = leftTotal + rightTotal;
                if (all == 0) {
                    return 0.0;
                }
                return (leftTotal / (double) all) * gini(lLegit, lFraud)
                        + (rightTotal / (double) all) * gini(rLegit, rFraud);
            }

            private double gini(int legit, int fraud) {
                int total = legit + fraud;
                if (total == 0) {
                    return 0.0;
                }
                double pLegit = legit / (double) total;
                double pFraud = fraud / (double) total;
                return 1.0 - pLegit * pLegit - pFraud * pFraud;
            }
        }

        private static class SplitCandidate implements Serializable {
            private static final long serialVersionUID = 1L;

            private final int featureIndex;
            private final int thresholdIndex;
            private final double gain;
            private final int leftLegit;
            private final int leftFraud;
            private final int rightLegit;
            private final int rightFraud;

            private SplitCandidate(int featureIndex, int thresholdIndex, double gain,
                                   int leftLegit, int leftFraud, int rightLegit, int rightFraud) {
                this.featureIndex = featureIndex;
                this.thresholdIndex = thresholdIndex;
                this.gain = gain;
                this.leftLegit = leftLegit;
                this.leftFraud = leftFraud;
                this.rightLegit = rightLegit;
                this.rightFraud = rightFraud;
            }
        }
    }
}
