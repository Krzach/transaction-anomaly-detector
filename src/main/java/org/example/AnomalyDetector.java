package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class AnomalyDetector {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("transactions-topic")
                .setGroupId("flink-anomaly-detector-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("alarms-topic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        DataStream<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        dataStream
                .keyBy(value -> {
                    try {
                        return new ObjectMapper().readTree(value).get("card_id").asText();
                    } catch (Exception e) {
                        return "unknown";
                    }
                })
                .process(new AnomalyProcessFunction())
                .sinkTo(sink);

        env.execute("Transaction Anomaly Detector");
    }

    public static class AnomalyProcessFunction extends KeyedProcessFunction<String, String, String> {
        private transient ValueState<History> historyState;
        private static final ObjectMapper mapper = new ObjectMapper();

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<History> descriptor = new ValueStateDescriptor<>(
                    "card_history",
                    Types.POJO(History.class)
            );
            historyState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            try {
                JsonNode transaction = mapper.readTree(value);
                String cardId = transaction.has("card_id") ? transaction.get("card_id").asText() : null;
                JsonNode location = transaction.get("location");
                double amount = transaction.has("amount") ? transaction.get("amount").asDouble() : 0.0;

                if (cardId == null) return;

                History history = historyState.value();
                if (history == null) {
                    history = new History();
                }

                if (history.amounts.size() >= 3) {
                    double mean = history.amounts.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
                    double variance = history.amounts.stream().mapToDouble(x -> Math.pow(x - mean, 2)).sum() / history.amounts.size();
                    double stdDev = variance > 0 ? Math.sqrt(variance) : 0;

                    if (stdDev > 0) {
                        double zScore = (amount - mean) / stdDev;
                        if (zScore > 3) {
                            out.collect(mapper.writeValueAsString(new Alert(
                                    "statistical_amount_anomaly", cardId, amount, round(mean), round(zScore),
                                    "Kwota transakcji znacznie odbiega od średniej."
                            )));
                        }
                    } else if (mean > 0 && amount > mean * 5) {
                        out.collect(mapper.writeValueAsString(new Alert(
                                "amount_spike_anomaly", cardId, amount, round(mean), null,
                                "Nagły, bardzo duży wzrost kwoty transakcji."
                        )));
                    }
                }

                long currentTimestamp = ctx.timerService().currentProcessingTime();
                if (!history.locations.isEmpty() && !history.timestamps.isEmpty() && location != null && location.has("longitude") && location.has("latitude")) {
                    Location lastLocation = history.locations.get(history.locations.size() - 1);
                    long lastTimestamp = history.timestamps.get(history.timestamps.size() - 1);

                    double distance = haversine(
                            lastLocation.longitude, lastLocation.latitude,
                            location.get("longitude").asDouble(), location.get("latitude").asDouble()
                    );
                    double timeDeltaSeconds = (currentTimestamp - lastTimestamp) / 1000.0;

                    if (timeDeltaSeconds > 0) {
                        double speedKmh = (distance / timeDeltaSeconds) * 3600;
                        if (speedKmh > 900 && distance > 50) {
                            out.collect(mapper.writeValueAsString(new AlertSpeed(
                                    "location_speed_anomaly", cardId, round(speedKmh), round(distance),
                                    "Nierealistyczna odległość pokonana w zbyt krótkim czasie."
                            )));
                        }
                    }
                }

                history.amounts.add(amount);
                if (location != null && location.has("longitude") && location.has("latitude")) {
                    history.locations.add(new Location(location.get("longitude").asDouble(), location.get("latitude").asDouble()));
                } else {
                     history.locations.add(new Location(0.0, 0.0)); // fallback
                }
                history.timestamps.add(currentTimestamp);

                if (history.amounts.size() > 10) {
                    history.amounts.remove(0);
                    history.locations.remove(0);
                    history.timestamps.remove(0);
                }

                historyState.update(history);

            } catch (Exception e) {
                // Ignore parsing errors for now
            }
        }

        private double haversine(double lon1, double lat1, double lon2, double lat2) {
            lon1 = Math.toRadians(lon1);
            lat1 = Math.toRadians(lat1);
            lon2 = Math.toRadians(lon2);
            lat2 = Math.toRadians(lat2);

            double dlon = lon2 - lon1;
            double dlat = lat2 - lat1;
            double a = Math.pow(Math.sin(dlat / 2), 2) + Math.cos(lat1) * Math.cos(lat2) * Math.pow(Math.sin(dlon / 2), 2);
            double c = 2 * Math.asin(Math.sqrt(a));
            double r = 6371;
            return c * r;
        }

        private double round(double value) {
            return Math.round(value * 100.0) / 100.0;
        }
    }

    public static class History implements Serializable {
        public List<Double> amounts = new ArrayList<>();
        public List<Location> locations = new ArrayList<>();
        public List<Long> timestamps = new ArrayList<>();
    }

    public static class Location implements Serializable {
        public double longitude;
        public double latitude;

        public Location() {}
        public Location(double longitude, double latitude) {
            this.longitude = longitude;
            this.latitude = latitude;
        }
    }

    public static class Alert {
        public String type;
        public String card_id;
        public Double amount;
        public Double mean_amount;
        public Double z_score;
        public String description;

        public Alert(String type, String card_id, Double amount, Double mean_amount, Double z_score, String description) {
            this.type = type;
            this.card_id = card_id;
            this.amount = amount;
            this.mean_amount = mean_amount;
            this.z_score = z_score;
            this.description = description;
        }
    }

    public static class AlertSpeed {
        public String type;
        public String card_id;
        public Double speed_kmh;
        public Double distance_km;
        public String description;

        public AlertSpeed(String type, String card_id, Double speed_kmh, Double distance_km, String description) {
            this.type = type;
            this.card_id = card_id;
            this.speed_kmh = speed_kmh;
            this.distance_km = distance_km;
            this.description = description;
        }
    }
}