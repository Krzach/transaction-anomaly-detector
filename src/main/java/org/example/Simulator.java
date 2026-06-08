package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class Simulator {

    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String TRANSACTIONS_TOPIC = "transactions-topic";
    private static final int NUM_CARDS = 10000;
    private static final int NUM_USERS = 5000;

    private static final Faker fake = new Faker();
    private static final Random random = new Random();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = createProducer();
        if (producer == null) {
            return;
        }

        List<Map<String, Object>> cards = generateCardData();

        while (true) {
            Map<String, Object> card = cards.get(random.nextInt(cards.size()));
            Map<String, Object> transaction = generateTransaction(card);

            if (random.nextDouble() < 0.05) { // 5% chance of anomaly
                transaction = injectAnomaly(transaction);
                System.out.println("--- Anomaly Injected: " + transaction + " ---");
            }

            try {
                String transactionJson = objectMapper.writeValueAsString(transaction);
                producer.send(new ProducerRecord<>(TRANSACTIONS_TOPIC, transactionJson));
                System.out.println("Sent transaction: " + transactionJson);
            } catch (Exception e) {
                System.err.println("Error sending transaction: " + e.getMessage());
            }

            if (random.nextDouble() < 0.01) {
                System.out.println("--- Frequency Anomaly ---");
                for (int i = 0; i < random.nextInt(4) + 2; i++) {
                    try {
                        Thread.sleep(random.nextInt(401) + 100);
                        Map<String, Object> frequentTransaction = generateTransaction(card);
                        String transactionJson = objectMapper.writeValueAsString(frequentTransaction);
                        producer.send(new ProducerRecord<>(TRANSACTIONS_TOPIC, transactionJson));
                        System.out.println("Sent transaction: " + transactionJson);
                    } catch (Exception e) {
                        System.err.println("Error sending frequent transaction: " + e.getMessage());
                    }
                }
            }

            try {
                Thread.sleep(random.nextInt(1501) + 500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static KafkaProducer<String, String> createProducer() {
        try {
            Properties props = new Properties();
            props.put("bootstrap.servers", KAFKA_BROKER);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            return new KafkaProducer<>(props);
        } catch (Exception e) {
            System.err.println("Error creating Kafka producer: " + e.getMessage());
            return null;
        }
    }

    private static List<Map<String, Object>> generateCardData() {
        List<Map<String, Object>> cards = new ArrayList<>();
        int[] limits = {1000, 2000, 5000, 10000};
        for (int i = 0; i < NUM_CARDS; i++) {
            Map<String, Object> card = new HashMap<>();
            card.put("card_id", i);
            card.put("user_id", random.nextInt(NUM_USERS));
            card.put("limit", limits[random.nextInt(limits.length)]);
            cards.add(card);
        }
        return cards;
    }

    private static Map<String, Object> generateTransaction(Map<String, Object> card) {
        Map<String, Object> transaction = new HashMap<>();
        transaction.put("card_id", card.get("card_id"));
        transaction.put("user_id", card.get("user_id"));

        Map<String, Double> location = new HashMap<>();
        location.put("latitude", Double.parseDouble(fake.address().latitude()));
        location.put("longitude", Double.parseDouble(fake.address().longitude()));
        transaction.put("location", location);

        transaction.put("amount", Math.round(random.nextDouble() * ((int) card.get("limit") / 10.0) * 100.0) / 100.0);
        transaction.put("available_limit", card.get("limit"));
        return transaction;
    }

    private static Map<String, Object> injectAnomaly(Map<String, Object> transaction) {
        String[] anomalyTypes = {"amount", "location", "frequency"};
        String anomalyType = anomalyTypes[random.nextInt(anomalyTypes.length)];

        if (anomalyType.equals("amount")) {
            transaction.put("amount", (double) transaction.get("amount") * (random.nextInt(11) + 10));
        } else if (anomalyType.equals("location")) {
            Map<String, Double> location = new HashMap<>();
            location.put("latitude", Double.parseDouble(fake.address().latitude()));
            location.put("longitude", Double.parseDouble(fake.address().longitude()));
            transaction.put("location", location);
        }
        return transaction;
    }
}
