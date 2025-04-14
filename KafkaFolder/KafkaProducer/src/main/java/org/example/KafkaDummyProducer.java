package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class KafkaDummyProducer {
    public static void main(String[] args) throws InterruptedException {

        String topic = "my-dummy-topic";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");  //todo: change to config file
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Random random = new Random();
        String[] stockNames = {"AAPL", "GOOGL", "AMZN", "MSFT", "TSLA"};

        while (true) {
            if(!isTopicAvailable(topic, props)){
                System.out.println("Topic is not available: " + topic);
                Thread.sleep(10000); // every 2 seconds
                continue;
            } else {
                System.out.println("Topic is available: " + topic);

            }
            String stockName = stockNames[random.nextInt(stockNames.length)];
            double currentPrice = 100 + (500 - 100) * random.nextDouble(); // Random price between 100 and 500
            long timestamp = System.currentTimeMillis();

            // Manually construct the JSON string
            String value = String.format(
                    "{\"stockName\":\"%s\",\"currentPrice\":%.2f,\"timestamp\":%d,\"metadata\":\"Sample metadata\"}",
                    stockName, currentPrice, timestamp
            );

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, stockName, value);

            producer.send(record, (metadataRecord, exception) -> {
                if (exception == null) {
                    System.out.printf("Sent: %s to partition=%d offset=%d%n",
                            value, metadataRecord.partition(), metadataRecord.offset());
                } else {
                    exception.printStackTrace();
                }
            });

            Thread.sleep(2000); // every 2 seconds
        }

        // producer.close(); // unreachable due to infinite loop, add shutdown hook if needed
    }

    private static boolean isTopicAvailable(String topic, Properties props) {
        try (AdminClient adminClient = AdminClient.create(props)) {
            ListTopicsResult topics = adminClient.listTopics();
            return topics.names().get().contains(topic);
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Error checking topic availability: " + e.getMessage());
            return false;
        }
    }
}