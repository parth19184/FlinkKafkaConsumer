package org.example;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class FlinkKafkaConsumerApp {
    public static void main(String[] args) throws Exception {
        // 1. Set up the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. Configure Kafka consumer properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "flink-consumer-group");

        // 3. Create the Kafka consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "my-dummy-topic", // topic name
                new SimpleStringSchema(), // deserialization schema
                props
        );

        // 4. Add the source to the environment
        env.addSource(consumer)
                .name("Kafka Source")
                .print();

        // 5. Execute the job
        env.execute("Flink Kafka Consumer Job");
    }
}