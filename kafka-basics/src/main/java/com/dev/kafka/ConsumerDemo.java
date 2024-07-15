package com.dev.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class ConsumerDemo {

    public static void main(String[] args) throws InterruptedException {

        log.info("I am Consumer");
        // create Producer properties
        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // connect to conduktor playground
        // let create server on conduktor playground

        // set producer properties

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        String groupId = "myApp";
        String topic = "demo_java";

        // config for consumber
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        // create a producer record
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subcribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            log.info("Polling........");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                log.info("Key-Value: " + record.key() + " " + record.value()
                        + " | " + "Partition: " + record.partition() + " | " + "Offset: " + record.offset());
            }
        }
    }
}
