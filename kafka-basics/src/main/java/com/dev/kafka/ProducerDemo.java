package com.dev.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Objects;
import java.util.Properties;

@Slf4j
public class ProducerDemo {

    public static void main(String[] args) throws InterruptedException {
        // create Producer properties
        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // connect to conduktor playground
        // let create server on conduktor playground

        // set producer properties

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // set size for batch (kb)
        properties.setProperty("batch.size", "500");

        // create the producer

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a producer recordw

        for (int j = 0; j < 5; j++) {
            for (int i = 0; i < 20; i++) {
                ProducerRecord<String, String> recordProducer = new ProducerRecord<>("demo_java", "value" + i);

                // send data

                // Round robin producer will seperate devde equally in partitions, rebalance
                // distribute messages to partitions sequetially
                // not guaranteed order message
                // use for app need distribute load evenly between partitions.

                // StickyPartitions producer will remember partition, will keep message in producer only send same partition
                // , (if this partition available)
                // Good for cases that need to process data in order, or want to reduce to load on broker
                // Can lead to load imbalance if producers have different message sending rate
                producer.send(recordProducer, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (Objects.isNull(e)) {
                            log.info("Received new metadata\n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp());
                        } else {
                            log.error("Not received! " + e);
                        }
                    }
                });

                Thread.sleep(2000);
            }
            Thread.sleep(1000);
        }

        // tell the producer to send all data and block until done --synchronous
        producer.flush();

        // flush and close the producer
        producer.close();


    }
}
