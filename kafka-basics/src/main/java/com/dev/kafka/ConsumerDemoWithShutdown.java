package com.dev.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class ConsumerDemoWithShutdown {

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

        // Get reference to main thread
        final Thread threadCurrent = Thread.currentThread();

        // Adding the shutdown hook
        // mechanism when app is shutdown
        // The line registers a shutdown hook will Java runtime. It is executed just before JVM terminates.
        //
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");

                // THIS method wakes upp the consumer from its poll()
                // allows consumer to gracefull exit the polling loop and stop consumer messages.
                consumer.wakeup();

                // join main thread to allow the execution
                try {

                    // tries to wait for the threadCurrent to finnish running.
                    threadCurrent.join();

                } catch (InterruptedException e) {
                    // error migh occur if main thread if interrupted the stack strace of exception
                    throw new RuntimeException(e);
                }

            }
        });

        try {
            // subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            while (true) {
                log.info("Polling........");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key-Value: " + record.key() + " " + record.value()
                            + " | " + "Partition: " + record.partition() + " | " + "Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shutdown");
        } catch (Exception e) {
            log.error("Unexpected exception in consumer", e);
        } finally {
            consumer.close(); // close consumer, will also commit offsets
            log.info("THe consumer is now gracefully shut down");
        }


    }
}

//If have new instance (Producer or COnsumer) Kafka Coordination will re-balance partition for every instance
// Consumer belong to the same consumer group will share partitions of a topic together
//Update metadata offset for consumer group
// Re-balance might cause delays

// And Zookeeper will save information metadata: broker list, config of topic, offset current of each consumer group,
// information about partition leaders: broker
// Synchronization: zookeeper ensures that all brokers have up-to-date information about new config.

// JAVA Consumer incremental Cooperative Rebalance

// TYPE rebalance
// 1. Eager Rebalance
// 1.1 All consumer stop, give up their membership of partitions
// 1.2 They rejoin consumer gorup and get a new partition assignment
// 1.3 During a short period of time, the entire consumer gorup stop process
// 1.4 COnsumer don't neccessarily get back the same partitions as they use to

// 2. Cooperative Rebalance  (Incremental Rebalance)
// 2.1 Reassigning a small subset of the partitions from one consumer to another
// 2.2 Other consumers that don't reassigned partitions , can still process uninterrupted
// 2.3 Can go through serveral iterations to find "stable" assignment ( hence " incremental")
// 2.4 Avouds " stop-the-world" events where all consumers stop processing data


// RangeAssignor: assign partitions on a per-topic basic ( can lead to imbalance)
// RoundRObin: assign partitions across all topic in round-robin fashion, optimal balance
//StickyAssignor: balanced like RoundRobin, and then minimises partition movements
// when consumber join/leave group in order minize movement ( base on history and number of partition current)
// CooperativeStickyAssignor: rebalance strategy is identical StickyAssignor but supports cooperative rebalances
// and therefore consummer can keep on consuming from topic (base on history, consent of consumore==r-e


// Kafka Consumer - Auto Offset Commit Behavior
// In the Java Consumer API, offsets are regularly committed
// Enable are committed when i call .poll() and auto.commit.interval.ms have elapsed
// Example: auto.commit.interval.ms = 500 and enable.auto.commit = true will commit
// make sure messages are all successfully processed before you call poll() again.
// If you don't, you will be in at least one reading scenario

// i  that ( rare) case, you must disable enable.auto.commit and most likeky most .commitAsync()
// with commit offset manual ( multiple thread)





















