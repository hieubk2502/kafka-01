package com.dev;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // connect to conduktor playground
        // let create server on conduktor playground

        // set producer properties
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // set hight throughput producer configs
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");



        // CREATE producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String topic = "wikimedia.recentchange";
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        BackgroundEventHandler backgroundEventHandler = new WikimediaChangeHandler(producer, topic);

        // start producer in another thread

        BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(backgroundEventHandler, new EventSource.Builder(URI.create(url)));
        BackgroundEventSource eventSource = builder.build();

        // start producer in another thread
        eventSource.start();

        // we produce for 10 minutes and block the program until them
        TimeUnit.SECONDS.sleep(10);

    }
}
// acks = 1
// producer consider messages as " written success" when the message was acknowledged by only the leader
// leader response is requested, but replication is not a guarantee as it happens in the background
// if leader broker goes offline unexpectedly but replicas have't replicated the data yet, we have data loss
// if an ack not received the producer may retry the request

// acks = all ( acks = -1)
// producer consider messages as "written successfully" when the message is accepted by all ISR in sync replicas

// min.insync.replicas && acks = all
// min.insync.replicas
// if it = 1: only broker leader needs to successfully ack
// if it = 2: only broker leader and 1 replica broker needs to successfully ack
// ack = all and min.insync.replicas = 2 is most popular option

// Producer Retries
// in case of trasient failures, dev are expected handle exception, otherwise the data will be lost
// Xample ( NOT_ENOUGH_REPLICAS( due to  in.insync.replicas setting)

// set retry.,backoff.ms (default 100ms)
// kafka > 2.1 set delivery.timout.ms = 120000 = 2 min
// delivery.timeout.ms >= linger.ms + retry.backoff.ms + request..timout.ms
// SEnd -(linger)batching -> await send --retries (retry.backoff.ms)--> Inflight ( request.timeout.ms)

//  if you using an idempotent producer
//  case of retries, there is a chance that messages will be send out of order ( if a batch has failed to be sent)
// if you rely on key based ordering, that can be an issue.
// for this, you can set the setting while controls how many producer request can be made in parallel
// max.in.flight.requests.per.connection

// better solution with idempotent producers;

// use idemptent prodcer for remove duplicate
// idempotent producer are great to guarantee a stable and safe pipeline
// enable.idempotence

// safe kafka producer
// acks = -1
// min.insync.replicas = 2 (broker/topic level/)
// enable.idempotence = true
// retries = MAX_INT
// delivery.timeout.ms = 120000
// max.in.flight.request. = 5

// Message compression
// advantage: fastter to transfer data. over the network, better throughput, better disk utilisation (store messages on disk)
// disadvantages: Producers must commit some CPU cycles to compression
// consumers must commit some cpu cycles to decompression

// compression.type =producer the broker take the compresss batch from the producer client
// and write it directly to the topic log file without recompressing the data
// compression.type=none all batches are decompressed by the broker
// commpression.type= lz4
// if it matching the producer setting, data is stored on dis as is
// if it a different compression setting, batches are decompressed by the broker and then re-compressed using the compression algorithm specified

// By default, kafka producers try to send records as soon as possible
// it will have up connect=5 meaning up to 5 message batches being in flight. ( being sent between the producer in the broker) at most
// after this, if more messages must be sent while others are in flight, kafka is smart and will start batching them before the next batch send
// heap increase throughtput while maintaining very low latency
// added benefit: batchees have higher compression retio so better efficiency

// two setting to influence the batching mechanism
// linger.ms : 0   long to wait until send a batch.  ( head add more messages in the batch at trhe expense of latency)
// batch.size: if a batch is filled before linger.ms, increase the batch size

//Producerbatch wait up to linger.ms or( 1 batch/1 request max size = batch.size) and send topic kafka

// high throughput producer
// we add snappy message compression in our producer
// snappy is very helpfull if your messages are text based, for example log lines or Json documents
// snappy has a good balance of CPU/ compression ratio
// increase batch.size to 32KB and introduce a small delay through linger.ms 20 ms



