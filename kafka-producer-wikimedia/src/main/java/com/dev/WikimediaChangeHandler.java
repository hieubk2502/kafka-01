package com.dev;


import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements BackgroundEventHandler {

    private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    KafkaProducer<String, String> kafkaProducer;
    String topic;

    WikimediaChangeHandler(KafkaProducer<String, String> producer, String topic){
        this.kafkaProducer = producer;
        this.topic = topic;
    }


    @Override
    public void onOpen() throws Exception {

    }

    @Override
    public void onClosed() throws Exception {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        log.info(messageEvent.getData());
        // asynchonous
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String s) throws Exception {

        // nothing hear
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Error in stream reading", throwable);
    }
}
