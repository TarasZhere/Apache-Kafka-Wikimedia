package com.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class ChangeHandler implements EventHandler {

    private KafkaProducer<String, String> producer;
    private String topic;
    private final Logger log = LoggerFactory.getLogger(ChangeHandler.class.getSimpleName());

    public ChangeHandler(KafkaProducer<String, String> producer, String topic){
        this.producer = producer;
        this.topic = topic;

    }

    @Override
    public void onOpen() throws Exception{
//      nothing in here
    }

    @Override
    public void onClosed(){
        producer.flush();
        producer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception{
        log.info(messageEvent.getData());
//        async code
        producer.send(new ProducerRecord<>(this.topic, messageEvent.getData()));
    }
    @Override
    public void onComment(String comment){
//        No code here
    }

    @Override
    public void onError(Throwable throwable){
        log.info("Error:", throwable);
    }

}
