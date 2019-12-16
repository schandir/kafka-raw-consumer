package com.nathan.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.nathan.kafka.model.SampleMessage;

public class SampleMessageService implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(SampleMessageService.class);

    private final KafkaConsumer<String, SampleMessage> sampleMessageConsumer;

    public SampleMessageService(KafkaConsumer<String, SampleMessage> sampleMessageConsumer) {
        this.sampleMessageConsumer = sampleMessageConsumer;
    }

    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, SampleMessage> records = sampleMessageConsumer.poll(100);
                for (ConsumerRecord<String, SampleMessage> record : records) {
                    log.info("Recieved message");
                    log.info("consuming from topic = {}, partition = {}, ts = {}, ts-type = {},  offset = {}, key = {}, value = {}",
                            record.topic(), record.partition(), record.timestamp(), record.timestampType(), record.offset(), record.key(), record.value());

                }
            }
        } finally {
            this.sampleMessageConsumer.close();
        }
    }
}
