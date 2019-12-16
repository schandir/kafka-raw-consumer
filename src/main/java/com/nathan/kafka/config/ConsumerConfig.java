package com.nathan.kafka.config;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.nathan.kafka.model.SampleMessage;
import com.nathan.kafka.serializer.JsonDeserializer;

import java.util.Collections;
import java.util.Properties;

@Configuration
public class ConsumerConfig {

    @Bean
    public KafkaConsumer<String, SampleMessage> sampleMessageConsumer(KafkaConsumerProperties kafkaConsumerProperties) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaConsumerProperties.getBootstrap());
        props.put("group.id", kafkaConsumerProperties.getGroup());
        props.put("auto.offset.reset", "earliest");
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, SampleMessage> consumer = new KafkaConsumer<>(props, stringKeyDeserializer(), sampleMessageJsonValueDeserializer());
        consumer.subscribe(Collections.singletonList(kafkaConsumerProperties.getTopic()));
        return consumer;
    }

    @Bean
    public Deserializer stringKeyDeserializer() {
        return new StringDeserializer();
    }

    @Bean
    public Deserializer sampleMessageJsonValueDeserializer() {
        return new JsonDeserializer(SampleMessage.class);
    }
}
