package com.nathan.kafka.service;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import com.nathan.kafka.model.SampleMessage;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class KafkaListener implements SmartLifecycle {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaListener.class);


    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    private final KafkaConsumer<String, SampleMessage> kafkaSampleMessageConsumer;

    private volatile boolean running = false;

    public KafkaListener(KafkaConsumer<String, SampleMessage> kafkaSampleMessageConsumer) {
        this.kafkaSampleMessageConsumer = kafkaSampleMessageConsumer;
    }

    @Override
    public void start() {
        SampleMessageService sampleMessageService = new SampleMessageService(this.kafkaSampleMessageConsumer);
        executorService.submit(sampleMessageService);
        this.running = true;
    }

    @Override
    public void stop() {
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable callback) {

    }

    @Override
    public int getPhase() {
        return 0;
    }
}
