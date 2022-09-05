package com.aqualen.epamkafka.logic;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class Consumer {

  @SendTo
  @KafkaListener(topics = "${kafka.source-topic-name}", groupId = "${kafka.consumer-group-id}")
  public String consumeMessage(String message) {
    log.info("Received message from Kafka: {}", message);
    return "Success";
  }
}
