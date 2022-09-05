package com.aqualen.epamkafka.logic;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class Consumer {

  private final ExternalService externalService;

  @KafkaListener(topics = "${kafka.source-topic-name}", groupId = "${kafka.consumer-group-id}")
  public void consumeMessage(String message) {
    log.info("Received message from Kafka: {}", message);
    externalService.sendToExternalSystem(message);
  }
}
