package com.aqualen.epamkafka.logic;

import com.aqualen.epamkafka.properties.CustomKafkaProperties;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class Producer {

  private final KafkaTemplate<String, String> kafkaTemplate;
  private final CustomKafkaProperties customKafkaProperties;
  private final ExternalService externalService;

  @SneakyThrows
  @Transactional
  public void sendMessage(String key, String message) {
    log.info("Sending message: {} to Kafka.", message);
    ProducerRecord<String, String> producerRecord =
        new ProducerRecord<>(customKafkaProperties.getSourceTopicName(), key, message);
    kafkaTemplate.send(producerRecord);
    externalService.sendToExternalSystem(message);
    log.info("Sending message: {} to Kafka.", message);
    kafkaTemplate.send(producerRecord);
  }
}
