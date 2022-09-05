package com.aqualen.epamkafka.logic;

import com.aqualen.epamkafka.properties.CustomKafkaProperties;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class Producer {

  private final KafkaTemplate<String, String> kafkaTemplate;
  private final CustomKafkaProperties customKafkaProperties;

  @SneakyThrows
  public void sendMessage(String message) {
    log.info("Sending message: {} to Kafka.", message);
    ProducerRecord<String, String> producerRecord =
        new ProducerRecord<>(customKafkaProperties.getSourceTopicName(), message);
    kafkaTemplate.send(producerRecord);
  }
}
