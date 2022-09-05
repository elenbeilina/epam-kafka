package com.aqualen.epamkafka.logic;

import com.aqualen.epamkafka.properties.CustomKafkaProperties;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Slf4j
@Component
@RequiredArgsConstructor
public class Producer {

  private final ReplyingKafkaTemplate<String, String, String> replyingKafkaTemplate;
  private final CustomKafkaProperties customKafkaProperties;

  @SneakyThrows
  public void sendMessage(String message) {
    log.info("Sending message: {} to Kafka.", message);
    ProducerRecord<String, String> producerRecord =
        new ProducerRecord<>(customKafkaProperties.getSourceTopicName(), message);
    ConsumerRecord<String, String> consumerRecord =
        replyingKafkaTemplate.sendAndReceive(producerRecord, Duration.ofMillis(10000)).get();
    log.info("Message was processed by consumer with status: {}", consumerRecord.value());
  }
}
