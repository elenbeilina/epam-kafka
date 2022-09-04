package com.aqualen.epamkafka.logic;

import com.aqualen.epamkafka.dto.Taxi;
import com.aqualen.epamkafka.properties.KafkaProperties;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TaxiProducer {

  private final KafkaTemplate<Long, Taxi> kafkaTemplate;
  private final KafkaProperties kafkaProperties;

  @SneakyThrows
  public void sendTaxi(Taxi taxi) {
    log.info("Sending taxi with id: {} to Kafka.", taxi.id());
    kafkaTemplate.send(kafkaProperties.getTopicName(), taxi.id(), taxi);
  }
}
