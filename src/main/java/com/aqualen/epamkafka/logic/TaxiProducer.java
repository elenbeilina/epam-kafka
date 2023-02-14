package com.aqualen.epamkafka.logic;

import com.aqualen.epamkafka.dto.Taxi;
import com.aqualen.epamkafka.properties.KafkaProperties;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.stream.LongStream;

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

  @Profile("test")
  @EventListener(ApplicationStartedEvent.class)
  public void sendTaxis() {
    LongStream.range(0, 10).forEach(
        nbr -> kafkaTemplate.send(kafkaProperties.getTopicName(), nbr, Taxi.builder()
            .id(nbr)
            .coordinates(String.valueOf(nbr))
            .build())
    );
  }
}
