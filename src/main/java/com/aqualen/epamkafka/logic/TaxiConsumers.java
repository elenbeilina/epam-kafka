package com.aqualen.epamkafka.logic;

import com.aqualen.epamkafka.aspect.ThreeListeners;
import com.aqualen.epamkafka.dto.Taxi;
import jakarta.annotation.PostConstruct;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.StandardOpenOption.APPEND;

@Slf4j
@Component
public class TaxiConsumers {

  private static final Path logFile = Paths.get("taxi-log.txt");

  @SneakyThrows
  @PostConstruct
  private void clearFileContent() {
    Files.writeString(logFile, "");
  }

  @ThreeListeners(topics = "${kafka.topic-name}",
      groupId = "taxi-consumer")
  public void consumer(ConsumerRecord<Long, Taxi> taxi, @Header(KafkaHeaders.CONSUMER) Consumer<Long, Taxi> consumer) {
    writeToTheFile("Consumer: " + consumer.groupMetadata().memberId(), taxi.value());
  }

  @SneakyThrows
  private void writeToTheFile(String consumerName, Taxi taxi) {
    Files.writeString(logFile, consumerName + ": " + taxi.toString() + "\n", APPEND);
    log.info("Taxi received by " + consumerName + " -> {}", taxi);
  }
}
