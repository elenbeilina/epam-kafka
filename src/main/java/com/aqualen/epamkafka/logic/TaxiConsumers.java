package com.aqualen.epamkafka.logic;

import com.aqualen.epamkafka.dto.Taxi;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
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

  @SneakyThrows
  @KafkaListener(topics = "${kafka.topic-name}", groupId = "taxi-consumer")
  public void consumer1(ConsumerRecord<Long, Taxi> taxi) {
    writeToTheFile("Consumer#1", taxi.value());
  }

  @SneakyThrows
  @KafkaListener(topics = "${kafka.topic-name}", groupId = "taxi-consumer")
  public void consumer2(ConsumerRecord<Long, Taxi> taxi) {
    writeToTheFile("Consumer#2", taxi.value());
  }

  @SneakyThrows
  @KafkaListener(topics = "${kafka.topic-name}", groupId = "taxi-consumer")
  public void consumer3(ConsumerRecord<Long, Taxi> taxi) {
    writeToTheFile("Consumer#3", taxi.value());
  }

  @SneakyThrows
  private void writeToTheFile(String consumerName, Taxi taxi) {
    Files.writeString(logFile, consumerName + ": " + taxi.toString() + "\n", APPEND);
    log.info("Taxi received by " + consumerName + " -> {}", taxi);
  }
}
