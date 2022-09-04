package com.aqualen.epamkafka.config;

import com.aqualen.epamkafka.properties.KafkaProperties;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;

@EnableKafka
@Configuration
@RequiredArgsConstructor
public class KafkaConfig {

  private final KafkaProperties kafkaProperties;

  @Bean
  public NewTopic taxiTopic() {
    return TopicBuilder.name(kafkaProperties.getTopicName())
        .partitions(3)
        .replicas(3)
        .build();
  }
}
