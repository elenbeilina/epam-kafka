package com.aqualen.epamkafka.config;

import com.aqualen.epamkafka.properties.CustomKafkaProperties;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;

@EnableKafka
@Configuration
@RequiredArgsConstructor
public class KafkaConfig {

  private final CustomKafkaProperties customKafkaProperties;

  @Bean
  public NewTopic taxiTopic() {
    return TopicBuilder.name(customKafkaProperties.getSourceTopicName())
        .partitions(3)
        .replicas(3)
        .build();
  }

  @Bean
  //at least once
  public KafkaTemplate<String, String> kafkaTemplate(
      KafkaProperties properties) {
    Map<String, Object> props = properties.buildProducerProperties();
    //This is set by default:
    //https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#producerconfigs_acks
    props.put(ProducerConfig.ACKS_CONFIG, "all");

    return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
  }

  //at most once
  //https://medium.com/@andy.bryant/processing-guarantees-in-kafka-12dd2e30be0e#:~:text=Kafka%20Consumer%20API,-Processes%20pull%20data&text=The%20partitions%20of%20any%20topics,are%20stored%20in%20the%20log.
  @Bean
  public ConsumerFactory<String, String> consumerFactory(KafkaProperties kafkaProperties) {
    Map<String, Object> props = kafkaProperties.buildConsumerProperties();
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
    return new DefaultKafkaConsumerFactory<>(props);
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
      ConsumerFactory<String, String> consumerFactory) {

    ConcurrentKafkaListenerContainerFactory<String, String> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);

    return factory;
  }
}
