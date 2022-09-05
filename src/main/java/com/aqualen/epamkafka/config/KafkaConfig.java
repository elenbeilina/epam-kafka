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
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.Map;

@EnableKafka
@Configuration
@RequiredArgsConstructor
@EnableTransactionManagement
public class KafkaConfig {

  private final CustomKafkaProperties customKafkaProperties;

  @Bean
  public NewTopic topic() {
    return TopicBuilder.name(customKafkaProperties.getSourceTopicName())
        .partitions(3)
        .replicas(3)
        .build();
  }

  @Bean
  KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
    return new KafkaTemplate<>(producerFactory);
  }

  @Bean
  KafkaTransactionManager<String, String> kafkaTransactionManager(ProducerFactory<String, String> producerFactory) {
    return new KafkaTransactionManager<>(producerFactory);
  }

  @Bean
  ProducerFactory<String, String> producerFactory(KafkaProperties properties) {
    Map<String, Object> props = properties.buildProducerProperties();
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction-id");
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    return new DefaultKafkaProducerFactory<>(props);
  }

  @Bean
  ConsumerFactory<String, String> consumerFactory(KafkaProperties kafkaProperties) {
    Map<String, Object> props = kafkaProperties.buildConsumerProperties();
    props.put(ConsumerConfig.GROUP_ID_CONFIG, customKafkaProperties.getConsumerGroupId());
    return new DefaultKafkaConsumerFactory<>(props);
  }

  @Bean
  ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
      ConsumerFactory<String, String> consumerFactory) {

    ConcurrentKafkaListenerContainerFactory<String, String> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);

    return factory;
  }
}
