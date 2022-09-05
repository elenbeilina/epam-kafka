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
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

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
  public ReplyingKafkaTemplate<String, String, String> replyingKafkaTemplate(
      KafkaProperties properties,
      ConcurrentMessageListenerContainer<String, String> replyContainer
  ) {
    Map<String, Object> props = properties.buildProducerProperties();
    //This is set by default:
    //https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#producerconfigs_acks
    props.put(ProducerConfig.ACKS_CONFIG, "all");

    return new ReplyingKafkaTemplate<>(new DefaultKafkaProducerFactory<>(props), replyContainer);
  }

  @Bean
  KafkaTemplate<String, String> replyKafkaTemplate(KafkaProperties properties) {
    return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
            properties.buildProducerProperties())
        );
  }

  @Bean
  public ConcurrentMessageListenerContainer<String, String> replyContainer(
      ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory) {
    ConcurrentMessageListenerContainer<String, String> replyContainer =
        kafkaListenerContainerFactory.createContainer(customKafkaProperties.getReplyTopicName());
    replyContainer.getContainerProperties().setGroupId(customKafkaProperties.getConsumerGroupId());
    replyContainer.getContainerProperties().setMissingTopicsFatal(false);
    return replyContainer;
  }


  //at most once
  @Bean
  public ConsumerFactory<String, String> consumerFactory(KafkaProperties kafkaProperties) {
    Map<String, Object> props = kafkaProperties.buildConsumerProperties();
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
    return new DefaultKafkaConsumerFactory<>(props);
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
      ConsumerFactory<String, String> consumerFactory,
      KafkaTemplate<String, String> replyKafkaTemplate
  ) {

    ConcurrentKafkaListenerContainerFactory<String, String> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.setReplyTemplate(replyKafkaTemplate);

    return factory;
  }
}
