package com.aqualen.epamkafka;

import com.aqualen.epamkafka.logic.Consumer;
import com.aqualen.epamkafka.logic.Producer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
class EpamKafkaApplicationTests {

  @Captor
  ArgumentCaptor<String> messageArgumentCaptor;
  @SpyBean
  private Consumer consumer;
  @Autowired
  private Producer producer;

  @Test
  void testApplication() {
    producer.sendMessage("test");
    verify(consumer, timeout(10000).times(1)).consumeMessage(messageArgumentCaptor.capture());
    assertThat(messageArgumentCaptor.getValue()).isEqualTo("test");
  }

}
