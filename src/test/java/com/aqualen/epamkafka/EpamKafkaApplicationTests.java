package com.aqualen.epamkafka;

import com.aqualen.epamkafka.logic.Consumer;
import com.aqualen.epamkafka.logic.ExternalService;
import com.aqualen.epamkafka.logic.Producer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(count = 3, partitions = 1)
class EpamKafkaApplicationTests {
  @SpyBean
  private Consumer consumer;
  @MockBean
  private ExternalService externalService;
  @Autowired
  private Producer producer;

  @Test
  void testApplicationSuccess() {
    producer.sendMessage("1", "test");
    verify(consumer, timeout(1000).times(2)).consumeMessage(anyString());
  }

  @Test
  void testApplicationFail() {
    when(externalService.sendToExternalSystem(anyString())).thenThrow(new RuntimeException("System is offline"));
    assertThrows(RuntimeException.class, () -> producer.sendMessage("1", "test"));
    verify(consumer, timeout(1000).times(0)).consumeMessage(anyString());
  }

}
