package com.aqualen.epamkafka.logic;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ExternalService {
  public String sendToExternalSystem(String message) {
    log.info("Sending to external system message: {}", message);
    return "Success";
  }
}
