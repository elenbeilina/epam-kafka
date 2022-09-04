package com.aqualen.epamkafka.controllers;

import com.aqualen.epamkafka.dto.Taxi;
import com.aqualen.epamkafka.logic.TaxiProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("taxi")
@RequiredArgsConstructor
public class TaxiController {

  private final TaxiProducer taxiProducer;

  @PostMapping
  ResponseEntity<Void> sendTaxi(@RequestBody Taxi taxi) {
    taxiProducer.sendTaxi(taxi);
    return ResponseEntity.ok().build();
  }
}
