package com.aqualen.epamkafka.dto;

import lombok.Builder;

@Builder
public record Taxi(Long id, String coordinates) {
}
