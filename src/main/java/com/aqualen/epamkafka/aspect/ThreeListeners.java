package com.aqualen.epamkafka.aspect;

import org.springframework.core.annotation.AliasFor;
import org.springframework.kafka.annotation.KafkaListener;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@KafkaListener
public @interface ThreeListeners {

    @AliasFor(annotation = KafkaListener.class, attribute = "topics")
    String[] topics();

    @AliasFor(annotation = KafkaListener.class, attribute = "concurrency")
    String concurrency() default "3";

    @AliasFor(annotation = KafkaListener.class, attribute = "groupId")
    String groupId();
}