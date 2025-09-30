package com.bank.dispatch_consumer.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

//Crea una clase de configuración para mapear topics de Kafka desde el application.yml.

@Getter @Setter
@Configuration
@ConfigurationProperties(prefix = "app.topics")
public class KafkaTopicsProperties {
    private String main;
    private String dlt;
}
