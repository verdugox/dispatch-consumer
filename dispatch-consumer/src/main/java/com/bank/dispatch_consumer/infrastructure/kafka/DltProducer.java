package com.bank.dispatch_consumer.infrastructure.kafka;

import com.bank.dispatch_consumer.config.KafkaTopicsProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

@Slf4j
@Component
@RequiredArgsConstructor
public class DltProducer {
    private final SenderOptions<String, String> senderOptions;
    private final KafkaTopicsProperties topics;

    public Mono<Void> send(String key, String value) {
        return KafkaSender.create(senderOptions)
                .send(Mono.just(SenderRecord.create(new ProducerRecord<>(topics.getDlt(), key, value), null)))
                .then();
    }
}
