package com.bank.dispatch_consumer.infrastructure.kafka;

import lombok.AllArgsConstructor;
import lombok.Getter;
import reactor.kafka.receiver.ReceiverOffset;

@Getter
@AllArgsConstructor
public class EventMessage<T> {
    private final T payload;
    private final ReceiverOffset offset;
    private final Object rawKafkaValue; // útil para DLT/log
    public void ack(){ offset.acknowledge(); }
}
