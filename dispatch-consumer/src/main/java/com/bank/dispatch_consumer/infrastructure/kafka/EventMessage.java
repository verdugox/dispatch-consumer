package com.bank.dispatch_consumer.infrastructure.kafka;

import lombok.AllArgsConstructor;
import lombok.Getter;
import reactor.kafka.receiver.ReceiverOffset;

//Es un wrapper que encapsula un mensaje consumido desde Kafka.
//Campos:
//payload → el objeto de dominio (ej: CardReplacementEvent) ya parseado.
//offset → referencia al offset de Kafka para poder ACKear.
//rawKafkaValue → el valor crudo del Kafka record (ej. GenericRecord de Avro), útil para logging o DLT.
//Método:
//ack() → confirma el procesamiento del mensaje en Kafka (offset.acknowledge()).
//Resumen: convierte un ReceiverRecord de Kafka en un objeto más amigable para tu dominio.

@Getter
@AllArgsConstructor
public class EventMessage<T> {
    private final T payload;
    private final ReceiverOffset offset;
    private final Object rawKafkaValue; // útil para DLT/log
    public void ack(){ offset.acknowledge(); }
}
