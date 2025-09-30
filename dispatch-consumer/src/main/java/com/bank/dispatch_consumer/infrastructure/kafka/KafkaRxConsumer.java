package com.bank.dispatch_consumer.infrastructure.kafka;

import com.bank.dispatch_consumer.config.KafkaTopicsProperties;
import com.bank.dispatch_consumer.domain.mapper.EntityMapper; // tu mapper (renombrado a EntityMapper)
import com.bank.dispatch_consumer.domain.model.CardReplacementEvent;
import io.reactivex.rxjava3.core.Flowable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;
import reactor.adapter.rxjava.RxJava3Adapter;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Collections;


/*
Es el adaptador de Kafka que devuelve un stream reactivo de eventos.
stream():
Se suscribe al tópico principal (topics.getMain()).
Usa KafkaReceiver.create(...) (de Reactor Kafka).
Convierte el Flux<ReceiverRecord> en un Flowable<EventMessage<CardReplacementEvent>> de RxJava.
toMessage(...):
Intenta mapear el valor de Kafka (GenericRecord Avro) a CardReplacementEvent usando EntityMapper.
Si no es Avro → log warning.
Devuelve un EventMessage con:
payload = evento parseado.
offset = para ACK.
rawKafkaValue = valor crudo (útil para DLT/log).
Resumen: convierte Kafka Avro → Evento de dominio → EventMessage listo para ProcessingService.
*/

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaRxConsumer {

    private final ReceiverOptions<String, Object> baseOptions;
    private final KafkaTopicsProperties topics;
    private final EntityMapper mapper;

    public Flowable<EventMessage<CardReplacementEvent>> stream() {
        var options = baseOptions.subscription(Collections.singleton(topics.getMain()));
        return RxJava3Adapter.fluxToFlowable(
                KafkaReceiver.create(options)
                        .receive()
                        .map(this::toMessage)
        );
    }

    private EventMessage<CardReplacementEvent> toMessage(ReceiverRecord<String, Object> rec) {
        Object value = rec.value();
        CardReplacementEvent ev = null;
        try {
            if (value instanceof GenericRecord gr) {
                ev = mapper.toEvent(gr); // mapea Avro -> dominio
            } else {
                log.warn("Valor no Avro ({}), se ignora", value == null ? "null" : value.getClass());
            }
        } catch (Exception e) {
            log.error("Error mapeando Avro -> Event", e);
        }
        return new EventMessage<>(ev, rec.receiverOffset(), value);
    }
}
