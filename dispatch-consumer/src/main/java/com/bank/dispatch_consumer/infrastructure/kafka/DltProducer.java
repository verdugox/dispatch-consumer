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
/*
* Crea un KafkaSender con la configuración (senderOptions).
*  Es el productor reactivo de Kafka (de reactor-kafka).
* Crea un ProducerRecord:
* new ProducerRecord<>(topics.getDlt(), key, value)
* topics.getDlt() → obtiene el nombre del tópico DLT desde application.yml.
* key → clave del mensaje Kafka (usada para particionar).
* value → el payload del mensaje (normalmente un JSON con el evento fallido).
 * Lo envuelve en un SenderRecord (es como el sobre que reactor-kafka necesita).
 * Lo envía con KafkaSender.send(...):
* Se pasa como Mono.just(...) porque solo mandamos un mensaje en este caso.
* El null en el SenderRecord es el "correlation metadata" (no usado aquí).
* .then():
* Convierte la respuesta en Mono<Void>.
* O sea: no te interesa la metadata del envío, solo quieres saber que se completó o falló.
* */
public class DltProducer {
    private final SenderOptions<String, String> senderOptions;
    private final KafkaTopicsProperties topics;

    public Mono<Void> send(String key, String value) {
        return KafkaSender.create(senderOptions)
                .send(Mono.just(SenderRecord.create(new ProducerRecord<>(topics.getDlt(), key, value), null)))
                .then();
    }
}
