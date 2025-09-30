package com.bank.dispatch_consumer.infrastructure.mongo;

import com.bank.dispatch_consumer.domain.entity.CardReplacementEntity;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Mono;

/*
Es un repositorio reactivo de Spring Data MongoDB.
Extiende ReactiveMongoRepository para CardReplacementEntity.
Métodos custom:
existsByRequestId → consulta si existe un documento con ese requestId.
findByRequestId → devuelve la entidad completa si existe.
Resumen: este es el driver reactivo nativo de Mongo, y el adapter lo convierte para que el dominio pueda trabajar en RxJava.
*/

public interface SpringCardReplacementReactiveRepo extends ReactiveMongoRepository<CardReplacementEntity, String>
{
    Mono<Boolean> existsByRequestId(String requestId);   // <-- importante
    Mono<CardReplacementEntity> findByRequestId(String requestId);
}
