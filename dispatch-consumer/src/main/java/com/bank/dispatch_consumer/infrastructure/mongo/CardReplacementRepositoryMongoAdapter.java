package com.bank.dispatch_consumer.infrastructure.mongo;

import com.bank.dispatch_consumer.domain.entity.CardReplacementEntity;
import com.bank.dispatch_consumer.domain.repo.CardReplacementRepository;
import io.reactivex.rxjava3.core.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.adapter.rxjava.RxJava3Adapter;

/*
Es el adaptador de infraestructura MongoDB.
Implementa la interfaz de dominio CardReplacementRepository.
Usa un repo reactivo Spring (SpringCardReplacementReactiveRepo) pero lo adapta a RxJava3 con RxJava3Adapter.
Métodos:
findByRequestId → busca entidad en Mongo.
existsByRequestId → verifica existencia por ID.
save → inserta o actualiza entidad.
Resumen: traduce entre tu dominio (RxJava3) y la base MongoDB (Reactor/Mono).
*/

@Component
@RequiredArgsConstructor
public class CardReplacementRepositoryMongoAdapter implements CardReplacementRepository {

    private final SpringCardReplacementReactiveRepo reactiveRepo;

    @Override
    public Maybe<CardReplacementEntity> findByRequestId(String requestId) {
        return RxJava3Adapter.monoToMaybe(reactiveRepo.findByRequestId(requestId));
    }

    @Override
    public Single<Boolean> existsByRequestId(String requestId) {
        return RxJava3Adapter.monoToSingle(reactiveRepo.existsByRequestId(requestId));
    }

    @Override
    public Single<CardReplacementEntity> save(CardReplacementEntity entity) {
        return RxJava3Adapter.monoToSingle(reactiveRepo.save(entity));
    }
}
