package com.bank.dispatch_consumer.infrastructure.mongo;

import com.bank.dispatch_consumer.domain.entity.CardReplacementEntity;
import com.bank.dispatch_consumer.domain.repo.CardReplacementRepository;
import io.reactivex.rxjava3.core.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.adapter.rxjava.RxJava3Adapter;

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
