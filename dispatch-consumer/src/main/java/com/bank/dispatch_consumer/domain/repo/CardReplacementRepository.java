package com.bank.dispatch_consumer.domain.repo;

import com.bank.dispatch_consumer.domain.entity.CardReplacementEntity;
import io.reactivex.rxjava3.core.*;

public interface CardReplacementRepository {
    Maybe<CardReplacementEntity> findByRequestId(String requestId);
    Single<Boolean> existsByRequestId(String requestId);   // <-- agrega esto
    Single<CardReplacementEntity> save(CardReplacementEntity entity);
}
