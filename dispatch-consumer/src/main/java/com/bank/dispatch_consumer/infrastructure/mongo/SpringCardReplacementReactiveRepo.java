package com.bank.dispatch_consumer.infrastructure.mongo;

import com.bank.dispatch_consumer.domain.entity.CardReplacementEntity;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Mono;

public interface SpringCardReplacementReactiveRepo
        extends ReactiveMongoRepository<CardReplacementEntity, String> {
    Mono<Boolean> existsByRequestId(String requestId);   // <-- importante
    Mono<CardReplacementEntity> findByRequestId(String requestId);
}
