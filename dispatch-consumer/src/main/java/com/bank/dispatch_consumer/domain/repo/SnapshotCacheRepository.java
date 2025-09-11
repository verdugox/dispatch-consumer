package com.bank.dispatch_consumer.domain.repo;

import io.reactivex.rxjava3.core.Maybe;

public interface SnapshotCacheRepository {
    Maybe<String> getSnapshotJson(String requestId);
}
