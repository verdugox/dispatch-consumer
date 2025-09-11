package com.bank.dispatch_consumer.infrastructure.redis;

import com.bank.dispatch_consumer.domain.repo.SnapshotCacheRepository;
import io.reactivex.rxjava3.core.Maybe;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.adapter.rxjava.RxJava3Adapter;

@Component
@RequiredArgsConstructor
public class RedisSnapshotCacheRepository implements SnapshotCacheRepository {

    private final ReactiveStringRedisTemplate redis;

    @Override
    public Maybe<String> getSnapshotJson(String requestId) {
        return RxJava3Adapter.monoToMaybe(redis.opsForValue().get(key(requestId)));
    }

    private String key(String id){ return "card:event:" + id; }
}
