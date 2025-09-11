package com.bank.dispatch_consumer.api;

import com.bank.dispatch_consumer.domain.entity.CardReplacementEntity;
import com.bank.dispatch_consumer.domain.repo.CardReplacementRepository;
import com.bank.dispatch_consumer.domain.repo.SnapshotCacheRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class QueryController {

    private final SnapshotCacheRepository cache;
    private final CardReplacementRepository mongo;

    @GetMapping("/health")
    public ResponseEntity<?> health() { return ResponseEntity.ok().build(); }

    @GetMapping("/events/{requestId}")
    public ResponseEntity<?> byRequestId(@PathVariable String requestId) {
        String snap = cache.getSnapshotJson(requestId).blockingGet(null);
        if (snap != null) return ResponseEntity.ok(snap); // JSON snapshot
        CardReplacementEntity e = mongo.findByRequestId(requestId).blockingGet(null);
        return e == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(e);
    }
}
