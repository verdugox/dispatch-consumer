package com.bank.dispatch_consumer.api;

import com.bank.dispatch_consumer.domain.entity.CardReplacementEntity;
import com.bank.dispatch_consumer.domain.repo.CardReplacementRepository;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class QueryController {

    private final CardReplacementRepository repo;

    /** GET /api/events/{requestId} -> 200 con el doc o 404 si no existe */
    @GetMapping("/events/{requestId}")
    public Single<ResponseEntity<CardReplacementEntity>> byPath(@PathVariable String requestId) {
        return repo.findByRequestId(requestId)          // Maybe<CardReplacementEntity>
                .map(ResponseEntity::ok)                // 200 OK
                .switchIfEmpty(Maybe.just(ResponseEntity.notFound().build()))
                .toSingle();
    }

    /** GET /api/events?requestId=... -> igual que arriba (fallback) */
    @GetMapping("/events")
    public Single<ResponseEntity<CardReplacementEntity>> byQuery(@RequestParam String requestId) {
        return repo.findByRequestId(requestId)
                .map(ResponseEntity::ok)
                .switchIfEmpty(Maybe.just(ResponseEntity.notFound().build()))
                .toSingle();
    }

    /** GET /api/health -> simple health */
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("OK");
    }
}
