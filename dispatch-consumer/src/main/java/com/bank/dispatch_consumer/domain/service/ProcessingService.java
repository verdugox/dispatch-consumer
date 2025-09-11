package com.bank.dispatch_consumer.domain.service;

import com.bank.dispatch_consumer.domain.entity.CardReplacementEntity;
import com.bank.dispatch_consumer.domain.mapper.EntityMapper;
import com.bank.dispatch_consumer.domain.model.CardReplacementEvent;
import com.bank.dispatch_consumer.domain.repo.CardReplacementRepository;
import com.bank.dispatch_consumer.domain.repo.SnapshotCacheRepository;
import com.bank.dispatch_consumer.infrastructure.kafka.DltProducer;
import com.bank.dispatch_consumer.infrastructure.kafka.EventMessage;
import com.bank.dispatch_consumer.infrastructure.kafka.KafkaRxConsumer;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProcessingService {

    private final KafkaRxConsumer consumer;
    private final CardReplacementRepository mongoRepo;
    private final SnapshotCacheRepository cacheRepo;
    private final DltProducer dlt;
    private final EntityMapper mapper;
    private final ObjectMapper json = new ObjectMapper();

    private Disposable subscription;

    @PostConstruct
    public void start() {
        subscription = consumer.stream()
                .observeOn(Schedulers.io())
                .flatMapCompletable(this::routeAndProcess, /*delayErrors*/ false, /*maxConcurrency*/ 4)
                .subscribe(
                        () -> log.info("Stream completed"),
                        err -> log.error("Stream error", err)
                );
    }

    private Completable routeAndProcess(EventMessage<CardReplacementEvent> msg) {
        return Completable.defer(() -> {
            CardReplacementEvent ev = msg.getPayload();
            if (ev == null || ev.getRequestId() == null) {
                return sendToDltAndAck("unknown", safeJson(msg.getRawKafkaValue()), msg);
            }
            int attempt = ev.getAttemptNumber() == null ? 1 : ev.getAttemptNumber();
            return (attempt <= 1 ? handleFirstAttempt(ev, msg) : handleSecondAttempt(ev, msg))
                    .onErrorResumeNext(err -> {
                        log.error("Error processing requestId={}", ev.getRequestId(), err);
                        return sendToDltAndAck(ev.getRequestId(), safeJson(ev), msg);
                    });
        });
    }

    /** 1ra vez → persistir en Mongo (si no existe) y despachar */
    private Completable handleFirstAttempt(CardReplacementEvent ev, EventMessage<CardReplacementEvent> msg) {
        return mongoRepo.existsByRequestId(ev.getRequestId())
                .flatMapCompletable(exists -> exists
                        ? ackDuplicate(ev, msg)
                        : persistAndDispatch(ev, "DISPATCHED", msg));
    }

    /** 2da vez → leer snapshot Redis (si hay), upsert Mongo y despachar */
    private Completable handleSecondAttempt(CardReplacementEvent ev, EventMessage<CardReplacementEvent> msg) {
        return cacheRepo.getSnapshotJson(ev.getRequestId())
                .defaultIfEmpty(safeJson(ev))
                .flatMapCompletable(snapshotJson -> {
                    CardReplacementEvent enriched = mergeSnapshot(snapshotJson, ev);
                    return mongoRepo.existsByRequestId(ev.getRequestId())
                            .flatMapCompletable(exists ->
                                    exists ? updateAndDispatch(enriched, "DISPATCHED_CACHE", msg)
                                            : persistAndDispatch(enriched, "DISPATCHED_CACHE", msg)
                            );
                });
    }

    private Completable ackDuplicate(CardReplacementEvent ev, EventMessage<CardReplacementEvent> msg) {
        return Completable.fromAction(() -> {
            log.warn("Duplicate (1st attempt) requestId={}, ack only", ev.getRequestId());
            msg.ack();
        });
    }

    private Completable persistAndDispatch(CardReplacementEvent ev, String status, EventMessage<CardReplacementEvent> msg) {
        CardReplacementEntity entity = mapper.toEntity(ev);
        entity.setStatus(status);
        // Si añadiste auditoría:
        // entity.setReceivedAt(Instant.now().toEpochMilli());

        return mongoRepo.save(entity)                                   // Single<CardReplacementEntity>
                .flatMapCompletable(saved -> simulateDispatch(ev))      // <- usar flatMapCompletable (devuelve Completable)
                .andThen(Completable.fromAction(msg::ack));             // ack al final
    }

    private Completable updateAndDispatch(CardReplacementEvent ev, String status, EventMessage<CardReplacementEvent> msg) {
        CardReplacementEntity entity = mapper.toEntity(ev);
        entity.setStatus(status);
        // processedAt es Long (epoch millis)
        entity.setProcessedAt(Instant.now().toEpochMilli());

        return mongoRepo.save(entity)
                .flatMapCompletable(saved -> simulateDispatch(ev))
                .andThen(Completable.fromAction(msg::ack));
    }

    /** Aquí enchufas el llamado real a tu sistema externo; por ahora simulado */
    private Completable simulateDispatch(CardReplacementEvent ev) {
        return Completable.timer(150, TimeUnit.MILLISECONDS)
                .doOnComplete(() -> log.info("Dispatched [{}] reqId={} cust={}",
                        ev.getAttemptNumber() != null && ev.getAttemptNumber() > 1 ? "CACHE" : "DB",
                        ev.getRequestId(), ev.getCustomerId()));
    }

    private Completable sendToDltAndAck(String key, String jsonPayload, EventMessage<?> msg) {
        return Completable.fromPublisher(dlt.send(key, jsonPayload))
                .andThen(Completable.fromAction(msg::ack));
    }

    private String safeJson(Object o) {
        try { return json.writeValueAsString(o); }
        catch (Exception e) { return "{}"; }
    }

    /** Merge: snapshot (JSON) tiene prioridad; cae al evento si falta algo */
    private CardReplacementEvent mergeSnapshot(String jsonSnap, CardReplacementEvent base) {
        try {
            var t = json.readTree(jsonSnap);
            var out = new CardReplacementEvent();
            out.setEventId(t.has("eventId") ? t.get("eventId").asText(base.getEventId()) : base.getEventId());
            out.setRequestId(t.has("requestId") ? t.get("requestId").asText(base.getRequestId()) : base.getRequestId());
            out.setCustomerId(t.has("customerId") ? t.get("customerId").asText(base.getCustomerId()) : base.getCustomerId());
            out.setCardPANMasked(t.has("cardPANMasked") ? t.get("cardPANMasked").asText(base.getCardPANMasked()) : base.getCardPANMasked());
            out.setReasonCode(t.has("reasonCode") ? t.get("reasonCode").asText(base.getReasonCode()) : base.getReasonCode());
            out.setPriority(t.has("priority") ? t.get("priority").asText(base.getPriority()) : base.getPriority());
            out.setBranchCode(t.has("branchCode") ? t.get("branchCode").asText(base.getBranchCode()) : base.getBranchCode());
            out.setDeliveryAddress(t.has("deliveryAddress") ? t.get("deliveryAddress").asText(base.getDeliveryAddress()) : base.getDeliveryAddress());
            long ts = t.has("requestedAt")
                    ? t.get("requestedAt").asLong(base.getRequestedAt() == null ? 0 : base.getRequestedAt().toEpochMilli())
                    : (base.getRequestedAt() == null ? 0 : base.getRequestedAt().toEpochMilli());
            out.setRequestedAt(Instant.ofEpochMilli(ts));
            out.setAttemptNumber(t.has("attemptNumber")
                    ? t.get("attemptNumber").asInt(base.getAttemptNumber() == null ? 1 : base.getAttemptNumber())
                    : (base.getAttemptNumber() == null ? 1 : base.getAttemptNumber()));
            out.setCorrelationId(t.has("correlationId") ? t.get("correlationId").asText(base.getCorrelationId()) : base.getCorrelationId());
            out.setStatus(t.has("status") ? t.get("status").asText(base.getStatus()) : base.getStatus());
            return out;
        } catch (Exception e) {
            log.warn("Snapshot inválido; uso base Avro. {}", e.toString());
            return base;
        }
    }
}
