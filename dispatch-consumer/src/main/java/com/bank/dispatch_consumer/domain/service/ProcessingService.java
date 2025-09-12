// src/main/java/com/bank/dispatch_consumer/domain/service/ProcessingService.java
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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
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
    private final MeterRegistry meter;              // <— Micrometer
    private final ObjectMapper json = new ObjectMapper();

    private Disposable subscription;

    // ====== Métricas ======
    private Counter consumed;
    private Counter duplicateAck;
    private Counter persistedFirst;
    private Counter upsertSecond;
    private Counter sentToDlt;
    private Counter processingErrors;
    private Timer   processTimer;

    @PostConstruct
    public void start() {
        // Inicializa métricas (una sola vez)
        consumed        = Counter.builder("dispatch_events_consumed_total")
                .description("Eventos consumidos desde Kafka").register(meter);
        duplicateAck    = Counter.builder("dispatch_duplicate_ack_total")
                .description("Primer intento duplicado, solo ACK").register(meter);
        persistedFirst  = Counter.builder("dispatch_persist_first_total")
                .description("Insert en Mongo en primer intento").register(meter);
        upsertSecond    = Counter.builder("dispatch_upsert_second_total")
                .description("Upsert en Mongo con snapshot (segundo intento)")
                .register(meter);
        sentToDlt       = Counter.builder("dispatch_dlt_total")
                .description("Eventos enviados a DLT").register(meter);
        processingErrors= Counter.builder("dispatch_processing_errors_total")
                .description("Errores durante el procesamiento").register(meter);
        processTimer    = Timer.builder("dispatch_process_timer")
                .description("Duración del procesamiento por evento")
                .publishPercentileHistogram()
                .register(meter);

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
            long start = System.nanoTime();
            try {
                consumed.increment();

                CardReplacementEvent ev = msg.getPayload();
                if (ev == null || ev.getRequestId() == null) {
                    sentToDlt.increment();
                    return sendToDltAndAck("unknown", safeJson(msg.getRawKafkaValue()), msg)
                            .doOnTerminate(() ->
                                    processTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS));
                }

                int attempt = ev.getAttemptNumber() == null ? 1 : ev.getAttemptNumber();
                return (attempt <= 1 ? handleFirstAttempt(ev, msg) : handleSecondAttempt(ev, msg))
                        .doOnTerminate(() ->
                                processTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS))
                        .onErrorResumeNext(err -> {
                            processingErrors.increment();
                            log.error("Error processing requestId={}", ev.getRequestId(), err);
                            sentToDlt.increment();
                            return sendToDltAndAck(ev.getRequestId(), safeJson(ev), msg);
                        });

            } catch (Throwable t) {
                processingErrors.increment();
                log.error("Fatal error before routing", t);
                // asegúrate de ack para no bloquear el flujo
                return Completable.fromAction(msg::ack)
                        .doOnTerminate(() ->
                                processTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS));
            }
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
            duplicateAck.increment();
            log.warn("Duplicate (1st attempt) requestId={}, ack only", ev.getRequestId());
            msg.ack();
        });
    }

    private Completable persistAndDispatch(CardReplacementEvent ev, String status, EventMessage<CardReplacementEvent> msg) {
        CardReplacementEntity entity = mapper.toEntity(ev);
        entity.setStatus(status);
        // entity.setReceivedAt(Instant.now().toEpochMilli());

        return mongoRepo.save(entity)
                .doOnSuccess(saved -> persistedFirst.increment())
                .flatMapCompletable(saved -> simulateDispatch(ev))
                .andThen(Completable.fromAction(msg::ack));
    }

    private Completable updateAndDispatch(CardReplacementEvent ev, String status, EventMessage<CardReplacementEvent> msg) {
        CardReplacementEntity entity = mapper.toEntity(ev);
        entity.setStatus(status);
        entity.setProcessedAt(Instant.now().toEpochMilli());

        return mongoRepo.save(entity)
                .doOnSuccess(saved -> upsertSecond.increment())
                .flatMapCompletable(saved -> simulateDispatch(ev))
                .andThen(Completable.fromAction(msg::ack));
    }

    /** Simula el despacho externo */
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
