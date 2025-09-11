package com.bank.dispatch_consumer.domain.mapper;

import com.bank.dispatch_consumer.domain.entity.CardReplacementEntity;
import com.bank.dispatch_consumer.domain.model.CardReplacementEvent;
import org.apache.avro.generic.GenericRecord;
import org.mapstruct.Mapper;

import java.time.Instant;

@Mapper(componentModel = "spring")
public interface EntityMapper {

    // Avro GenericRecord -> Modelo de dominio
    default CardReplacementEvent toEvent(GenericRecord gr) {
        if (gr == null) return null;
        CardReplacementEvent e = new CardReplacementEvent();
        e.setEventId((String) gr.get("eventId"));
        e.setRequestId((String) gr.get("requestId"));
        e.setCustomerId((String) gr.get("customerId"));
        e.setCardPANMasked((String) gr.get("cardPANMasked"));
        e.setReasonCode((String) gr.get("reasonCode"));
        e.setPriority((String) gr.get("priority"));
        e.setBranchCode((String) gr.get("branchCode"));
        e.setDeliveryAddress((String) gr.get("deliveryAddress"));
        Object ts = gr.get("requestedAt");
        if (ts instanceof Long l) e.setRequestedAt(Instant.ofEpochMilli(l));
        Object at = gr.get("attemptNumber");
        if (at instanceof Integer i) e.setAttemptNumber(i);
        e.setCorrelationId((String) gr.get("correlationId"));
        e.setStatus((String) gr.get("status"));
        return e;
    }

    // Dominio -> Entidad Mongo (solo campos que EXISTEN en tu Entity)
    default CardReplacementEntity toEntity(CardReplacementEvent ev) {
        if (ev == null) return null;
        CardReplacementEntity en = new CardReplacementEntity();
        // Tu @Id está en requestId
        en.setRequestId(ev.getRequestId());
        en.setCustomerId(ev.getCustomerId());
        en.setCardPANMasked(ev.getCardPANMasked());
        en.setReasonCode(ev.getReasonCode());
        en.setPriority(ev.getPriority());
        en.setBranchCode(ev.getBranchCode());
        en.setDeliveryAddress(ev.getDeliveryAddress());
        en.setRequestedAt(ev.getRequestedAt() != null ? ev.getRequestedAt().toEpochMilli() : 0L);
        en.setAttemptNumber(ev.getAttemptNumber() == null ? 1 : ev.getAttemptNumber());
        en.setCorrelationId(ev.getCorrelationId());
        en.setStatus(ev.getStatus());
        return en;
    }
}
