package com.bank.dispatch_consumer.domain.entity;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document("card_replacements")
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CardReplacementEntity {
    @Id
    private String requestId;

    private String customerId;
    private String cardPANMasked;
    private String reasonCode;
    private String priority;
    private String branchCode;
    private String deliveryAddress;

    private long requestedAt;
    private int attemptNumber;
    private String correlationId;
    private String status;

    // nuevos (recomendado)
    private Long receivedAt;    // epoch millis
    private Long processedAt;   // epoch millis
    private String rawPayload;
    private String errorMessage;
}
