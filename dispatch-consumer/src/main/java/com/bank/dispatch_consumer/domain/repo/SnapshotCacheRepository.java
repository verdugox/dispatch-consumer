package com.bank.dispatch_consumer.domain.repo;

import io.reactivex.rxjava3.core.Maybe;

//Está en domain.repo → o sea, es un puerto de salida en tu arquitectura hexagonal.
//Define una abstracción para consultar snapshots de eventos a partir de un requestId.
//No sabe si el snapshot está en Redis, en memoria, en DB o en un archivo. Solo dice:
//“Dado un requestId, dame el JSON del snapshot si existe”.

public interface SnapshotCacheRepository {
    Maybe<String> getSnapshotJson(String requestId);
}
