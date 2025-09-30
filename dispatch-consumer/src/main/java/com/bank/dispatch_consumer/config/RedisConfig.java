package com.bank.dispatch_consumer.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

//Configura el acceso a Redis usando la versión reactiva de Spring Data Redis.
//ReactiveStringRedisTemplate → permite guardar y leer strings en Redis de forma reactiva (con Mono/Flux).
//Se usará para:
//Guardar snapshots de los eventos consumidos.
//Controlar reintentos o duplicados (igual que en el producer).
//Resumen: habilita que el microservicio pueda leer/escribir en Redis de manera no bloqueante.
@Configuration
public class RedisConfig {
    @Bean
    public ReactiveStringRedisTemplate reactiveStringRedisTemplate(ReactiveRedisConnectionFactory cf){
        return new ReactiveStringRedisTemplate(cf);
    }
}
