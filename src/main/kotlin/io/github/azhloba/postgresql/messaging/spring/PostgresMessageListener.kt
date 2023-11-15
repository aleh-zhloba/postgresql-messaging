package io.github.azhloba.postgresql.messaging.spring

import org.springframework.messaging.handler.annotation.MessageMapping

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
@MustBeDocumented
@MessageMapping
annotation class PostgresMessageListener(
    /**
     * List of listen/notify channels
     */
    vararg val value: String = [],
    /**
     * Defines if it's needed to skip events from the same instance
     */
    val skipLocal: Boolean = false
)
