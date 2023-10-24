package com.github.zhloba.spring.messaging.postgresql.core

import org.springframework.messaging.handler.annotation.MessageMapping

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
@MustBeDocumented
@MessageMapping
annotation class PostgresNotificationListener(
    /**
     * List of listen/notify channels
     * @return list of channels
     */
    vararg val value: String = [],
    val skipLocal: Boolean = false,
)
