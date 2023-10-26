package com.github.zhloba.postgresql.messaging.eventbus

import reactor.core.publisher.Flux

interface PostgresNotificationEventBus {
    fun notify(
        channel: String,
        payload: String?,
    )

    fun listen(vararg channels: String): Flux<NotificationEvent>

    fun listen(channels: Collection<String>): Flux<NotificationEvent>
}

interface NotificationEvent {
    val channel: String
    val payload: String?
    val isLocal: Boolean
}
