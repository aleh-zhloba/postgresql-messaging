package io.github.azhloba.postgresql.messaging.pubsub

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

/**
 * Generic interface for Postgresql publish/subscribe implementations
 *
 * @author Aleh Zhloba
 */
interface PostgresPubSub : AutoCloseable {
    /**
     * Subscribe to the given channels notification events
     * @param channels listening channels
     * @return notification events [Flux]
     */
    fun subscribe(vararg channels: String): Flux<out NotificationEvent>

    /**
     * Subscribe to the given channels notification events
     * @param channels listening channels
     * @return notification events [Flux]
     */
    fun subscribe(channels: Collection<String>): Flux<out NotificationEvent>

    /**
     * Publish notification event
     * @param channel event destination channel
     * @param payload event payload, may be null
     * @return operation result [Mono]
     */
    fun publish(
        channel: String,
        payload: String?
    ): Mono<Void>

    /**
     * Publish notification events
     * @param requests notification event requests
     * @return operation result [Mono]
     */
    fun publish(vararg requests: NotificationRequest): Mono<Void>

    /**
     * Publish notification events
     * @param requests notification event requests
     * @return operation result [Mono]
     */
    fun publish(requests: Collection<NotificationRequest>): Mono<Void>

    /**
     * Publish notification event asynchronously
     * @param channel event destination channel
     * @param payload event payload, may be null
     */
    fun publishAsync(
        channel: String,
        payload: String?
    )

    fun connect()

    fun shutdown()

    companion object {
        private const val MAX_IDENTIFIER_SIZE = 63

        fun isValidPostgresIdentifier(channel: String): Boolean =
            channel.toByteArray(Charsets.UTF_8).size <= MAX_IDENTIFIER_SIZE &&
                channel.all { it.isLetterOrDigit() || "_$".contains(it) }
    }
}

data class NotificationRequest(
    val channel: String,
    val payload: String?
)

interface NotificationEvent {
    val channel: String
    val payload: String?

    /**
     * Return true in case of notification event was sent by the same instance
     */
    val isLocal: Boolean
}
