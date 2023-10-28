package io.github.azhloba.postgresql.messaging.spring.converter

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.azhloba.postgresql.messaging.eventbus.NotificationEvent
import org.springframework.messaging.Message

class JacksonNotificationMessageConverter(private val objectMapper: ObjectMapper) : BaseNotificationMessageConverter() {
    override fun fromNotification(notificationEvent: NotificationEvent): Message<String> =
        notificationEvent.payload?.let { payload ->
            val container = objectMapper.readValue(payload, NotificationMessageContainer::class.java)
            buildMessage(notificationEvent, container.payload, container.headers)
        } ?: buildMessage(notificationEvent, null, mapOf())

    override fun toNotificationPayload(message: Message<*>): String {
        return objectMapper.writeValueAsString(
            NotificationMessageContainer(
                payload = message.payload?.let { java.lang.String.valueOf(it) },
                headers = normalizeHeaders(message.headers),
            ),
        )
    }

    data class NotificationMessageContainer(
        val payload: String?,
        val headers: Map<String, Any?>,
    )
}
