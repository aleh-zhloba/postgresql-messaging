package com.github.zhloba.postgresql.messaging.spring.converter

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.zhloba.postgresql.messaging.eventbus.NotificationEvent
import kotlinx.serialization.json.Json
import org.springframework.messaging.Message

class JacksonNotificationMessageConverter(private val objectMapper: ObjectMapper) : BaseNotificationMessageConverter() {
    override fun fromNotification(notificationEvent: NotificationEvent): Message<String> =
        notificationEvent.payload?.let { payload ->
            val container = Json.decodeFromString<NotificationMessageContainer>(payload)
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
        @JsonProperty("p") val payload: String?,
        @JsonProperty("h") val headers: Map<String, Any?>,
    )
}
