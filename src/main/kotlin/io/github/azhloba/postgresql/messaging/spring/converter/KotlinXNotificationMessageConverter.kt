package io.github.azhloba.postgresql.messaging.spring.converter

import io.github.azhloba.postgresql.messaging.eventbus.NotificationEvent
import kotlinx.serialization.Contextual
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.springframework.messaging.Message
import java.lang.String.valueOf

class KotlinXNotificationMessageConverter : BaseNotificationMessageConverter() {
    override fun fromNotification(notificationEvent: NotificationEvent): Message<String> =
        notificationEvent.payload?.let { payload ->
            val container = Json.decodeFromString<NotificationMessageContainer>(payload)
            buildMessage(notificationEvent, container.payload, container.headers)
        } ?: buildMessage(notificationEvent, null, mapOf())

    override fun toNotificationPayload(message: Message<*>): String {
        return Json.encodeToString(
            NotificationMessageContainer.serializer(),
            NotificationMessageContainer(
                payload = message.payload?.let { valueOf(it) },
                headers = normalizeHeaders(message.headers),
            ),
        )
    }

    @Serializable
    data class NotificationMessageContainer(
        @SerialName("p") val payload: String?,
        @SerialName("h") val headers: Map<String, @Contextual Any?>,
    )
}
