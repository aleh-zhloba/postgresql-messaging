package io.github.azhloba.postgresql.messaging.spring.converter

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.messaging.Message

class JacksonNotificationMessageConverter(private val objectMapper: ObjectMapper = ObjectMapper()) :
    BaseNotificationMessageConverter() {
    override fun fromPayload(payload: String): MessageContainer {
        return objectMapper.readValue(payload, NotificationMessageContainer::class.java)
    }

    override fun toNotificationPayload(message: Message<*>): String {
        return objectMapper.writeValueAsString(
            NotificationMessageContainer(
                payload = message.payload?.let { java.lang.String.valueOf(it) },
                headers = normalizeHeaders(message.headers)
            )
        )
    }

    data class NotificationMessageContainer(
        override val payload: String?,
        override val headers: Map<String, Any?>
    ) : MessageContainer
}
