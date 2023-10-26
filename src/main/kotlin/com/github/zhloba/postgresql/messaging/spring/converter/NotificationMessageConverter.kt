package com.github.zhloba.postgresql.messaging.spring.converter

import com.github.zhloba.postgresql.messaging.eventbus.NotificationEvent
import com.github.zhloba.postgresql.messaging.spring.PostgresMessageHeaders.IS_LOCAL
import org.springframework.messaging.Message
import org.springframework.messaging.MessageHeaders
import org.springframework.messaging.support.MessageBuilder

interface NotificationMessageConverter {
    fun fromNotification(notificationEvent: NotificationEvent): Message<String>

    fun toNotificationPayload(message: Message<*>): String
}

abstract class BaseNotificationMessageConverter : NotificationMessageConverter {
    protected fun buildMessage(
        notificationEvent: NotificationEvent,
        messagePayload: String?,
        messageHeaders: Map<String, Any?>,
    ): Message<String> {
        val notificationMessageHeaders =
            HashMap<String, Any?>(messageHeaders).apply {
                putAll(getCommonNotificationHeaders(notificationEvent))
            }

        return MessageBuilder.createMessage(messagePayload, MessageHeaders(notificationMessageHeaders))
    }

    private fun getCommonNotificationHeaders(notificationEvent: NotificationEvent): Map<String, Any> =
        mapOf(IS_LOCAL to notificationEvent.isLocal)

    protected fun normalizeHeaders(headers: Map<String, Any?>): Map<String, Any?> =
        headers.mapValues { (_, value) ->
            when (value) {
                null,
                is Boolean,
                is Number,
                -> value

                else -> value.toString()
            }
        }
}
