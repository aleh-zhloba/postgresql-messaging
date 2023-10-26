package com.github.zhloba.postgresql.messaging.spring

import com.github.zhloba.postgresql.messaging.eventbus.R2DBCPostgresNotificationEventBus
import com.github.zhloba.postgresql.messaging.spring.converter.NotificationMessageConverter
import org.springframework.messaging.Message
import org.springframework.messaging.core.AbstractMessageSendingTemplate
import org.springframework.messaging.core.DestinationResolvingMessageSendingOperations
import org.springframework.messaging.core.MessagePostProcessor

class PostgresMessageSendingTemplate(
    private val eventBus: R2DBCPostgresNotificationEventBus,
    private val notificationMessageConverter: NotificationMessageConverter,
) : AbstractMessageSendingTemplate<PostgresMessageChannel>(),
    DestinationResolvingMessageSendingOperations<PostgresMessageChannel> {
    override fun doSend(
        destination: PostgresMessageChannel,
        message: Message<*>,
    ) {
        destination.send(message)
    }

    override fun send(
        destinationName: String,
        message: Message<*>,
    ) {
        send(resolveMessageChannel(destinationName), message)
    }

    override fun <T : Any> convertAndSend(
        destinationName: String,
        payload: T,
    ) {
        convertAndSend(destinationName, payload, null, null)
    }

    override fun <T : Any> convertAndSend(
        destinationName: String,
        payload: T,
        headers: MutableMap<String, Any>?,
    ) {
        convertAndSend(destinationName, payload, headers, null)
    }

    override fun <T : Any> convertAndSend(
        destinationName: String,
        payload: T,
        postProcessor: MessagePostProcessor?,
    ) {
        convertAndSend(destinationName, payload, null, postProcessor)
    }

    override fun <T : Any> convertAndSend(
        destinationName: String,
        payload: T,
        headers: MutableMap<String, Any>?,
        postProcessor: MessagePostProcessor?,
    ) {
        val message = doConvert(payload, headers, postProcessor)
        send(destinationName, message)
    }

    private fun resolveMessageChannel(destination: String): PostgresMessageChannel {
        return PostgresMessageChannel(eventBus, notificationMessageConverter, destination)
    }
}
