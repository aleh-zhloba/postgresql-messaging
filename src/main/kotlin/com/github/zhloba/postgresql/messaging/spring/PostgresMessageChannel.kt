package com.github.zhloba.postgresql.messaging.spring

import com.github.zhloba.postgresql.messaging.eventbus.PostgresNotificationEventBus
import com.github.zhloba.postgresql.messaging.spring.converter.NotificationMessageConverter
import org.springframework.messaging.Message
import org.springframework.messaging.support.AbstractSubscribableChannel

class PostgresMessageChannel(
    private val eventBus: PostgresNotificationEventBus,
    private val converter: NotificationMessageConverter,
    val channelName: String,
) : AbstractSubscribableChannel() {
    override fun sendInternal(
        message: Message<*>,
        timeout: Long,
    ): Boolean {
        val notificationMessagePayload = converter.toNotificationPayload(message)

        try {
            eventBus.notify(channelName, notificationMessagePayload)
        } catch (e: Exception) {
            logger.error("Error during sending message in channel: $channelName", e)

            return false
        }

        return true
    }
}
