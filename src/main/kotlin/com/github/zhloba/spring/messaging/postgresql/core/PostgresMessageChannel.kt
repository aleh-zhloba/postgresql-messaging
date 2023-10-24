package com.github.zhloba.spring.messaging.postgresql.core

import com.github.zhloba.spring.messaging.postgresql.converter.NotificationMessageConverter
import com.github.zhloba.spring.messaging.postgresql.eventbus.PostgresNotificationEventBus
import org.springframework.messaging.Message
import org.springframework.messaging.support.AbstractSubscribableChannel

class PostgresMessageChannel(
    private val eventBus: PostgresNotificationEventBus,
    private val converter: NotificationMessageConverter,
    private val channelName: String,
) : AbstractSubscribableChannel() {
    fun getChannelName(): String = channelName

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
