package io.github.azhloba.postgresql.messaging.spring

import io.github.azhloba.postgresql.messaging.eventbus.PostgresEventBus
import io.github.azhloba.postgresql.messaging.spring.converter.JacksonNotificationMessageConverter
import io.github.azhloba.postgresql.messaging.spring.converter.NotificationMessageConverter
import org.springframework.messaging.Message
import org.springframework.messaging.MessageHandler
import org.springframework.messaging.core.AbstractMessageSendingTemplate
import org.springframework.messaging.core.DestinationResolvingMessageSendingOperations
import org.springframework.messaging.core.MessagePostProcessor
import reactor.core.Disposable
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.util.concurrent.Executor

class PostgresMessagingTemplate(
    private val eventBus: PostgresEventBus
) : AbstractMessageSendingTemplate<String>(),
    PostgresMessageHandlerReceivingOperations<String>,
    DestinationResolvingMessageSendingOperations<String> {
    var messageHandlingScheduler: Scheduler = Schedulers.boundedElastic()
    var messageHandlingExecutor: Executor? = null
        set(value) {
            if (value != null) messageHandlingScheduler = Schedulers.fromExecutor(value)
        }
    var notificationMessageConverter: NotificationMessageConverter = JacksonNotificationMessageConverter()

    override fun receive(
        vararg destinations: String,
        handler: MessageHandler
    ): Disposable = receive(destinations.toSet(), handler)

    override fun receive(
        destinations: Collection<String>,
        handler: MessageHandler
    ): Disposable {
        return eventBus.listen(destinations)
            .publishOn(messageHandlingScheduler)
            .flatMap { notificationEvent ->
                Mono.fromCallable {
                    handler.handleMessage(notificationMessageConverter.fromNotification(notificationEvent))
                }
            }
            .subscribe()
    }

    override fun send(
        destinationName: String,
        message: Message<*>
    ) {
        doSend(destinationName, message)
    }

    override fun <T : Any> convertAndSend(
        destinationName: String,
        payload: T
    ) {
        convertAndSend(destinationName, payload, null, null)
    }

    override fun <T : Any> convertAndSend(
        destinationName: String,
        payload: T,
        headers: MutableMap<String, Any>?
    ) {
        convertAndSend(destinationName, payload, headers, null)
    }

    override fun <T : Any> convertAndSend(
        destinationName: String,
        payload: T,
        postProcessor: MessagePostProcessor?
    ) {
        convertAndSend(destinationName, payload, null, postProcessor)
    }

    override fun <T : Any> convertAndSend(
        destinationName: String,
        payload: T,
        headers: MutableMap<String, Any>?,
        postProcessor: MessagePostProcessor?
    ) {
        val message = doConvert(payload, headers, postProcessor)
        send(destinationName, message)
    }

    override fun doSend(
        destination: String,
        message: Message<*>
    ) {
        try {
            val notificationMessagePayload = notificationMessageConverter.toNotificationPayload(message)

            eventBus.notifyAsync(destination, notificationMessagePayload)
        } catch (e: Exception) {
            logger.error("Error during sending message in channel: $destination", e)
            throw e
        }
    }
}
