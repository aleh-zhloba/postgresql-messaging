package io.github.azhloba.postgresql.messaging.spring

import io.github.azhloba.postgresql.messaging.pubsub.PostgresPubSub
import io.github.azhloba.postgresql.messaging.spring.converter.JacksonNotificationMessageConverter
import io.github.azhloba.postgresql.messaging.spring.converter.NotificationMessageConverter
import org.springframework.context.SmartLifecycle
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
import java.util.concurrent.atomic.AtomicBoolean

class PostgresMessagingTemplate(
    private val pubSub: PostgresPubSub
) : AbstractMessageSendingTemplate<String>(),
    PostgresMessageListeningOperations<String>,
    DestinationResolvingMessageSendingOperations<String>,
    SmartLifecycle {
    private val initializedConnection = AtomicBoolean()

    var messageHandlingScheduler: Scheduler = Schedulers.boundedElastic()
    var messageHandlingExecutor: Executor? = null
        set(value) {
            if (value != null) messageHandlingScheduler = Schedulers.fromExecutor(value)
        }
    var notificationMessageConverter: NotificationMessageConverter = JacksonNotificationMessageConverter()

    override fun listen(
        vararg destinations: String,
        handler: MessageHandler
    ): Disposable = listen(destinations.toSet(), handler)

    override fun listen(
        destinations: Collection<String>,
        handler: MessageHandler
    ): Disposable {
        return pubSub.subscribe(destinations)
            .publishOn(messageHandlingScheduler)
            .flatMap { notificationEvent ->
                Mono.fromCallable {
                    handler.handleMessage(notificationMessageConverter.fromNotification(notificationEvent))
                }
                    .onErrorResume { e ->
                        logger.error("Notification $notificationEvent handling failed, event skipped", e)
                        Mono.empty()
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

            pubSub.publishAsync(destination, notificationMessagePayload)
        } catch (e: Exception) {
            logger.error("Error during sending message in channel: $destination", e)
            throw e
        }
    }

    override fun start() {
        if (this.initializedConnection.compareAndSet(false, true)) {
            pubSub.connect()
        }
    }

    override fun stop() {
        pubSub.shutdown()
    }

    override fun isRunning(): Boolean = this.initializedConnection.get()
}
