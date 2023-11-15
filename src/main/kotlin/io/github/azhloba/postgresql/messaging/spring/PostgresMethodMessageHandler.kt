package io.github.azhloba.postgresql.messaging.spring

import io.github.azhloba.postgresql.messaging.spring.PostgresMessageHeaders.CHANNEL
import io.github.azhloba.postgresql.messaging.spring.PostgresMessageHeaders.IS_LOCAL
import org.springframework.context.SmartLifecycle
import org.springframework.core.annotation.AnnotationUtils
import org.springframework.core.convert.ConversionService
import org.springframework.core.convert.support.DefaultConversionService
import org.springframework.messaging.Message
import org.springframework.messaging.handler.annotation.support.AnnotationExceptionHandlerMethodResolver
import org.springframework.messaging.handler.annotation.support.HeaderMethodArgumentResolver
import org.springframework.messaging.handler.annotation.support.MessageMethodArgumentResolver
import org.springframework.messaging.handler.annotation.support.PayloadMethodArgumentResolver
import org.springframework.messaging.handler.invocation.AbstractExceptionHandlerMethodResolver
import org.springframework.messaging.handler.invocation.AbstractMethodMessageHandler
import org.springframework.messaging.handler.invocation.CompletableFutureReturnValueHandler
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver
import org.springframework.messaging.handler.invocation.HandlerMethodReturnValueHandler
import org.springframework.messaging.handler.invocation.ReactiveReturnValueHandler
import org.springframework.util.comparator.ComparableComparator
import reactor.core.Disposable
import java.lang.reflect.Method
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.Volatile

class PostgresMethodMessageHandler(
    private val postgresMessagingTemplate: PostgresMessagingTemplate
) : AbstractMethodMessageHandler<PostgresMethodMessageHandler.MappingData>(), SmartLifecycle {
    var methodArgumentResolverConversionService: ConversionService = DefaultConversionService()

    private val subscribed = AtomicBoolean()

    @Volatile
    private var subscription: Disposable? = null

    override fun initArgumentResolvers(): List<HandlerMethodArgumentResolver> =
        ArrayList<HandlerMethodArgumentResolver>(customArgumentResolvers).apply {
            add(HeaderMethodArgumentResolver(methodArgumentResolverConversionService, null))
            add(MessageMethodArgumentResolver(postgresMessagingTemplate.messageConverter))
            add(PayloadMethodArgumentResolver(postgresMessagingTemplate.messageConverter))
        }

    override fun initReturnValueHandlers(): List<HandlerMethodReturnValueHandler> =
        ArrayList(customReturnValueHandlers).apply {
            add(CompletableFutureReturnValueHandler())
            add(ReactiveReturnValueHandler())
        }

    override fun isHandler(beanType: Class<*>): Boolean = true

    override fun getMappingForMethod(
        method: Method,
        handlerType: Class<*>
    ): MappingData? {
        val postgresListenerAnnotation =
            AnnotationUtils.findAnnotation(method, PostgresMessageListener::class.java)
        if (postgresListenerAnnotation != null && postgresListenerAnnotation.value.isNotEmpty()) {
            val channels = postgresListenerAnnotation.value.filter { it.isNotBlank() }.toSet()
            val skipLocal = postgresListenerAnnotation.skipLocal

            return MappingData(channels, skipLocal)
        }

        return null
    }

    override fun getDirectLookupDestinations(mapping: MappingData): Set<String> = mapping.channels

    override fun getDestination(message: Message<*>): String {
        return message.headers[CHANNEL].toString()
    }

    override fun getMatchingMapping(
        mapping: MappingData,
        message: Message<*>
    ): MappingData? =
        mapping.takeIf {
            mapping.channels.contains(getDestination(message)) &&
                !(mapping.skipLocal && message.headers[IS_LOCAL] == true)
        }

    override fun getMappingComparator(message: Message<*>): Comparator<MappingData> = ComparableComparator()

    override fun createExceptionHandlerMethodResolverFor(beanType: Class<*>): AbstractExceptionHandlerMethodResolver =
        AnnotationExceptionHandlerMethodResolver(beanType)

    class MappingData(val channels: Set<String>, val skipLocal: Boolean = false) : Comparable<MappingData> {
        override fun compareTo(other: MappingData): Int = 0

        override fun toString(): String = "(channels=[${channels.joinToString(", ")}])"
    }

    override fun start() {
        if (this.subscribed.compareAndSet(false, true)) {
            val channelsToListen = handlerMethods.keys.flatMap { it.channels }.toSet()
            this.subscription = postgresMessagingTemplate.receive(channelsToListen, this)
        }
    }

    override fun stop() {
        this.subscription?.dispose()
    }

    override fun isRunning(): Boolean = this.subscribed.get()
}
