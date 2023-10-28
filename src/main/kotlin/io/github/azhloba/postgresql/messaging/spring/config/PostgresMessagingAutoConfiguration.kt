package io.github.azhloba.postgresql.messaging.spring.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.azhloba.postgresql.messaging.eventbus.PostgresNotificationEventBus
import io.github.azhloba.postgresql.messaging.eventbus.R2DBCPostgresNotificationEventBus
import io.github.azhloba.postgresql.messaging.spring.PostgresMessageSendingTemplate
import io.github.azhloba.postgresql.messaging.spring.PostgresMethodMessageHandler
import io.github.azhloba.postgresql.messaging.spring.converter.JacksonNotificationMessageConverter
import io.github.azhloba.postgresql.messaging.spring.converter.NotificationMessageConverter
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Wrapped
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.messaging.converter.MappingJackson2MessageConverter
import org.springframework.messaging.converter.MessageConverter

@Configuration
class PostgresMessagingAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    fun messageConverter(): MessageConverter {
        return MappingJackson2MessageConverter().apply {
            setSerializedPayloadClass(String::class.java)
        }
    }

    @Bean
    @ConditionalOnMissingBean
    fun messageContainerConverter(messageConverter: MessageConverter): NotificationMessageConverter {
        val objectMapper =
            when (messageConverter) {
                is MappingJackson2MessageConverter -> messageConverter.objectMapper
                else -> ObjectMapper()
            }


        objectMapper.registerKotlinModule()

        return JacksonNotificationMessageConverter(objectMapper)
    }

    @Bean
    @ConditionalOnMissingBean
    fun postgresEventBus(connectionFactory: ConnectionFactory): PostgresNotificationEventBus {
        return R2DBCPostgresNotificationEventBus(connectionFactory.unwrapPostgresConnectionFactory())
    }

    @Bean
    @ConditionalOnMissingBean
    fun postgresMessageSendingTemplate(
        postgresEventBus: PostgresNotificationEventBus,
        messageConverter: MessageConverter,
        messageContainerConverter: NotificationMessageConverter,
    ): PostgresMessageSendingTemplate {
        return PostgresMessageSendingTemplate(postgresEventBus, messageContainerConverter).apply {
            this.messageConverter = messageConverter
        }
    }

    @Bean
    @ConditionalOnMissingBean
    fun postgresMethodMessageHandler(
        postgresEventBus: PostgresNotificationEventBus,
        messageConverter: MessageConverter,
        messageContainerConverter: NotificationMessageConverter,
    ): PostgresMethodMessageHandler = PostgresMethodMessageHandler(postgresEventBus, messageConverter, messageContainerConverter)

    private fun ConnectionFactory.unwrapPostgresConnectionFactory(): PostgresqlConnectionFactory =
        when (this) {
            is PostgresqlConnectionFactory -> this
            is Wrapped<*> -> (this.unwrap() as ConnectionFactory).unwrapPostgresConnectionFactory()
            else -> throw RuntimeException("Can't inject PostgresqlConnectionFactory")
        }
}
