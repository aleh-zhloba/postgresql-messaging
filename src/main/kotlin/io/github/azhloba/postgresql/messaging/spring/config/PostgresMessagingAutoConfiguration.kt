package io.github.azhloba.postgresql.messaging.spring.config

import io.github.azhloba.postgresql.messaging.eventbus.PostgresEventBus
import io.github.azhloba.postgresql.messaging.eventbus.R2dbcPostgresEventBus
import io.github.azhloba.postgresql.messaging.eventbus.R2dbcPostgresEventBusConfig
import io.github.azhloba.postgresql.messaging.spring.PostgresMessagingTemplate
import io.github.azhloba.postgresql.messaging.spring.PostgresMethodMessageHandler
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions
import io.r2dbc.spi.Wrapped
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties
import org.springframework.context.annotation.Bean

@AutoConfiguration(after = [DataSourceAutoConfiguration::class])
class PostgresMessagingAutoConfiguration {
    @Bean
    @ConditionalOnBean(DataSourceProperties::class)
    @ConditionalOnMissingBean
    fun connectionFactory(dataSourceProperties: DataSourceProperties): ConnectionFactory {
        return ConnectionFactories.get(
            ConnectionFactoryOptions.parse(dataSourceProperties.url.replaceFirst("jdbc:", "r2dbc:"))
                .mutate()
                .option(ConnectionFactoryOptions.DRIVER, "postgresql")
                .option(ConnectionFactoryOptions.USER, dataSourceProperties.username)
                .option(ConnectionFactoryOptions.PASSWORD, dataSourceProperties.password)
                .build()
        )
    }

    @Bean
    @ConditionalOnMissingBean
    fun r2dbcPostgresEventBusConfig(): R2dbcPostgresEventBusConfig {
        return R2dbcPostgresEventBusConfig()
    }

    @Bean
    @ConditionalOnMissingBean
    fun r2dbcPostgresEventBus(
        connectionFactory: ConnectionFactory,
        config: R2dbcPostgresEventBusConfig
    ): PostgresEventBus {
        return R2dbcPostgresEventBus(connectionFactory.unwrapPostgresConnectionFactory(), config)
    }

    @Bean
    @ConditionalOnMissingBean
    fun postgresMessagingTemplate(postgresEventBus: PostgresEventBus): PostgresMessagingTemplate {
        return PostgresMessagingTemplate(postgresEventBus)
    }

    @Bean
    @ConditionalOnMissingBean
    fun postgresMethodMessageHandler(postgresMessagingTemplate: PostgresMessagingTemplate): PostgresMethodMessageHandler =
        PostgresMethodMessageHandler(postgresMessagingTemplate)

    companion object {
        fun ConnectionFactory.unwrapPostgresConnectionFactory(): PostgresqlConnectionFactory =
            when (this) {
                is PostgresqlConnectionFactory -> this
                is Wrapped<*> -> (this.unwrap() as ConnectionFactory).unwrapPostgresConnectionFactory()
                else -> throw RuntimeException("Can't inject PostgresqlConnectionFactory")
            }
    }
}
