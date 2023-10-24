package com.github.zhloba.spring.messaging.postgresql.eventbus

import com.github.zhloba.spring.messaging.postgresql.config.PostgresMessagingAutoConfiguration
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.r2dbc.R2dbcAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import reactor.test.StepVerifier
import java.time.Duration

@SpringBootTest(classes = [R2dbcAutoConfiguration::class, PostgresMessagingAutoConfiguration::class])
@Testcontainers
class R2DBCPostgresNotificationEventBusTest(
    @Autowired private val eventBus: PostgresNotificationEventBus,
) {
    companion object {
        @Container
        @ServiceConnection
        val postgresContainer: PostgreSQLContainer<*> =
            PostgreSQLContainer("postgres:16.0")
                .withDatabaseName("test")
                .withUsername("postgres")
                .withPassword("postgres")
    }

    @Test
    fun `single message no listeners flow`() {
        val channel = "test_channel1"
        val payload = "test_notification1"

        assertDoesNotThrow { eventBus.notify(channel, payload) }
    }

    @Test
    fun `single message single listener flow`() {
        val channel = "test_channel1"
        val payload = "test_notification1"

        val verifier =
            StepVerifier.create(eventBus.listen(channel))
                .assertNext { assert(it.channel == channel && it.payload == payload) }
                .thenCancel()
                .verifyLater()

        eventBus.notify(channel, payload)

        verifier.verify(Duration.ofMillis(500))
    }

    @Test
    fun `single message multiple listeners flow`() {
        val channel = "test_channel1"
        val payload = "test_notification1"

        val verifiers =
            (1..32).map {
                StepVerifier.create(eventBus.listen(channel))
                    .assertNext { assert(it.channel == channel && it.payload == payload) }
                    .thenCancel()
                    .verifyLater()
            }

        eventBus.notify(channel, payload)

        verifiers.forEach { it.verify(Duration.ofMillis(500)) }
    }

    @Test
    fun `multiple messages multiple listeners preserve ordering flow`() {
        val channel = "test_channel2"
        val messageRange = 1..1024

        val verifiers =
            (1..8).map {
                StepVerifier.create(eventBus.listen(channel).map { it.payload })
                    .expectNext(*messageRange.map { it.toString() }.toTypedArray())
                    .thenCancel()
                    .verifyLater()
            }

        for (i in messageRange) {
            eventBus.notify(channel, "$i")
        }

        verifiers.forEach { it.verify(Duration.ofMillis(500)) }
    }
}
