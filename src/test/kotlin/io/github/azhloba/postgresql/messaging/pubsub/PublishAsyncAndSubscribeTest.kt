package io.github.azhloba.postgresql.messaging.pubsub

import io.github.azhloba.postgresql.messaging.spring.config.PostgresMessagingAutoConfiguration
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
import kotlin.random.Random
import kotlin.random.nextULong

@SpringBootTest(classes = [R2dbcAutoConfiguration::class, PostgresMessagingAutoConfiguration::class])
@Testcontainers
class PublishAsyncAndSubscribeTest(
    @Autowired private val pubSub: PostgresPubSub
) {
    companion object {
        @Container
        @ServiceConnection
        val postgresContainer: PostgreSQLContainer<*> =
            PostgreSQLContainer("postgres:16.0")
                .withDatabaseName("test")
                .withUsername("postgres")
                .withPassword("postgres")
        val duration: Duration = Duration.ofSeconds(1)
    }

    @Test
    fun `single message single producer no listeners flow`() {
        val channel = "test_channel_${Random.nextULong()}"
        val payload = "test_notification"

        assertDoesNotThrow { pubSub.publishAsync(channel, payload) }
    }

    @Test
    fun `single message single producer single listener flow`() {
        val channel = "test_channel_${Random.nextULong()}"
        val payload = "test_notification"

        val verifier =
            StepVerifier.create(pubSub.subscribe(channel))
                .assertNext { assert(it.channel == channel && it.payload == payload) }
                .thenCancel()
                .verifyLater()

        pubSub.publishAsync(channel, payload)

        verifier.verify(duration)
    }

    @Test
    fun `single message single producer multiple listeners flow`() {
        val channel = "test_channel_${Random.nextULong()}"
        val payload = "test_notification"

        val verifiers =
            (1..32).map {
                StepVerifier.create(pubSub.subscribe(channel))
                    .assertNext { assert(it.channel == channel && it.payload == payload) }
                    .thenCancel()
                    .verifyLater()
            }

        pubSub.publishAsync(channel, payload)

        verifiers.forEach { it.verify(duration) }
    }

    @Test
    fun `multiple messages single producer multiple listeners preserve ordering flow`() {
        val channel = "test_channel_${Random.nextULong()}"
        val messageRange = 1..1024

        val messageVerificationList = messageRange.map { it.toString() }
        val verifiers =
            (1..8).map {
                StepVerifier.create(pubSub.subscribe(channel).map { it.payload })
                    .expectNextSequence(messageVerificationList)
                    .thenCancel()
                    .verifyLater()
            }

        for (i in messageRange) {
            pubSub.publishAsync(channel, "$i")
        }

        verifiers.forEach { it.verify(duration) }
    }

    @Test
    fun `multiple messages multiple producers multiple listeners`() {
        val channel = "test_channel_${Random.nextULong()}"
        val messageRange = 1..1024

        val messageVerificationList = messageRange.map { it.toString() }
        val verifiers =
            (1..8).map {
                StepVerifier.create(pubSub.subscribe(channel).map { it.payload })
                    .expectNextSequence(messageVerificationList)
                    .thenCancel()
                    .verifyLater()
            }

        for (i in messageRange) {
            pubSub.publishAsync(channel, "$i")
        }

        verifiers.forEach { it.verify(duration) }
    }
}
