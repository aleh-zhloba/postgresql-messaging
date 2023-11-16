package io.github.azhloba.postgresql.messaging.pubsub

import io.github.azhloba.postgresql.messaging.spring.config.PostgresMessagingAutoConfiguration
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.r2dbc.R2dbcAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import reactor.kotlin.core.publisher.toFlux
import reactor.test.StepVerifier
import java.time.Duration
import kotlin.random.Random
import kotlin.random.nextULong

@SpringBootTest(classes = [R2dbcAutoConfiguration::class, PostgresMessagingAutoConfiguration::class])
@Testcontainers
class PublishAnsSubscribeTest(
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

        StepVerifier.create(pubSub.publish(channel, payload))
            .expectComplete()
            .verify(duration)
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

        StepVerifier.create(pubSub.publish(channel, payload))
            .verifyComplete()

        verifier.verify(duration)
    }

    @Test
    fun `multiple messages single producer multiple listeners preserving order flow`() {
        val channel = "test_channel_${Random.nextULong()}"
        val messageRange = 1..256

        val messageVerificationList = messageRange.map { it.toString() }
        val verifiers =
            (1..32).map {
                StepVerifier.create(pubSub.subscribe(channel).map { it.payload })
                    .expectNextSequence(messageVerificationList)
                    .thenCancel()
                    .verifyLater()
            }

        Thread.sleep(200)

        StepVerifier.create(
            messageVerificationList.toFlux()
                .concatMap { pubSub.publish(channel, it) }
        )
            .expectComplete()
            .verify()

        verifiers.forEach { it.verify(duration) }
    }

    @Test
    fun `multiple messages multiple producers multiple listeners flow`() {
        val channel = "test_channel_${Random.nextULong()}"
        val messageRange = 1..1024

        val messageVerificationSet = messageRange.map { it.toString() }.toSet()
        val verifiers =
            (1..8).map {
                StepVerifier.create(pubSub.subscribe(channel).map { it.payload })
                    .recordWith { mutableSetOf() }
                    .expectNextCount(messageVerificationSet.size.toLong())
                    .expectRecordedMatches {
                        messageVerificationSet.containsAll(it)
                    }
                    .thenCancel()
                    .verifyLater()
            }

        StepVerifier.create(
            Flux.fromIterable(messageRange)
                .map { it.toString() }
                .flatMap({ pubSub.publish(channel, it).subscribeOn(Schedulers.boundedElastic()) }, 32)
        )
            .verifyComplete()

        verifiers.forEach { it.verify(duration) }
    }

    @Test
    fun `multiple batch messages multiple producers multiple listeners flow`() {
        val channel = "test_channel_${Random.nextULong()}"
        val messageRange = 1..512

        val messageVerificationSet = messageRange.map { it.toString() }.toSet()
        val verifiers =
            (1..8).map {
                StepVerifier.create(pubSub.subscribe(channel).map { it.payload })
                    .recordWith { mutableSetOf() }
                    .expectNextCount(messageVerificationSet.size.toLong())
                    .expectRecordedMatches {
                        messageVerificationSet.containsAll(it)
                    }
                    .thenCancel()
                    .verifyLater()
            }

        StepVerifier.create(
            Flux.fromIterable(messageRange)
                .map { NotificationRequest(channel, it.toString()) }
                .buffer(32)
                .flatMap({ pubSub.publish(it).subscribeOn(Schedulers.boundedElastic()) }, 32)
        )
            .verifyComplete()

        verifiers.forEach { it.verify(duration) }
    }
}
