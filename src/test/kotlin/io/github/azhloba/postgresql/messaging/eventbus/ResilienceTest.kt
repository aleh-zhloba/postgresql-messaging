package io.github.azhloba.postgresql.messaging.eventbus

import eu.rekawek.toxiproxy.Proxy
import eu.rekawek.toxiproxy.ToxiproxyClient
import eu.rekawek.toxiproxy.model.ToxicDirection
import io.github.azhloba.postgresql.messaging.spring.config.PostgresMessagingAutoConfiguration
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.r2dbc.R2dbcAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.Network
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.ToxiproxyContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import reactor.test.StepVerifier
import java.time.Duration
import kotlin.random.Random
import kotlin.random.nextULong

@SpringBootTest(classes = [R2dbcAutoConfiguration::class, PostgresMessagingAutoConfiguration::class])
@Testcontainers
@ContextConfiguration
class ResilienceTest(
    @Autowired private val eventBus: PostgresEventBus
) {
    companion object {
        private val network: Network = Network.newNetwork()

        @Container
        val postgresContainer: PostgreSQLContainer<*> =
            PostgreSQLContainer("postgres:16.0")
                .withDatabaseName("test")
                .withUsername("postgres")
                .withPassword("postgres")
                .withNetwork(network)
                .withNetworkAliases("postgres")

        @Container
        val toxiproxyContainer =
            ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0")
                .withNetwork(network)

        private lateinit var toxiproxyClient: ToxiproxyClient
        private lateinit var proxy: Proxy

        @JvmStatic
        @BeforeAll
        fun init() {
            toxiproxyClient = ToxiproxyClient(toxiproxyContainer.host, toxiproxyContainer.controlPort)
            proxy =
                toxiproxyClient.createProxy(
                    "postgres",
                    "0.0.0.0:8666",
                    "postgres:${postgresContainer.exposedPorts.first()}"
                )
        }

        @JvmStatic
        @DynamicPropertySource
        fun configureProperties(registry: DynamicPropertyRegistry) {
            registry.add("spring.r2dbc.url") {
                "r2dbc:postgresql://${toxiproxyContainer.host}:${toxiproxyContainer.getMappedPort(8666)}/test"
            }
            registry.add("spring.r2dbc.username", postgresContainer::getUsername)
            registry.add("spring.r2dbc.password", postgresContainer::getPassword)
        }
    }

    @Test
    fun `connection failure`() {
        val channel = "test_channel_${Random.nextULong()}"
        val message = "test_message"

        proxy.toxics().resetPeer("RESET_PEER", ToxicDirection.DOWNSTREAM, 0)

        val verifier =
            StepVerifier.create(
                eventBus.listen(channel)
                    .map { it.payload }
            )
                .expectNext(message)
                .thenCancel()
                .verifyLater()

        Thread.sleep(1000)
        proxy.toxics()["RESET_PEER"].remove()

        eventBus.notifyAsync(channel, message)

        verifier.verify(Duration.ofSeconds(2))
    }
}
