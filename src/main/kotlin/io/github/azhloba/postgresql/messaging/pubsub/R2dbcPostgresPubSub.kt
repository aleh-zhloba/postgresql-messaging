package io.github.azhloba.postgresql.messaging.pubsub

import io.github.azhloba.postgresql.messaging.pubsub.PostgresPubSub.Companion.isValidPostgresIdentifier
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import io.r2dbc.postgresql.api.Notification
import io.r2dbc.postgresql.api.PostgresqlConnection
import io.r2dbc.postgresql.client.Client
import org.apache.commons.logging.LogFactory
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Schedulers
import reactor.kotlin.core.publisher.toFlux
import reactor.kotlin.core.publisher.toMono
import reactor.util.concurrent.Queues
import reactor.util.concurrent.Queues.SMALL_BUFFER_SIZE
import reactor.util.retry.Retry
import reactor.util.retry.RetryBackoffSpec
import java.time.Duration
import java.util.*
import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.atomic.AtomicReference

/**
 * An [PostgresPubSub] implementation using R2DBC connection.
 * @param connectionFactory PostgreSQL R2DB connection factory
 * @param config configuration
 *
 * @author Aleh Zhloba
 */
class R2dbcPostgresPubSub(
    private val connectionFactory: PostgresqlConnectionFactory,
    private val config: R2dbcPostgresPubSubConfig = R2dbcPostgresPubSubConfig()
) : PostgresPubSub, AutoCloseable {
    private val logger = LogFactory.getLog(javaClass)

    private val inboundSink =
        Sinks.unsafe().many().multicast().onBackpressureBuffer<Notification>(config.inboundBufferSize, false)
    private val outboundSink =
        Sinks.unsafe().many().unicast().onBackpressureBuffer(config.outboundBufferQueue)

    private val outboundPublishingScheduler = Schedulers.newSingle("postgres-notification-publisher", true)

    private val connectionRef: AtomicReference<PostgresqlConnection?> = AtomicReference()

    @Volatile
    private var connectionProcessId: Int = -1

    private val listeningChannels: MutableSet<String> = CopyOnWriteArraySet()

    private var inboundNotificationsProcessing: Disposable? = null

    override fun publishAsync(
        channel: String,
        payload: String?
    ) {
        if (logger.isDebugEnabled) {
            logger.debug("Emitting notification (channel=$channel, payload=$payload) into outbound stream")
        }

        require(isValidPostgresIdentifier(channel))

        outboundPublishingScheduler.schedule {
            val emitResult =
                outboundSink.tryEmitNext(
                    NotificationRequest(channel = channel, payload = payload)
                )

            if (logger.isDebugEnabled) {
                logger.debug("Emit result: $emitResult")
            }
        }
    }

    override fun publish(
        channel: String,
        payload: String?
    ): Mono<Void> = publish(NotificationRequest(channel, payload))

    override fun publish(vararg requests: NotificationRequest): Mono<Void> = publish(requests.toList())

    override fun publish(requests: Collection<NotificationRequest>): Mono<Void> {
        requests.forEach { require(isValidPostgresIdentifier(it.channel)) }

        val notifyMono =
            if (requests.size <= config.outboundBatchSize) {
                notifyChannels(requests)
            } else {
                Flux.fromIterable(requests)
                    .buffer(config.outboundBatchSize)
                    .concatMap { notifyChannels(it) }
                    .then()
            }

        return notifyMono.retryWhen(config.notifyRetry)
    }

    override fun subscribe(vararg channels: String): Flux<NotificationEvent> = subscribe(channels.toList())

    override fun subscribe(channels: Collection<String>): Flux<NotificationEvent> {
        if (logger.isDebugEnabled) {
            logger.debug("Received listen request for channels: ${channels.joinToString(", ")}")
        }

        if (channels.isEmpty()) return Flux.empty()
        val channelsSet =
            channels
                .onEach {
                    require(isValidPostgresIdentifier(it)) {
                        "Channel name must be valid PostgreSQL identifier"
                    }
                }
                .toSet()

        val newChannels = channelsSet.filter { listeningChannels.add(it) }.toSet()
        return listenChannels(newChannels).onErrorResume { Mono.empty() }
            .thenMany(inboundSink.asFlux())
            .filter { notification -> channelsSet.contains(notification.name) }
            .map { notification ->
                PostgresNotificationEvent(
                    notification,
                    connectionProcessId == notification.processId
                )
            }
    }

    override fun close() {
        shutdown()
    }

    override fun connect() {
        logger.debug("Initialize listen/notify connection and run notification processors")

        initConnection()
        initAsyncNotificationPublishing()
    }

    override fun shutdown() {
        logger.debug("Stop listen/notify connection and shutdown notification processors")

        inboundNotificationsProcessing?.dispose()
        outboundSink.tryEmitComplete()
    }

    private fun initConnection() {
        inboundNotificationsProcessing =
            Flux.usingWhen(
                connectionFactory.create(),
                { connection ->
                    logger.info("Postgres listen/notify connection has been established")
                    connectionRef.set(connection)

                    connectionProcessId = extractProcessId(connection)
                    logger.debug("Postgres listen/notify connection processId = $connectionProcessId")

                    listenChannels(listeningChannels)
                        .doOnNext { logger.debug("Start listen connection notifications") }
                        .thenMany(
                            connection.notifications.doOnNext(::emitOutboundNotification)
                        )
                        .then()
                },
                { connection ->
                    logger.debug("Closing Postgres listen/notify connection")

                    connectionRef.set(null)

                    connection.close()
                }
            )
                .retryWhen(
                    config.inboundRetrySpec
                        .doAfterRetry { signal ->
                            logger.error("Error during receiving notification stream", signal.failure())
                        }
                )
                .doFinally { inboundSink.tryEmitComplete() }
                .subscribe(
                    { _ -> logger.debug("Inbound sink processing completed") },
                    { error -> logger.error("Error during inbound sink processing", error) }
                )
    }

    private fun initAsyncNotificationPublishing() {
        outboundSink.asFlux()
            .bufferTimeout(config.outboundBatchSize, config.outboundBatchWindow, true)
            .filter { it.isNotEmpty() }
            .flatMap({
                notifyChannels(it)
                    .retryWhen(
                        config.outboundRetry
                            .doAfterRetry { signal ->
                                logger.error("Error during notification sending stream", signal.failure())
                            }
                    )
                    .onErrorResume { Mono.empty() }
            }, config.outboundParallelism)
            .subscribe(
                { _ -> logger.debug("Outbound sink processing completed") },
                { e -> logger.error("Error during outbound sink processing", e) }
            )
    }

    private fun emitOutboundNotification(notification: Notification) {
        if (logger.isDebugEnabled) {
            logger.debug("Received ${notification.toLogString()}")
        }

        try {
            val emitResult = inboundSink.tryEmitNext(notification)

            if (emitResult.isFailure) {
                logger.error("Emitting ${notification.toLogString()} failure with result = $emitResult")
            }
        } catch (e: Exception) {
            logger.error("Emitting ${notification.toLogString()} fatal error", e)
            throw e
        }
    }

    private fun listenChannels(channels: Set<String>): Mono<Void> = { connectionRef.get() }.toMono()
        .flatMap { connection ->
            channels.toFlux()
                .concatMap { channel ->
                    connection.createStatement("LISTEN $channel").execute()
                }
                .then()
        }
        .doOnError { e ->
            logger.error(
                "Error during channel listen queries execution for channels: " +
                    channels.joinToString(", "),
                e
            )
        }

    private fun notifyChannels(requests: Collection<NotificationRequest>): Mono<Void> =
        getConnection().flatMap { connection ->
            val executionResultsFlux =
                if (requests.size == 1) {
                    val request = requests.first()
                    val statement =
                        connection.createStatement("NOTIFY ${request.channel}, '${request.payload}'")

                    statement.execute()
                } else {
                    val batch =
                        requests.fold(connection.createBatch()) { statement, request ->
                            statement.add("NOTIFY ${request.channel}, '${request.payload}'")
                        }

                    batch.execute()
                }

            executionResultsFlux
                .doOnComplete {
                    if (logger.isDebugEnabled) {
                        logger.debug("Sent ${requests.size} notifications")
                    }
                }
                .then()
        }
            .doOnError { e -> logger.error("${requests.size} notification has not been sent", e) }

    private fun getConnection(): Mono<PostgresqlConnection> = { connectionRef.get() }.toMono()
        .switchIfEmpty(NoActiveConnectionException().toMono())

    // remove this reflection hack as soon as R2DBC connection interface explicitly provides process id
    private fun extractProcessId(connection: PostgresqlConnection): Int {
        try {
            connection::class.java.getDeclaredField("client").let { field ->
                field.trySetAccessible()
                val client = (field.get(connection) as Client)

                return client.processId.get()
            }
        } catch (e: Exception) {
            logger.warn("Can't extract processId from connection, same session detection will not work", e)
        }

        return -1
    }

    private fun Notification.toLogString() =
        "r2dbc notification event (name=${this.name}, parameter=${this.parameter}, processId=${this.processId})"
}

data class R2dbcPostgresPubSubConfig(
    /*
    Inbound events processing properties
     */
    val inboundBufferSize: Int = SMALL_BUFFER_SIZE * 4,
    val inboundRetrySpec: RetryBackoffSpec =
        Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(50))
            .maxBackoff(Duration.ofSeconds(5)),
    /*
    Outbound async events processing properties
     */
    val outboundBufferQueue: Queue<NotificationRequest> = Queues.unbounded<NotificationRequest>().get(),
    val outboundBatchSize: Int = 256,
    val outboundBatchWindow: Duration = Duration.ofMillis(20),
    val outboundParallelism: Int = 1,
    val outboundRetry: RetryBackoffSpec = Retry.backoff(5, Duration.ofMillis(50)),
    /*
    Notify properties
     */
    val notifyRetry: RetryBackoffSpec =
        Retry.backoff(3, Duration.ofMillis(50))
            .filter { it is NoActiveConnectionException }
)

data class PostgresNotificationEvent(
    override val channel: String,
    override val payload: String?,
    override val isLocal: Boolean,
    val processId: Int
) : NotificationEvent {
    constructor(notification: Notification, isLocal: Boolean = false) : this(
        channel = notification.name,
        payload = notification.parameter,
        processId = notification.processId,
        isLocal = isLocal
    )
}

class NoActiveConnectionException : RuntimeException()
