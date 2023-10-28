package io.github.azhloba.postgresql.messaging.eventbus

import io.r2dbc.postgresql.PostgresqlConnectionFactory
import io.r2dbc.postgresql.api.Notification
import io.r2dbc.postgresql.api.PostgresqlConnection
import io.r2dbc.postgresql.client.Client
import org.apache.commons.logging.LogFactory
import org.springframework.context.SmartLifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.kotlin.core.publisher.toFlux
import reactor.kotlin.core.publisher.toMono
import reactor.util.retry.Retry
import java.time.Duration
import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.atomic.AtomicReference

class R2DBCPostgresNotificationEventBus(
    private val connectionFactory: PostgresqlConnectionFactory,
) : PostgresNotificationEventBus, SmartLifecycle {
    private val logger = LogFactory.getLog(javaClass)

    private val inboundSink = Sinks.many().multicast().onBackpressureBuffer<Notification>(512, false)
    private val outboundSink = Sinks.many().unicast().onBackpressureBuffer<CreateNotificationRequest>()

    private val connectionRef: AtomicReference<PostgresqlConnection?> = AtomicReference()

    @Volatile
    private var connectionProcessId: Int = -1

    private val listeningChannels: MutableSet<String> = CopyOnWriteArraySet()

    private var outboundNotificationsBufferSize = 512
    private var outboundNotificationBufferWindow = Duration.ofMillis(50)
    private var outboundNotificationParallelism = 1

    private var inboundNotificationsProcessing: Disposable? = null
    private var outboundNotificationsProcessing: Disposable? = null

    override fun notify(
        channel: String,
        payload: String?,
    ) {
        if (logger.isDebugEnabled) {
            logger.debug("Emitting notification [channel=$channel, payload=$payload] into outbound stream")
        }

        outboundSink.tryEmitNext(CreateNotificationRequest(channel = channel, payload = payload))
    }

    override fun listen(vararg channels: String): Flux<NotificationEvent> {
        if (channels.isEmpty()) return Flux.empty()

        val channelsSet = setOf(*channels)
        val newChannels = channelsSet.filter { listeningChannels.add(it) }.toSet()

        return listenChannels(newChannels).thenMany(inboundSink.asFlux())
            .doOnNext {
                if (logger.isDebugEnabled) {
                    logger.debug("Received notification [channel: ${it.name}, payload: ${it.parameter}]")
                }
            }
            .filter { notification -> channelsSet.contains(notification.name) }
            .map { notification -> PostgresNotificationEvent(notification, connectionProcessId == notification.processId) }
    }

    override fun listen(channels: Collection<String>): Flux<NotificationEvent> = listen(*channels.toTypedArray())

    override fun start() {
        logger.debug("Start listen/notify flow")

        initConnection()
    }

    override fun stop() {
        logger.debug("Stop listen/notify flow")

        inboundNotificationsProcessing?.dispose()
    }

    override fun isRunning(): Boolean = inboundNotificationsProcessing?.let { !it.isDisposed } ?: false

    private fun initConnection() {
        inboundNotificationsProcessing =
            Flux.usingWhen(
                connectionFactory.create(),
                { connection ->
                    logger.info("Postgres listen/notify connection has been established")

                    connectionRef.set(connection)
                    connectionProcessId = extractProcessId(connection)

                    listenChannels(listeningChannels)
                        .doOnNext { logger.debug("Start listen connection notifications") }
                        .thenMany(
                            connection.notifications
                                .doOnNext {
                                    try {
                                        inboundSink.emitNext(it, Sinks.EmitFailureHandler.FAIL_FAST)
                                    } catch (e: Exception) {
                                        logger.error(
                                            "Notification [name=${it.name}, " +
                                                "parameter=${it.parameter}, processId=${it.processId}] can't be emitted",
                                            e,
                                        )
                                    }
                                },
                        )
                        .then()
                },
                { connection ->
                    logger.debug("Closing Postgres listen/notify connection")

                    connectionRef.set(null)

                    connection.close()
                },
            )
                .retryWhen(
                    Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(50))
                        .maxBackoff(Duration.ofSeconds(5))
                        .doAfterRetry { signal ->
                            logger.error("Error during notification receiving stream", signal.failure())
                        },
                )
                .doFinally { inboundSink.tryEmitComplete() }
                .subscribe(
                    { _ -> logger.debug("Inbound sink processing completed") },
                    { error -> logger.error("Error during inbound sink processing", error) },
                )

        outboundNotificationsProcessing =
            outboundSink.asFlux()
                .bufferTimeout(outboundNotificationsBufferSize, outboundNotificationBufferWindow)
                .filter { it.isNotEmpty() }
                .flatMap({ notifyChannels(it) }, outboundNotificationParallelism)
                .retryWhen(
                    Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(50))
                        .maxBackoff(Duration.ofSeconds(5))
                        .doAfterRetry { signal ->
                            logger.error("Error during notification sending stream", signal.failure())
                        },
                )
                .doFinally { outboundSink.tryEmitComplete() }
                .subscribe(
                    { _ -> logger.debug("Outbound sink processing completed") },
                    { error -> logger.error("Error during outbound sink processing", error) },
                )
    }

    private fun listenChannels(channels: Set<String>): Mono<Void> =
        connectionRef.get()?.let { connection ->
            channels.toFlux().concatMap { channel ->
                connection.createStatement("LISTEN $channel").execute()
            }
                .then()
                .doOnError { error ->
                    logger.error(
                        "Error during channel listen queries execution for channels: " +
                            channels.joinToString(", "),
                        error,
                    )
                }
        } ?: Mono.empty()

    private fun notifyChannels(requests: List<CreateNotificationRequest>): Mono<Void> =
        connectionRef.get().toMono()
            .switchIfEmpty(Mono.error(NoActiveConnectionException()))
            .flatMap { connection ->
                val executionResultsFlux =
                    if (requests.size == 1) {
                        val request = requests.first()
                        val statement = connection.createStatement("NOTIFY ${request.channel}, '${request.payload}'")

                        statement.execute()
                    } else {
                        val batch =
                            requests.fold(connection.createBatch()) { statement, request ->
                                statement.add("NOTIFY ${request.channel}, '${request.payload}'")
                            }

                        batch.execute()
                    }

                executionResultsFlux.doOnComplete {
                    if (logger.isDebugEnabled) {
                        logger.debug("Sent ${requests.size} notification")
                    }
                }.then()
            }
            .retryWhen(
                Retry.backoff(5, Duration.ofMillis(50))
                    .filter { it is NoActiveConnectionException }
                    .doAfterRetry { signal ->
                        logger.error(
                            "Retry sending ${requests.size} notifications",
                            signal.failure(),
                        )
                    },
            )
            .doOnError { error -> logger.error("${requests.size} notification has not been sent", error) }
            .onErrorResume { Mono.empty() }

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
}

data class PostgresNotificationEvent(
    override val channel: String,
    override val payload: String?,
    override val isLocal: Boolean,
    val processId: Int,
) : NotificationEvent {
    constructor(notification: Notification, isLocal: Boolean = false) : this(
        channel = notification.name,
        payload = notification.parameter,
        processId = notification.processId,
        isLocal = isLocal,
    )
}

data class CreateNotificationRequest(
    val channel: String,
    val payload: String?,
)

class NoActiveConnectionException : RuntimeException()
