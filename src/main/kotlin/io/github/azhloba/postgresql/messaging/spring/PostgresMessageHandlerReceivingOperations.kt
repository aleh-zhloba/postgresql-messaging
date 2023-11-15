package io.github.azhloba.postgresql.messaging.spring

import org.springframework.messaging.MessageHandler
import reactor.core.Disposable

interface PostgresMessageHandlerReceivingOperations<D> {
    fun receive(
        vararg destinations: D,
        handler: MessageHandler
    ): Disposable

    fun receive(
        destinations: Collection<D>,
        handler: MessageHandler
    ): Disposable
}
