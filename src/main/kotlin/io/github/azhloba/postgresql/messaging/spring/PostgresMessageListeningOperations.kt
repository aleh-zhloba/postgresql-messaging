package io.github.azhloba.postgresql.messaging.spring

import org.springframework.messaging.MessageHandler
import reactor.core.Disposable

interface PostgresMessageListeningOperations<D> {
    fun listen(
        vararg destinations: D,
        handler: MessageHandler
    ): Disposable

    fun listen(
        destinations: Collection<D>,
        handler: MessageHandler
    ): Disposable
}
