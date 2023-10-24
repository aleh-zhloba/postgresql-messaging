package com.github.zhloba.spring.messaging.postgresql.core

import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable

@Serializable
data class MessageDataContainer(
    val payload: String?,
    val headers: Map<String, @Contextual Any>,
)
