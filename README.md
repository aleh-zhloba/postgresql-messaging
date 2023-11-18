# postgresql-messaging
[![Kotlin](https://img.shields.io/badge/kotlin-1.9.20-blue.svg?logo=kotlin)](http://kotlinlang.org)
[![Apache License V.2](https://img.shields.io/badge/license-Apache%20V.2-blue.svg)](https://github.com/oshai/kotlin-logging/blob/master/LICENSE)

Lightweight Publish-Subscribe (PubSub) layer for PostgreSQL-backed distributed JVM applications.
Cheap inter-process communication without having to bring additional infrastructure.

Uses PostgreSQL `LISTEN / NOTIFY` built-in [asynchronous notifications](https://www.postgresql.org/docs/current/libpq-notify.html).

## Guarantees [^1]
1. "at-most-once" delivery
2. preserves publisher notifications order

## Features
1. Async and reactive
2. [Spring](https://docs.spring.io/spring-integration/docs/current/reference/html/core.html#spring-integration-core-messaging) integration with support of annotated listener methods
3. Micro-batch publishing using artificial delay to reduce the number of database requests

## 1.x release plans
1. Split codebase into **core** and **spring** modules
2. Implement unsubscribe logic
3. Allow to publish notifications in outer transactions

## Quick start
### Install
The library is available on maven central.

#### Gradle
`implementation("io.github.aleh-zhloba:postgresql-messaging:0.5.0")`

### Example of usage

### Spring

## License
**postgresql-messaging** is released under version 2.0 of the [Apache License](https://www.apache.org/licenses/LICENSE-2.0).

[^1]: with no IO exceptions retries configured
