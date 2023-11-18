# postgresql-messaging
![Maven Central](https://img.shields.io/maven-central/v/io.github.aleh-zhloba/postgresql-messaging?versionSuffix=0.5.0)
[![Kotlin](https://img.shields.io/badge/kotlin-1.9.20-blue.svg?logo=kotlin)](http://kotlinlang.org)
[![Apache License V.2](https://img.shields.io/badge/license-Apache%20V.2-blue.svg)](https://github.com/oshai/kotlin-logging/blob/master/LICENSE)

Lightweight Publish-Subscribe (pub/sub) layer for PostgreSQL-backed distributed JVM applications.
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
```kotlin
implementation("io.github.aleh-zhloba:postgresql-messaging:0.5.0")
```
#### Maven
```xml
<dependency>
    <groupId>io.github.aleh-zhloba</groupId>
    <artifactId>postgresql-messaging</artifactId>
    <version>0.5.0</version>
</dependency>
```

### Example of usage

The library comes with the auto-configuration class, so if you have configured JDBC `DataSource` or R2DBC `ConnectionFactory` no additional steps required.

Listen notification messages with handler method:
```kotlin
@PostgresMessageListener(value = ["channel1", "channel2"], skipLocal = true)
fun handleNotification(notification: YourNotificationClass) {
  // ...
}
```

Sending notification messages using `PostgresMessagingTemplate`:
```kotlin
messagingTemplate.convertAndSend("channel1", YourNotificationClass())
```

Reactive API:
```kotlin
val pubSub: PostgresPubSub = R2dbcPostgresPubSub(connectionFactory)

pubSub.subscribe("channel1", "channel2")
  .map { notification ->
    // ...
  }
  .subscribe()

pubSub.publish("channel1", "payload")
  .subscribe()
```

## License
**postgresql-messaging** is released under version 2.0 of the [Apache License](https://www.apache.org/licenses/LICENSE-2.0).

[^1]: with no IO exceptions retries configured
