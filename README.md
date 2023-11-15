# postgresql-messaging 
[![Kotlin](https://img.shields.io/badge/kotlin-1.9.20-blue.svg?logo=kotlin)](http://kotlinlang.org) 
[![Apache License V.2](https://img.shields.io/badge/license-Apache%20V.2-blue.svg)](https://github.com/oshai/kotlin-logging/blob/master/LICENSE)

Convenient event bus for PostgreSQL-backed distributed JVM applications. 

Kotlin wrapper over PostgreSQL `LISTEN / NOTIFY` built-in asynchronous notification system.

## Features
1. Async and reactive
2. Spring integration using [Spring Messaging](https://docs.spring.io/spring-integration/docs/current/reference/html/core.html#spring-integration-core-messaging) abstractions
3. Micro-batch publishing using artificial delay to reduce the number of database requests

## Guarantees [^1]
1. at-most-once delivery 
2. notification ordering
[^1]: with no IO exceptions retries configured

## License

**postgresql-messaging** is released under version 2.0 of the [Apache License](https://www.apache.org/licenses/LICENSE-2.0).
