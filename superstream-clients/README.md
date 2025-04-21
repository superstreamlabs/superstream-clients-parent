# Superstream Clients

A Java library for automatically optimizing Kafka producer configurations based on topic-specific recommendations.

## Overview

Superstream Clients works as a Java agent that intercepts Kafka producer creation and applies optimized configurations without requiring any code changes in your application. It dynamically retrieves optimization recommendations from Superstream and applies them based on impact analysis.

## Supported Libraries

Works with any Java library that depends on `kafka-clients`, including:

- Apache Kafka Clients
- Spring Kafka
- Alpakka Kafka (Akka Kafka)
- Kafka Streams
- Kafka Connect
- Any custom wrapper around the Kafka Java client

## Features

- **Zero-code integration**: No code changes required in your application
- **Dynamic configuration**: Applies optimized settings based on topic-specific recommendations
- **Intelligent optimization**: Identifies the most impactful topics to optimize
- **Graceful fallback**: Falls back to default settings if optimization fails

## Linger Time Configuration

The `linger.ms` parameter follows this priority:

1. `SUPERSTREAM_LINGER_MS` environment variable (if set)
2. Superstream's optimized value

## Installation

### Maven

```xml
<dependency>
    <groupId>ai.superstream</groupId>
    <artifactId>superstream-clients</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Gradle

```groovy
implementation 'ai.superstream:superstream-clients:1.0.0'
```

## Usage

Add the Java agent to your application's startup command:

```bash
java -javaagent:/path/to/superstream-clients-1.0.0.jar -jar your-application.jar
```

For Spring Boot applications:

```bash
java -javaagent:/path/to/superstream-clients-1.0.0.jar -jar your-spring-boot-app.jar
```

### Required Environment Variables

- `SUPERSTREAM_TOPICS_LIST`: Comma-separated list of topics your application produces to

### Optional Environment Variables

- `SUPERSTREAM_LINGER_MS`: Set a specific linger.ms value (in milliseconds)
- `SUPERSTREAM_DISABLED`: Set to "true" to disable optimization

Example:
```bash
export SUPERSTREAM_TOPICS_LIST=orders,payments,user-events
export SUPERSTREAM_EXPLICIT_LINGER_MS=2000
```

## Prerequisites

- Java 11 or higher
- Kafka cluster that is connected to the Superstream's console
- Read and write permissions to the `superstream.*` topics

## License

This project is licensed under the Apache License 2.0.