<div align="center">

<img src="https://github.com/user-attachments/assets/35899c78-24eb-4507-97ed-e87e84c49fea#gh-dark-mode-only" width="300">
<img src="https://github.com/user-attachments/assets/8a7bca49-c362-4a8c-945e-a331fb26d8eb#gh-light-mode-only" width="300">

</div>

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

The linger.ms parameter follows these rules:

1. If SUPERSTREAM_LATENCY_SENSITIVE is set to true:
   - Linger value will never be modified, regardless of other settings


2. If SUPERSTREAM_LATENCY_SENSITIVE is set to false or not set:
   - If no explicit linger exists in original configuration: Use Superstream's optimized value
   - If explicit linger exists: Use the maximum of original value and Superstream's optimized value

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

### Docker Integration

When using Superstream Clients with containerized applications, include the agent in your Dockerfile:

```dockerfile
FROM openjdk:11-jre

# Copy your application
COPY target/your-application.jar /app/your-application.jar

# Copy the Superstream agent
COPY path/to/superstream-clients-1.0.0.jar /app/lib/superstream-clients-1.0.0.jar

# Set environment variables
ENV SUPERSTREAM_TOPICS_LIST=your-topics

# Run with the Java agent
ENTRYPOINT ["java", "-javaagent:/app/lib/superstream-clients-1.0.0.jar", "-jar", "/app/your-application.jar"]
```

Alternatively, you can use a multi-stage build to download the agent from Maven Central:

```dockerfile
# Build stage
FROM maven:3.8-openjdk-11 AS build

# Get the Superstream agent
RUN mvn org.apache.maven.plugins:maven-dependency-plugin:3.2.0:get \
-DgroupId=ai.superstream \
-DartifactId=superstream-clients \
-Dversion=1.0.0

RUN mvn org.apache.maven.plugins:maven-dependency-plugin:3.2.0:copy \
-Dartifact=ai.superstream:superstream-clients:1.0.0 \
-DoutputDirectory=/tmp

# Final stage
FROM openjdk:11-jre

# Copy your application
COPY target/your-application.jar /app/your-application.jar

# Copy the agent from the build stage
COPY --from=build /tmp/superstream-clients-1.0.0.jar /app/lib/superstream-clients-1.0.0.jar
```

### Required Environment Variables

- `SUPERSTREAM_TOPICS_LIST`: Comma-separated list of topics your application produces to

### Optional Environment Variables

- `SUPERSTREAM_LATENCY_SENSITIVE`: Set to "true" to prevent any modification to linger.ms values
- `SUPERSTREAM_DISABLED`: Set to "true" to disable optimization

Example:
```bash
export SUPERSTREAM_TOPICS_LIST=orders,payments,user-events
export SUPERSTREAM_LATENCY_SENSITIVE=true
```

## Prerequisites

- Java 11 or higher
- Kafka cluster that is connected to the Superstream's console
- Read and write permissions to the `superstream.*` topics

## License

This project is licensed under the Apache License 2.0.
