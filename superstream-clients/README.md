# Superstream Clients

A Java library for optimizing Kafka producer configurations automatically based on topic-specific recommendations.

## Purpose

Superstream Clients is a non-intrusive library that automatically optimizes Kafka producer configurations in Java applications. It works by intercepting Kafka producer creation and applying optimized configurations based on recommendations from a designated Kafka topic, without requiring any code changes in your application.

## Supported Libraries

Superstream Clients works with any Java library that depends on `kafka-clients`, including:

- Apache Kafka Clients
- Spring Kafka
- Alpakka Kafka (Akka Kafka)
- Kafka Streams
- Kafka Connect
- Any custom wrapper around the Kafka Java client

## Features

- **Zero-code integration**: Works as a Java agent without requiring any code changes
- **Dynamic configuration**: Applies optimized settings based on recommendations from a Kafka topic
- **Impact-based optimization**: Identifies the most impactful topic to optimize based on throughput and potential savings
- **Fallback mechanism**: Gracefully falls back to original settings if optimization fails

## Prerequisites

- Java 11 or higher
- Read and write permissions to the `superstream.*` topics
- Kafka cluster accessible to the application

## Installation

### Maven

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>ai.superstream</groupId>
    <artifactId>superstream-clients</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Gradle

Add the following dependency to your `build.gradle`:

```groovy
implementation 'ai.superstream:superstream-clients:1.0.0'
```

## Usage

To use Superstream Clients, you need to add the Java agent to your application's startup command:

```bash
java -javaagent:/path/to/superstream-clients-1.0.0.jar -jar your-application.jar
```

For Spring Boot applications:

```bash
java -javaagent:/path/to/superstream-clients-1.0.0.jar -jar your-spring-boot-app.jar
```

For containerized applications, add the agent to your Docker entrypoint:

```dockerfile
ENTRYPOINT ["java", "-javaagent:/app/lib/superstream-clients-1.0.0.jar", "-jar", "/app/application.jar"]
```

### Environment Variables

Configure Superstream Clients using the following environment variables:

- `SUPERSTREAM_TOPICS_LIST`: Comma-separated list of topics your application produces to (required)
- `SUPERSTREAM_DISABLED`: Set to "true" to disable optimization (optional)

Example:
```bash
export SUPERSTREAM_TOPICS_LIST=orders,payments,user-events
java -javaagent:/path/to/superstream-clients-1.0.0.jar -jar your-application.jar
```

## Permissions Required

Your application needs the following Kafka permissions:

1. **Read permission** for `superstream.metadata` topic
2. **Write permission** for `superstream.clients` topic

If using ACLs, ensure these permissions are granted to the user/client your application uses to connect to Kafka.

## Examples

The repository includes several examples demonstrating how to use Superstream Clients with different Kafka client libraries.

### Kafka Clients Example

A basic example showing optimization with the standard Kafka client library.

#### Setup in IntelliJ IDEA

1. Open the project in IntelliJ IDEA
2. Navigate to `examples/kafka-clients-example`
3. Create a run configuration:
    - Main class: `ai.superstream.examples.KafkaProducerExample`
    - VM options: `-javaagent:$PROJECT_DIR$/superstream-clients/target/superstream-clients-1.0.0.jar`
    - Environment variables: `SUPERSTREAM_TOPICS_LIST=example-topic`

### Spring Kafka Example

Demonstrates integration with Spring Kafka.

#### Setup in IntelliJ IDEA

1. Open the project in IntelliJ IDEA
2. Navigate to `examples/spring-kafka-example`
3. Create a run configuration:
    - Main class: `ai.superstream.examples.SpringKafkaExampleApplication`
    - VM options: `-javaagent:$PROJECT_DIR$/superstream-clients/target/superstream-clients-1.0.0.jar`
    - Environment variables: `SUPERSTREAM_TOPICS_LIST=example-topic`

### Akka Kafka Example

Shows how to use Superstream Clients with Alpakka Kafka (Akka Kafka).

#### Setup in IntelliJ IDEA

1. Open the project in IntelliJ IDEA
2. Navigate to `examples/akka-kafka-example`
3. Create a run configuration:
    - Main class: `ai.superstream.examples.AkkaKafkaExample`
    - VM options: `-javaagent:$PROJECT_DIR$/superstream-clients/target/superstream-clients-1.0.0.jar`
    - Environment variables: `SUPERSTREAM_TOPICS_LIST=example-topic`

## Building Locally

### Using IntelliJ IDEA

1. Open the project in IntelliJ IDEA
2. Go to View → Tool Windows → Maven
3. Expand `superstream-clients` → Lifecycle
4. Double-click on `install`

### Using Command Line

```bash
mvn clean install
```

## Publishing to Maven Central

To publish the Superstream Clients library to Maven Central:

1. Ensure you have the required credentials for Maven Central (Sonatype OSSRH)
2. Add the following to your `~/.m2/settings.xml`:

```xml
<settings>
  <servers>
    <server>
      <id>ossrh</id>
      <username>your-sonatype-username</username>
      <password>your-sonatype-password</password>
    </server>
  </servers>
</settings>
```

3. Update the POM file with required metadata:

```xml
<project>
  <!-- ... -->
  <name>Superstream Clients</name>
  <description>A Java library for optimizing Kafka producer configurations automatically</description>
  <url>https://github.com/yourusername/superstream-clients</url>
  
  <licenses>
    <license>
      <name>The Apache License, Version 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
    </license>
  </licenses>
  
  <developers>
    <developer>
      <name>Your Name</name>
      <email>your.email@example.com</email>
      <organization>Your Organization</organization>
      <organizationUrl>https://www.example.com</organizationUrl>
    </developer>
  </developers>
  
  <scm>
    <connection>scm:git:git://github.com/yourusername/superstream-clients.git</connection>
    <developerConnection>scm:git:ssh://github.com/yourusername/superstream-clients.git</developerConnection>
    <url>https://github.com/yourusername/superstream-clients/tree/main</url>
  </scm>
  <!-- ... -->
</project>
```

4. Add the deployment and GPG plugins:

```xml
<build>
  <plugins>
    <!-- ... -->
    <plugin>
      <groupId>org.apache.maven.plugins</groupId>
      <artifactId>maven-source-plugin</artifactId>
      <version>3.2.1</version>
      <executions>
        <execution>
          <id>attach-sources</id>
          <goals>
            <goal>jar-no-fork</goal>
          </goals>
        </execution>
      </executions>
    </plugin>
    <plugin>
      <groupId>org.apache.maven.plugins</groupId>
      <artifactId>maven-javadoc-plugin</artifactId>
      <version>3.3.1</version>
      <executions>
        <execution>
          <id>attach-javadocs</id>
          <goals>
            <goal>jar</goal>
          </goals>
        </execution>
      </executions>
    </plugin>
    <plugin>
      <groupId>org.apache.maven.plugins</groupId>
      <artifactId>maven-gpg-plugin</artifactId>
      <version>3.0.1</version>
      <executions>
        <execution>
          <id>sign-artifacts</id>
          <phase>verify</phase>
          <goals>
            <goal>sign</goal>
          </goals>
        </execution>
      </executions>
    </plugin>
    <plugin>
      <groupId>org.sonatype.plugins</groupId>
      <artifactId>nexus-staging-maven-plugin</artifactId>
      <version>1.6.8</version>
      <extensions>true</extensions>
      <configuration>
        <serverId>ossrh</serverId>
        <nexusUrl>https://s01.oss.sonatype.org/</nexusUrl>
        <autoReleaseAfterClose>true</autoReleaseAfterClose>
      </configuration>
    </plugin>
  </plugins>
</build>
```

5. Sign and deploy to Maven Central:

```bash
cd superstream-clients
mvn clean deploy -P release
```

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.