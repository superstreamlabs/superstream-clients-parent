# Superstream Clients

This repository contains the Superstream Clients library and example applications.

For detailed information about the library, see the [Superstream Clients Documentation](https://github.com/superstreamlabs/superstream-clients-parent/README.md).

## Code Examples

This repository includes examples showing how to use Superstream Clients with different Kafka libraries.

### Running Examples

#### Kafka Clients Example

```bash
cd examples/kafka-clients-example
mvn clean package
export SUPERSTREAM_TOPICS_LIST=example-topic
java -javaagent:../../target/superstream-clients-1.0.0.jar -jar target/kafka-clients-example-1.0.0-jar-with-dependencies.jar
```

#### Spring Kafka Example

```bash
cd examples/spring-kafka-example
mvn clean package
export SUPERSTREAM_TOPICS_LIST=example-topic
java -javaagent:../../target/superstream-clients-1.0.0.jar -jar target/spring-kafka-example-1.0.0.jar
```

#### Akka Kafka Example

```bash
cd examples/akka-kafka-example
mvn clean package
export SUPERSTREAM_TOPICS_LIST=example-topic
java -javaagent:../../target/superstream-clients-1.0.0.jar -jar target/akka-kafka-example-1.0.0-jar-with-dependencies.jar
```

## Building the Clients Library

### Command Line

```bash
mvn clean package -pl superstream-clients
```

### IntelliJ IDEA

1. Go to View → Tool Windows → Maven
2. Expand `superstream-clients` → Lifecycle
3. Double-click on `install`


## License

Apache License 2.0