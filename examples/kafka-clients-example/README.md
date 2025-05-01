# Universal Kafka Producer Example

This example demonstrates how to use a single Kafka producer application that can connect to any Kafka vendor (standalone, AWS MSK, Confluent Cloud, Aiven, etc.) by using configuration from an external file.

## Prerequisites
1. Kafka cluster to connect to
2. Superstream agent JAR

## How to Use

1. Create a configuration file at `/tmp/cluster.conf.env` based on the example templates in `../cluster-configs/`
2. Customize the configuration file with your Kafka cluster details
3. Run the application:

```
java -javaagent:path/to/superstream-clients-1.0.0.jar \
     -Dlogback.configurationFile=logback.xml \
     -jar kafka-clients-example-1.0.0-jar-with-dependencies.jar
```

## Configuration Files

Example configuration files are provided for different Kafka vendors:

- `standalone.conf.env`: For local/standalone Kafka
- `aws-msk.conf.env`: For Amazon MSK
- `confluent.conf.env`: For Confluent Cloud
- `aiven.conf.env`: For Aiven Kafka

Copy one of these examples to `/tmp/cluster.conf.env` and modify as needed.

## Configuration Parameters

### Basic Settings
- `BOOTSTRAP_SERVERS`: Comma-separated list of Kafka broker addresses
- `CLIENT_ID`: Client identifier for the producer

### Message Settings
- `TOPIC_NAME`: Name of the topic to send messages to (default: "example-topic")
- `MESSAGE_KEY`: Key for the test message (default: "test-key")
- `MESSAGE_VALUE`: Value for the test message (default: "Hello, Superstream!")

### Producer Settings
- `COMPRESSION_TYPE`: Compression type (gzip, snappy, lz4, zstd)
- `BATCH_SIZE`: Producer batch size in bytes

### Security Settings
For secure clusters:
- `SECURITY_PROTOCOL`: Protocol used (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)

For SASL authentication:
- `SASL_MECHANISM`: SASL mechanism (PLAIN, SCRAM-SHA-256, AWS_MSK_IAM)
- `SASL_USERNAME`: SASL username (for PLAIN, SCRAM)
- `SASL_PASSWORD`: SASL password (for PLAIN, SCRAM)
- `AWS_ACCESS_KEY_ID`: AWS access key (for AWS_MSK_IAM)
- `AWS_SECRET_ACCESS_KEY`: AWS secret key (for AWS_MSK_IAM)

For SSL/TLS:
- `SSL_TRUSTSTORE_LOCATION`: Path to the truststore file
- `SSL_TRUSTSTORE_PASSWORD`: Password for the truststore
- `SSL_TRUSTSTORE_TYPE`: Truststore type (JKS, PKCS12)
- `SSL_KEYSTORE_LOCATION`: Path to the keystore file
- `SSL_KEYSTORE_PASSWORD`: Password for the keystore
- `SSL_KEYSTORE_TYPE`: Keystore type (JKS, PKCS12)

### Custom Producer Settings
Any additional producer configuration can be added using the `PRODUCER_` prefix:
- `PRODUCER_ACKS`: Becomes `acks` in the producer config
- `PRODUCER_LINGER_MS`: Becomes `linger.ms` in the producer config

Example: `PRODUCER_ACKS=all` sets `acks=all` in the producer configuration 