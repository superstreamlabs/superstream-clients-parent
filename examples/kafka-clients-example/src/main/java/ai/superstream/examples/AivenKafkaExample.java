package ai.superstream.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Example application that uses the Kafka Clients API to produce messages securely over SSL (TLS).
 *
 * Run with:
 * java -javaagent:path/to/superstream-clients-1.0.0.jar -Dlogback.configurationFile=logback.xml -jar kafka-clients-example-1.0.0-jar-with-dependencies.jar
 *
 * Prerequisites:
 * 1. A Kafka server with the following topics:
 *    - superstream.metadata_v1 - with a configuration message
 *    - superstream.clients - for client reports
 *    - example-topic - for test messages
 *
 * Environment variables:
 * - KAFKA_BOOTSTRAP_SERVERS: The Kafka bootstrap servers (default: superstream-test-superstream-3591.k.aivencloud.com:18837)
 * - SUPERSTREAM_TOPICS_LIST: Comma-separated list of topics to optimize for (default: example-topic)
 *
 * SSL Requirements:
 * To establish a secure SSL/TLS connection with Kafka brokers, you must generate and configure:
 *
 *   - CA certificate (`ca.pem`) — the trusted certificate authority (from the Kafka provider)
 *   - Client certificate (`client.cert.pem`) — identifies the client application
 *   - Client private key (`client.pk8.pem`) — the private key paired with the client certificate
 *
 * Steps to prepare truststore and keystore:
 *
 * Step 1: Create a Truststore (truststore.jks) containing the CA certificate:
 *     keytool -importcert \
 *         -trustcacerts \
 *         -alias aiven-ca \
 *         -file /path/to/ca.pem \
 *         -keystore /path/to/truststore.jks \
 *         -storepass changeit
 *
 * Step 2: Create a Keystore (keystore.p12) containing the client certificate and private key:
 *     openssl pkcs12 -export \
 *         -in /path/to/client.cert.pem \
 *         -inkey /path/to/client.pk8.pem \
 *         -out /path/to/keystore.p12 \
 *         -name kafka-client \
 *         -passout pass:changeit
 *
 * Notes:
 * - The Truststore (`truststore.jks`) ensures the client trusts the Kafka broker's SSL certificate.
 * - The Keystore (`keystore.p12`) provides client authentication (mutual TLS) toward the broker.
 * - Both Truststore and Keystore must be correctly configured for SSL handshake to succeed.
 *
 * Security Advice:
 * - The password must be at least 6 characters long and must match the password configured in your Java Kafka client.
 * - You must configure these passwords properly in your Java Kafka client (`ssl.truststore.password`, `ssl.keystore.password`).
 * - Use strong passwords instead of "changeit" in production environments.
 * - Protect your truststore.jks and keystore.p12 files carefully; leaking them would compromise your SSL security.
 */

public class AivenKafkaExample {
    private static final Logger logger = LoggerFactory.getLogger(AivenKafkaExample.class);

    // === Configuration Constants ===
    private static final String DEFAULT_BOOTSTRAP_SERVERS =
            "superstream-test-superstream-3591.k.aivencloud.com:18837";
    // Replace with full absolute path to your generated truststore.jks
    private static final String TRUSTSTORE_LOCATION = "/absolute/path/to/truststore.jks";
    // The password must be at least 6 characters long and must match the password configured in your Java Kafka client.
    private static final String TRUSTSTORE_PASSWORD = "changeit";
    // Replace with full absolute path to your generated keystore.p12
    private static final String KEYSTORE_KEY_PATH = "/absolute/path/to/keystore.p12";
    private static final String SECURITY_PROTOCOL = "SSL";
    private static final String TRUSTSTORE_TYPE = "JKS";
    private static  final String KEYSTORE_TYPE = "PKCS12";

    private static final String CLIENT_ID = "superstream-example-producer";
    private static final String COMPRESSION_TYPE = "gzip";
    private static final Integer BATCH_SIZE = 16384;

    private static final String TOPIC_NAME = "example-topic";
    private static final String MESSAGE_KEY = "test-key";
    private static final String MESSAGE_VALUE = "Hello, Superstream!";

    public static void main(String[] args) {
        // Get bootstrap servers from environment variable or use default
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
        }

        // Configure the producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put("client.id", CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put("security.protocol", SECURITY_PROTOCOL);
        props.put("ssl.truststore.location", TRUSTSTORE_LOCATION);
        props.put("ssl.truststore.password", TRUSTSTORE_PASSWORD);
        props.put("ssl.truststore.type", TRUSTSTORE_TYPE);
        props.put("ssl.keystore.location",KEYSTORE_KEY_PATH );
        props.put("ssl.keystore.password", TRUSTSTORE_PASSWORD);
        props.put("ssl.keystore.type", KEYSTORE_TYPE);
        // Set some basic configuration
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE);
        logger.info("Creating producer with bootstrap servers: {}", bootstrapServers);
        logger.info("Original producer configuration:");
        props.forEach((k, v) -> logger.info("  {} = {}", k, v));

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            // The Superstream Agent should have intercepted the producer creation
            // and potentially optimized the configuration

            // Log the actual configuration used by the producer
            logger.info("Actual producer configuration (after potential Superstream optimization):");

            // Get the actual configuration from the producer via reflection
            java.lang.reflect.Field configField = producer.getClass().getDeclaredField("producerConfig");
            configField.setAccessible(true);
            org.apache.kafka.clients.producer.ProducerConfig actualConfig =
                    (org.apache.kafka.clients.producer.ProducerConfig) configField.get(producer);

            logger.info("  compression.type = {}", actualConfig.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG));
            logger.info("  batch.size = {}", actualConfig.getInt(ProducerConfig.BATCH_SIZE_CONFIG));

            // Send a test message
            logger.info("Sending message to topic {}: key={}, value={}", TOPIC_NAME, MESSAGE_KEY, MESSAGE_VALUE);
            producer.send(new ProducerRecord<>(TOPIC_NAME, MESSAGE_KEY, MESSAGE_VALUE)).get();
            logger.info("Message sent successfully!");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while sending message", e);
        } catch (ExecutionException e) {
            logger.error("Error sending message", e);
        } catch (Exception e) {
            logger.error("Unexpected error", e);
        }
    }
}
