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
 * Example application that uses the Kafka Clients API to produce messages.
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
 * - KAFKA_BOOTSTRAP_SERVERS: The Kafka bootstrap servers (default: localhost:9092)
 * - SUPERSTREAM_TOPICS_LIST: Comma-separated list of topics to optimize for (default: example-topic)
 */
public class KafkaProducerExample {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerExample.class);

    // === Configuration Constants ===
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

    private static final String CLIENT_ID = "superstream-example-producer";
    private static final String COMPRESSION_TYPE = "gzip";
    private static final String BATCH_SIZE = "16384";

    private static final String TOPIC_NAME = "example-topic";
    private static final String MESSAGE_KEY = "test-key";
    private static final String MESSAGE_VALUE = "Hello, Superstream!";

    public static void main(String[] args) {
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