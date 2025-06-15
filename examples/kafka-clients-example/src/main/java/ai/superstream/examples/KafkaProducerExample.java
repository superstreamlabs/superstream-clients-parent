package ai.superstream.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Example application that uses the Kafka Clients API to produce messages.
 * Run with:
 * java -javaagent:path/to/superstream-clients-1.0.0.jar
 * -Dlogback.configurationFile=logback.xml -jar
 * kafka-clients-example-1.0.0-jar-with-dependencies.jar
 *
 * Prerequisites:
 * 1. A Kafka server with the following topics:
 * - superstream.metadata_v1 - with a configuration message
 * - superstream.clients - for client reports
 * - example-topic - for test messages
 *
 * Environment variables:
 * - KAFKA_BOOTSTRAP_SERVERS: The Kafka bootstrap servers (default:
 * localhost:9092)
 * - SUPERSTREAM_TOPICS_LIST: Comma-separated list of topics to optimize for
 * (default: example-topic)
 * - BOOTSTRAP_FORMAT: The format to use for bootstrap servers (string, list, arrays_list, or list_of)
 */
public class KafkaProducerExample {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerExample.class);

    // === Configuration Constants ===
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

    private static final String CLIENT_ID = "superstream-example-producer";
    private static final String COMPRESSION_TYPE = "zstd";  // Changed from gzip to snappy for better visibility
    private static final Integer BATCH_SIZE = 1048576; // 1MB batch size

    private static final String TOPIC_NAME = "example-topic";
    private static final String MESSAGE_KEY = "test-key";
    // Create a larger message that will compress well
    private static final String MESSAGE_VALUE = generateLargeCompressibleMessage();

    public static void main(String[] args) {
        // Get bootstrap format from environment variable
        String bootstrapFormat = System.getenv("BOOTSTRAP_FORMAT");
        if (bootstrapFormat == null || bootstrapFormat.isEmpty()) {
            bootstrapFormat = "string"; // Default format
        }

        // Get bootstrap servers from environment variable
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
        }

        Properties props = new Properties();
        props.put("client.id", CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 500); // Force batching by waiting 500ms

        // Set bootstrap servers based on the specified format
        switch (bootstrapFormat.toLowerCase()) {
            case "list":
                List<String> serversList = new ArrayList<>();
                for (String server : bootstrapServers.split(",")) {
                    serversList.add(server.trim());
                }
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serversList);
                logger.info("Using List format for bootstrap servers: {}", serversList);
                break;

            case "arrays_list":
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Arrays.asList(bootstrapServers));
                logger.info("Using Arrays.asList() format for bootstrap servers: {}", Arrays.asList(bootstrapServers));
                break;

            case "list_of":
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, List.of(bootstrapServers));
                logger.info("Using List.of() format for bootstrap servers: {}", List.of(bootstrapServers));
                break;

            case "string":
            default:
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                logger.info("Using String format for bootstrap servers: {}", bootstrapServers);
                break;
        }

        long recordCount = 50; // Number of messages to send
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            while (true) {
                // Send 50 large messages to see compression benefits
                logger.info("Starting to send {} large messages...", recordCount);
                for (int i = 1; i <= recordCount; i++) {
                    String messageKey = MESSAGE_KEY + "-" + i;
                    String messageValue = MESSAGE_VALUE + "-" + i + "-" + System.currentTimeMillis();
                    producer.send(new ProducerRecord<>(TOPIC_NAME, messageKey, messageValue));
                }

                logger.info("All 50 large messages queued successfully! Adding a producer.flush() to send them all at once...");
                producer.flush();
                Thread.sleep(60000);
                logger.info("Waking up and preparing to send the next batch of messages");
//                return;
            }
        } catch (Exception e) {
            logger.error("Error sending message", e);
        }
    }

    private static String generateLargeCompressibleMessage() {
        // Return a 1KB JSON string with repeating data that can be compressed well
        StringBuilder json = new StringBuilder();
        json.append("{\n");
        json.append("  \"metadata\": {\n");
        json.append("    \"id\": \"12345\",\n");
        json.append("    \"type\": \"example\",\n");
        json.append("    \"timestamp\": 1635954438000\n");
        json.append("  },\n");
        json.append("  \"data\": {\n");
        json.append("    \"metrics\": [\n");

        // Add repeating metrics data to reach ~1KB
        for (int i = 0; i < 15; i++) {
            if (i > 0) json.append(",\n");
            json.append("      {\n");
            json.append("        \"name\": \"metric").append(i).append("\",\n");
            json.append("        \"value\": ").append(i * 10).append(",\n");
            json.append("        \"tags\": [\"tag1\", \"tag2\", \"tag3\"],\n");
            json.append("        \"properties\": {\n");
            json.append("          \"property1\": \"value1\",\n");
            json.append("          \"property2\": \"value2\"\n");
            json.append("        }\n");
            json.append("      }");
        }

        json.append("\n    ]\n");
        json.append("  },\n");
        json.append("  \"config\": {\n");
        json.append("    \"sampling\": \"full\",\n");
        json.append("    \"retention\": \"30d\",\n");
        json.append("    \"compression\": true,\n");
        json.append("    \"encryption\": false\n");
        json.append("  }\n");
        json.append("}");

        String result = json.toString();
        logger.debug("Generated compressible message of {} bytes", result.getBytes(StandardCharsets.UTF_8).length);

        return result;
    }
}