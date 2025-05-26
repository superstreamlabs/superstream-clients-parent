package ai.superstream.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class KafkaProducerExample {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerExample.class);

    // === Configuration Constants ===
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

    private static final String CLIENT_ID = "superstream-example-producer";
    private static final String COMPRESSION_TYPE = "none"; // Changed from gzip to snappy for better visibility
    private static final Integer BATCH_SIZE = 10; // 1MB batch size

    private static final String TOPIC_NAME = "example-topic";
    private static final String MESSAGE_KEY = "test-key";
    // Create a larger message that will compress well
    private static final String MESSAGE_VALUE = generateLargeCompressibleMessage();

    public static void main(String[] args) {
        // Build the configuration map first using a mutable map
        Map<String, Object> mutableProps = new java.util.HashMap<>();
        mutableProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS);
        mutableProps.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        mutableProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        mutableProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        mutableProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE);
        mutableProps.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE);
        mutableProps.put(ProducerConfig.LINGER_MS_CONFIG, 500);

        // Wrap the map to make it immutable – simulates a user supplying an unmodifiable configuration object
        // Map<String, Object> props = java.util.Collections.unmodifiableMap(mutableProps);

        // Pass the immutable map directly to the KafkaProducer constructor
        Producer<String, String> producer = new KafkaProducer<String, String>(mutableProps);

        long recordCount = 50; // Number of messages to send
        try {
            while (true) {
                // Send 50 large messages to see compression benefits
                for (int i = 1; i <= recordCount; i++) {
                    String messageKey = MESSAGE_KEY + "-" + i;
                    String messageValue = MESSAGE_VALUE + "-" + i + "-" + System.currentTimeMillis();
                    producer.send(new ProducerRecord<>(TOPIC_NAME, messageKey, messageValue));
                }

                producer.flush();
                Thread.sleep(10000);
            }
        } catch (Exception e) {
            logger.error("Error sending message", e);
        } finally {
            producer.close();
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
            if (i > 0)
                json.append(",\n");
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