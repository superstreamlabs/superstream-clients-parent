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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Example application that creates multiple producers to multiple Kafka clusters using Map/HashMap.
 * This demonstrates Superstream SDK's ability to handle producers configured with Maps.
 *
 * Run with:
 * java -javaagent:path/to/superstream-clients-1.0.0.jar -jar multi-producer-with-map-example.jar
 *
 * Environment variables:
 * - CLUSTER1_BOOTSTRAP_SERVERS: Bootstrap servers for cluster 1 (default: localhost:9092)
 * - CLUSTER2_BOOTSTRAP_SERVERS: Bootstrap servers for cluster 2 (default: localhost:9095)
 * - CLUSTER3_BOOTSTRAP_SERVERS: Bootstrap servers for cluster 3 (default: localhost:9096)
 * - SUPERSTREAM_TOPICS_LIST: Comma-separated list of topics to optimize
 * - SUPERSTREAM_DISABLED: Set to true to disable Superstream optimization
 * - SUPERSTREAM_LATENCY_SENSITIVE: Set to true to preserve linger.ms values
 * - SUPERSTREAM_DEBUG: Set to true for detailed debug logging
 */
public class MultiProducerWithMapExample {
    private static final Logger logger = LoggerFactory.getLogger(MultiProducerWithMapExample.class);

    // Read cluster configurations from environment variables
    private static final String CLUSTER1_SERVERS = System.getenv("CLUSTER1_BOOTSTRAP_SERVERS") != null ?
            System.getenv("CLUSTER1_BOOTSTRAP_SERVERS") : "localhost:9092";
    private static final String CLUSTER2_SERVERS = System.getenv("CLUSTER2_BOOTSTRAP_SERVERS") != null ?
            System.getenv("CLUSTER2_BOOTSTRAP_SERVERS") : "localhost:9095";
    private static final String CLUSTER3_SERVERS = System.getenv("CLUSTER3_BOOTSTRAP_SERVERS") != null ?
            System.getenv("CLUSTER3_BOOTSTRAP_SERVERS") : "localhost:9096";

    // Topics for different clusters
    private static final String TOPIC_CLUSTER1_A = "cluster1-topic-a";
    private static final String TOPIC_CLUSTER1_B = "cluster1-topic-b";
    private static final String TOPIC_CLUSTER2 = "cluster2-topic";
    private static final String TOPIC_CLUSTER3 = "cluster3-topic";

    // Producer configurations
    private static final String COMPRESSION_TYPE = "lz4"; // Will be overridden by Superstream if configured
    private static final Integer BATCH_SIZE = 32768; // 32KB - smaller than recommendation to test override
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

    private static final String MESSAGE_VALUE = generateLargeCompressibleMessage();

    public static void main(String[] args) {
        logger.info("Starting MultiProducerWithMapExample");
        logger.info("Environment variables:");
        logger.info("  CLUSTER1_BOOTSTRAP_SERVERS: {}", CLUSTER1_SERVERS);
        logger.info("  CLUSTER2_BOOTSTRAP_SERVERS: {}", CLUSTER2_SERVERS);
        logger.info("  CLUSTER3_BOOTSTRAP_SERVERS: {}", CLUSTER3_SERVERS);
        logger.info("  SUPERSTREAM_TOPICS_LIST: {}", System.getenv("SUPERSTREAM_TOPICS_LIST"));
        logger.info("  SUPERSTREAM_DISABLED: {}", System.getenv("SUPERSTREAM_DISABLED"));
        logger.info("  SUPERSTREAM_LATENCY_SENSITIVE: {}", System.getenv("SUPERSTREAM_LATENCY_SENSITIVE"));
        logger.info("  SUPERSTREAM_DEBUG: {}", System.getenv("SUPERSTREAM_DEBUG"));

        List<Producer<String, String>> producers = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(4);

        try {
            // Create producers for Cluster 1 with different bootstrap server formats
            Producer<String, String> producer1A = createProducerWithMap("producer-cluster1-a", CLUSTER1_SERVERS);
            Producer<String, String> producer1B = createProducerWithHashMap("producer-cluster1-b", CLUSTER1_SERVERS);

            // Create producer for Cluster 2 with Arrays.asList() bootstrap format in Map
            Producer<String, String> producer2 = createProducerWithArraysList("producer-cluster2", CLUSTER2_SERVERS);

            // Create producer for Cluster 3 with List.of() bootstrap format in Map
            Producer<String, String> producer3 = createProducerWithListOf("producer-cluster3", CLUSTER3_SERVERS);

            producers.add(producer1A);
            producers.add(producer1B);
            producers.add(producer2);
            producers.add(producer3);

            logger.info("Created {} producers across {} clusters", producers.size(), 3);

            // Send messages concurrently to all clusters
            executorService.submit(() -> sendMessages(producer1A, TOPIC_CLUSTER1_A, "cluster1-producerA"));
            executorService.submit(() -> sendMessages(producer1B, TOPIC_CLUSTER1_B, "cluster1-producerB"));
            executorService.submit(() -> sendMessages(producer2, TOPIC_CLUSTER2, "cluster2-producer"));
            executorService.submit(() -> sendMessages(producer3, TOPIC_CLUSTER3, "cluster3-producer"));

            // Keep running for demonstration
            Thread.sleep(300000); // Run for 5 minutes

        } catch (Exception e) {
            logger.error("Error in main execution", e);
        } finally {
            // Cleanup
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            }

            // Close all producers
            for (Producer<String, String> producer : producers) {
                try {
                    producer.close();
                    logger.info("Closed producer");
                } catch (Exception e) {
                    logger.error("Error closing producer", e);
                }
            }
        }
    }

    private static Producer<String, String> createProducerWithMap(String clientId, String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        logger.info("Creating producer with clientId: {} for cluster: {} using Map with String bootstrap format",
                clientId, bootstrapServers);
        return new KafkaProducer<>(props);
    }

    private static Producer<String, String> createProducerWithHashMap(String clientId, String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Create a List of servers
        List<String> servers = new ArrayList<>();
        for (String server : bootstrapServers.split(",")) {
            servers.add(server.trim());
        }
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);

        logger.info("Creating producer with clientId: {} for cluster: {} using HashMap with List bootstrap format",
                clientId, bootstrapServers);
        return new KafkaProducer<>(props);
    }

    private static Producer<String, String> createProducerWithArraysList(String clientId, String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Using Arrays.asList() format
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Arrays.asList(bootstrapServers));

        logger.info("Creating producer with clientId: {} for cluster: {} using Map with Arrays.asList() bootstrap format",
                clientId, bootstrapServers);
        return new KafkaProducer<>(props);
    }

    private static Producer<String, String> createProducerWithListOf(String clientId, String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Using List.of() format (Java 9+)
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, List.of(bootstrapServers));

        logger.info("Creating producer with clientId: {} for cluster: {} using Map with List.of() bootstrap format",
                clientId, bootstrapServers);
        return new KafkaProducer<>(props);
    }

    private static void sendMessages(Producer<String, String> producer, String topic, String producerName) {
        int messageCount = 0;
        try {
            while (!Thread.currentThread().isInterrupted()) {
                for (int i = 0; i < 50; i++) {
                    String key = producerName + "-key-" + messageCount;
                    String value = MESSAGE_VALUE + "-" + producerName + "-" + System.currentTimeMillis();

                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                    producer.send(record);
                    messageCount++;
                }

                producer.flush();
                logger.info("{} sent {} messages to topic {}", producerName, messageCount, topic);

                Thread.sleep(30000); // Send batch every 30 seconds
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("{} interrupted", producerName);
        } catch (Exception e) {
            logger.error("Error sending messages from {}", producerName, e);
        }
    }

    private static String generateLargeCompressibleMessage() {
        StringBuilder json = new StringBuilder();
        json.append("{\n");
        json.append("  \"metadata\": {\n");
        json.append("    \"id\": \"12345\",\n");
        json.append("    \"type\": \"multi-cluster-example\",\n");
        json.append("    \"timestamp\": ").append(System.currentTimeMillis()).append("\n");
        json.append("  },\n");
        json.append("  \"data\": {\n");
        json.append("    \"metrics\": [\n");

        // Generate ~3KB of compressible data
        for (int i = 0; i < 15; i++) {
            if (i > 0) json.append(",\n");
            json.append("      {\n");
            json.append("        \"name\": \"metric").append(i).append("\",\n");
            json.append("        \"value\": ").append(i * 10).append(",\n");
            json.append("        \"tags\": [\"tag1\", \"tag2\", \"tag3\"],\n");
            json.append("        \"properties\": {\n");
            json.append("          \"property1\": \"value1\",\n");
            json.append("          \"property2\": \"value2\",\n");
            json.append("          \"description\": \"This is a sample metric for testing Superstream optimization\"\n");
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