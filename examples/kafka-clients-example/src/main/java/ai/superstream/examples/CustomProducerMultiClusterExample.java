package ai.superstream.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Example application that creates multiple producers using custom classes
 * that extend KafkaProducer to multiple Kafka clusters.
 *
 * This demonstrates Superstream SDK's ability to intercept and optimize
 * custom producer implementations that extend the original KafkaProducer class.
 *
 * IMPORTANT: With the fix in Superstream agent, it should only intercept the
 * actual KafkaProducer constructor, not the custom class constructors.
 *
 * Run with:
 * java -javaagent:path/to/superstream-clients-1.0.0.jar -jar custom-producer-multi-cluster-example.jar
 *
 * Environment variables:
 * - CLUSTER1_BOOTSTRAP_SERVERS: Bootstrap servers for cluster 1 (default: localhost:9092)
 * - CLUSTER2_BOOTSTRAP_SERVERS: Bootstrap servers for cluster 2 (default: localhost:9095)
 * - SUPERSTREAM_TOPICS_LIST: Comma-separated list of topics to optimize
 * - SUPERSTREAM_DISABLED: Set to true to disable Superstream optimization
 * - SUPERSTREAM_LATENCY_SENSITIVE: Set to true to preserve linger.ms values
 * - SUPERSTREAM_DEBUG: Set to true for detailed debug logging
 * - ENABLE_METRICS_LOGGING: Set to true to enable custom metrics logging
 */
public class CustomProducerMultiClusterExample {
    private static final Logger logger = LoggerFactory.getLogger(CustomProducerMultiClusterExample.class);

    // Read cluster configurations from environment variables
    private static final String CLUSTER1_SERVERS = System.getenv("CLUSTER1_BOOTSTRAP_SERVERS") != null ?
            System.getenv("CLUSTER1_BOOTSTRAP_SERVERS") : "localhost:9092";
    private static final String CLUSTER2_SERVERS = System.getenv("CLUSTER2_BOOTSTRAP_SERVERS") != null ?
            System.getenv("CLUSTER2_BOOTSTRAP_SERVERS") : "localhost:9095";

    // Feature flags
    private static final boolean ENABLE_METRICS_LOGGING = "true".equalsIgnoreCase(System.getenv("ENABLE_METRICS_LOGGING"));

    // Topics
    private static final String TOPIC_CLUSTER1 = "custom-producer-topic-1";
    private static final String TOPIC_CLUSTER2 = "custom-producer-topic-2";

    // Configuration
    private static final String COMPRESSION_TYPE = "snappy"; // Different from base example
    private static final Integer BATCH_SIZE = 16384; // 16KB - very small to test optimization

    /**
     * Custom KafkaProducer implementation that adds metrics tracking and custom logic
     */
    public static class MetricsAwareProducer<K, V> extends KafkaProducer<K, V> {
        private final String producerName;
        private final AtomicLong messagesSent = new AtomicLong(0);
        private final AtomicLong bytesSent = new AtomicLong(0);
        private final AtomicLong errorCount = new AtomicLong(0);
        private final Logger producerLogger = LoggerFactory.getLogger(MetricsAwareProducer.class);

        public MetricsAwareProducer(Properties properties, String producerName) {
            super(properties);
            this.producerName = producerName;
            producerLogger.info("Created MetricsAwareProducer: {} with client.id: {}",
                    producerName, properties.getProperty(ProducerConfig.CLIENT_ID_CONFIG));
        }

        public MetricsAwareProducer(Map<String, Object> configs, String producerName) {
            super(configs);
            this.producerName = producerName;
            producerLogger.info("Created MetricsAwareProducer with Map config: {} with client.id: {}",
                    producerName, configs.get(ProducerConfig.CLIENT_ID_CONFIG));
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
            // Wrap the callback to track metrics
            Callback wrappedCallback = new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        messagesSent.incrementAndGet();
                        if (metadata != null && metadata.serializedValueSize() > 0) {
                            bytesSent.addAndGet(metadata.serializedValueSize());
                        }
                    } else {
                        errorCount.incrementAndGet();
                        producerLogger.error("Error sending message in {}: {}", producerName, exception.getMessage());
                    }

                    // Call original callback if provided
                    if (callback != null) {
                        callback.onCompletion(metadata, exception);
                    }
                }
            };

            return super.send(record, wrappedCallback);
        }

        public void logMetrics() {
            if (ENABLE_METRICS_LOGGING) {
                producerLogger.info("Metrics for {}: Messages sent: {}, Bytes sent: {}, Errors: {}",
                        producerName, messagesSent.get(), bytesSent.get(), errorCount.get());
            }
        }

        public String getProducerName() {
            return producerName;
        }
    }

    /**
     * Another custom producer implementation with retry logic
     */
    public static class RetryableProducer<K, V> extends KafkaProducer<K, V> {
        private final int maxRetries;
        private final long retryBackoffMs;
        private final Logger producerLogger = LoggerFactory.getLogger(RetryableProducer.class);

        public RetryableProducer(Properties properties, int maxRetries, long retryBackoffMs) {
            super(properties);
            this.maxRetries = maxRetries;
            this.retryBackoffMs = retryBackoffMs;
            producerLogger.info("Created RetryableProducer with maxRetries: {}, backoff: {}ms",
                    maxRetries, retryBackoffMs);
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
            return sendWithRetry(record, callback, 0);
        }

        private Future<RecordMetadata> sendWithRetry(ProducerRecord<K, V> record, Callback originalCallback, int attemptNumber) {
            return super.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null && attemptNumber < maxRetries) {
                        producerLogger.warn("Send failed, attempt {}/{}. Retrying...",
                                attemptNumber + 1, maxRetries);

                        // Schedule retry in a separate thread to avoid blocking
                        new Thread(() -> {
                            try {
                                Thread.sleep(retryBackoffMs);
                                sendWithRetry(record, originalCallback, attemptNumber + 1);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                if (originalCallback != null) {
                                    originalCallback.onCompletion(null, e);
                                }
                            }
                        }).start();
                    } else {
                        // Final result - either success or all retries exhausted
                        if (originalCallback != null) {
                            originalCallback.onCompletion(metadata, exception);
                        }
                    }
                }
            });
        }
    }

    /**
     * This class name simulates the exact customer class that was problematic.
     * With the Superstream agent fix, this class should NOT be directly intercepted.
     * Only the underlying KafkaProducer constructor should be intercepted.
     */
    public static class PricelineKafkaProducer<K, V> extends KafkaProducer<K, V> {
        private final Logger producerLogger = LoggerFactory.getLogger(PricelineKafkaProducer.class);
        private final String clientName;

        public PricelineKafkaProducer(Properties properties, String clientName) {
            super(properties);
            this.clientName = clientName;
            producerLogger.info("Created PricelineKafkaProducer for client: {}", clientName);
        }

        public PricelineKafkaProducer(Map<String, Object> configs, String clientName) {
            super(configs);
            this.clientName = clientName;
            producerLogger.info("Created PricelineKafkaProducer with Map configs for client: {}", clientName);
        }

        @Override
        public void flush() {
            producerLogger.info("PricelineKafkaProducer flushing for client: {}", clientName);
            super.flush();
        }

        @Override
        public void close() {
            producerLogger.info("PricelineKafkaProducer closing for client: {}", clientName);
            super.close();
        }
    }

    public static void main(String[] args) {
        logger.info("Starting CustomProducerMultiClusterExample");
        logger.info("Environment variables:");
        logger.info("  CLUSTER1_BOOTSTRAP_SERVERS: {}", CLUSTER1_SERVERS);
        logger.info("  CLUSTER2_BOOTSTRAP_SERVERS: {}", CLUSTER2_SERVERS);
        logger.info("  SUPERSTREAM_TOPICS_LIST: {}", System.getenv("SUPERSTREAM_TOPICS_LIST"));
        logger.info("  SUPERSTREAM_DISABLED: {}", System.getenv("SUPERSTREAM_DISABLED"));
        logger.info("  SUPERSTREAM_LATENCY_SENSITIVE: {}", System.getenv("SUPERSTREAM_LATENCY_SENSITIVE"));
        logger.info("  SUPERSTREAM_DEBUG: {}", System.getenv("SUPERSTREAM_DEBUG"));
        logger.info("  ENABLE_METRICS_LOGGING: {}", ENABLE_METRICS_LOGGING);

        List<Producer<String, String>> producers = new ArrayList<>();

        try {
            // Create custom producers for Cluster 1
            MetricsAwareProducer<String, String> metricsProducer1 = createMetricsAwareProducer(
                    "metrics-producer-cluster1", CLUSTER1_SERVERS);

            PricelineKafkaProducer<String, String> pricelineProducer = createPricelineProducer(
                    "priceline-producer-cluster1", CLUSTER1_SERVERS);

            RetryableProducer<String, String> retryableProducer1 = createRetryableProducer(
                    "retryable-producer-cluster1", CLUSTER1_SERVERS, 3, 1000);

            // Create custom producers for Cluster 2
            MetricsAwareProducer<String, String> metricsProducer2 = createMetricsAwareProducerWithMap(
                    "metrics-producer-cluster2", CLUSTER2_SERVERS);

            producers.add(metricsProducer1);
            producers.add(pricelineProducer);
            producers.add(retryableProducer1);
            producers.add(metricsProducer2);

            logger.info("Created {} custom producers across {} clusters", producers.size(), 2);
            logger.info("NOTE: PricelineKafkaProducer should NOT be directly intercepted by Superstream");

            // Send messages and track metrics
            long startTime = System.currentTimeMillis();
            int iterations = 0;

            while (iterations < 10) { // Run for 10 iterations
                // Send to cluster 1 with different custom producers
                sendBatch(metricsProducer1, TOPIC_CLUSTER1, "metrics-cluster1", 50);
                sendBatch(pricelineProducer, TOPIC_CLUSTER1, "priceline-cluster1", 30);
                sendBatch(retryableProducer1, TOPIC_CLUSTER1, "retryable-cluster1", 30);

                // Send to cluster 2 with MetricsAwareProducer
                sendBatch(metricsProducer2, TOPIC_CLUSTER2, "metrics-cluster2", 40);

                // Log metrics periodically
                metricsProducer1.logMetrics();
                metricsProducer2.logMetrics();

                Thread.sleep(30000); // Wait 30 seconds between iterations
                iterations++;

                logger.info("Completed iteration {}/10", iterations);
            }

            long endTime = System.currentTimeMillis();
            logger.info("Test completed in {} seconds", (endTime - startTime) / 1000);

        } catch (Exception e) {
            logger.error("Error in main execution", e);
        } finally {
            // Close all producers
            for (Producer<String, String> producer : producers) {
                try {
                    producer.close();
                    logger.info("Closed producer: {}", producer.getClass().getSimpleName());
                } catch (Exception e) {
                    logger.error("Error closing producer", e);
                }
            }
        }
    }

    private static MetricsAwareProducer<String, String> createMetricsAwareProducer(
            String clientId, String bootstrapServers) {
        Properties props = createBaseProperties(clientId, bootstrapServers);
        return new MetricsAwareProducer<>(props, clientId);
    }

    private static MetricsAwareProducer<String, String> createMetricsAwareProducerWithMap(
            String clientId, String bootstrapServers) {
        Map<String, Object> configs = createBaseMap(clientId, bootstrapServers);
        return new MetricsAwareProducer<>(configs, clientId);
    }

    private static PricelineKafkaProducer<String, String> createPricelineProducer(
            String clientId, String bootstrapServers) {
        // Use Map configuration for PricelineKafkaProducer to test Map constructor
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE);
        configs.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE);
        configs.put(ProducerConfig.LINGER_MS_CONFIG, 300); // Different linger setting
        configs.put(ProducerConfig.ACKS_CONFIG, "all");
        configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        return new PricelineKafkaProducer<>(configs, clientId);
    }

    private static RetryableProducer<String, String> createRetryableProducer(
            String clientId, String bootstrapServers, int maxRetries, long retryBackoffMs) {
        Properties props = createBaseProperties(clientId, bootstrapServers);
        // Set lower retries in Kafka config since we handle retries in custom logic
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        return new RetryableProducer<>(props, maxRetries, retryBackoffMs);
    }

    private static Properties createBaseProperties(String clientId, String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 200); // Different from base example
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        return props;
    }

    private static Map<String, Object> createBaseMap(String clientId, String bootstrapServers) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE);
        configs.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE);
        configs.put(ProducerConfig.LINGER_MS_CONFIG, 200);
        configs.put(ProducerConfig.ACKS_CONFIG, "all");
        configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        return configs;
    }

    private static void sendBatch(Producer<String, String> producer, String topic,
                                  String keyPrefix, int messageCount) {
        for (int i = 0; i < messageCount; i++) {
            String key = keyPrefix + "-" + System.currentTimeMillis() + "-" + i;
            String value = generateMessage(keyPrefix);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record);
        }

        producer.flush();
        logger.debug("Sent {} messages to topic {} with key prefix {}",
                messageCount, topic, keyPrefix);
    }

    private static String generateMessage(String source) {
        StringBuilder json = new StringBuilder();
        json.append("{\n");
        json.append("  \"source\": \"").append(source).append("\",\n");
        json.append("  \"timestamp\": ").append(System.currentTimeMillis()).append(",\n");
        json.append("  \"data\": {\n");
        json.append("    \"payload\": \"");

        // Generate repeating pattern for compression
        for (int i = 0; i < 50; i++) {
            json.append("AAABBBCCCDDDEEEFFFGGGHHHIIIJJJKKKLLLMMMNNNOOOPPPQQQRRRSSSTTTUUUVVVWWWXXXYYYZZZ");
        }

        json.append("\",\n");
        json.append("    \"checksum\": \"").append(source.hashCode()).append("\"\n");
        json.append("  }\n");
        json.append("}");

        return json.toString();
    }
}