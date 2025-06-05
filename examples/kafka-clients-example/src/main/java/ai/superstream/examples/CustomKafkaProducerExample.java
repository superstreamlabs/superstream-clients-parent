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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Example application that creates multiple producers using custom classes
 * that extend the original KafkaProducer class to multiple clusters.
 *
 * This demonstrates Superstream SDK's ability to intercept and optimize custom
 * producer implementations that extend the original KafkaProducer class, including a custom
 * class called PricelineKafkaProducer (to match the exact customer scenario mentioned).
 *
 * Run with:
 * java -javaagent:path/to/superstream-clients-1.0.0.jar -jar custom-kafka-producer-example.jar
 *
 * Environment variables:
 * - CLUSTER1_BOOTSTRAP_SERVERS: Bootstrap servers for cluster 1 (default: localhost:9092)
 * - CLUSTER2_BOOTSTRAP_SERVERS: Bootstrap servers for cluster 2 (default: localhost:9095)
 * - SUPERSTREAM_TOPICS_LIST: Comma-separated list of topics to optimize
 * - SUPERSTREAM_DISABLED: Set to true to disable Superstream optimization
 * - SUPERSTREAM_LATENCY_SENSITIVE: Set to true to preserve linger.ms values
 * - SUPERSTREAM_DEBUG: Set to true for detailed debug logging
 */
public class CustomKafkaProducerExample {
    private static final Logger logger = LoggerFactory.getLogger(CustomKafkaProducerExample.class);

    // Read cluster configurations from environment variables
    private static final String CLUSTER1_SERVERS = System.getenv("CLUSTER1_BOOTSTRAP_SERVERS") != null ?
            System.getenv("CLUSTER1_BOOTSTRAP_SERVERS") : "localhost:9092";
    private static final String CLUSTER2_SERVERS = System.getenv("CLUSTER2_BOOTSTRAP_SERVERS") != null ?
            System.getenv("CLUSTER2_BOOTSTRAP_SERVERS") : "localhost:9095";

    // Topics
    private static final String TOPIC_CLUSTER1 = "custom-producer-topic-1";
    private static final String TOPIC_CLUSTER2 = "custom-producer-topic-2";

    // Configuration
    private static final String COMPRESSION_TYPE = "snappy";
    private static final Integer BATCH_SIZE = 16384; // 16KB - very small to test optimization

    /**
     * A custom KafkaProducer that adds metrics tracking capabilities.
     * This simulates a customer's custom producer implementation that extends the
     * original KafkaProducer class.
     */
    public static class MetricsKafkaProducer<K, V> extends KafkaProducer<K, V> {
        private final String producerName;
        private final AtomicLong messagesSent = new AtomicLong(0);
        private final AtomicLong bytesSent = new AtomicLong(0);
        private final AtomicLong errorCount = new AtomicLong(0);
        private final Logger producerLogger = LoggerFactory.getLogger(MetricsKafkaProducer.class);

        public MetricsKafkaProducer(Properties properties, String producerName) {
            super(properties);
            this.producerName = producerName;
            producerLogger.info("Created MetricsKafkaProducer: {} with client.id: {}",
                    producerName, properties.getProperty(ProducerConfig.CLIENT_ID_CONFIG));
        }

        public MetricsKafkaProducer(Map<String, Object> configs, String producerName) {
            super(configs);
            this.producerName = producerName;
            producerLogger.info("Created MetricsKafkaProducer with Map config: {} with client.id: {}",
                    producerName, configs.get(ProducerConfig.CLIENT_ID_CONFIG));
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
            // Wrap the callback to track metrics
            Callback wrappedCallback = (metadata, exception) -> {
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
            };

            return super.send(record, wrappedCallback);
        }

        public void logMetrics() {
            producerLogger.info("Metrics for {}: Messages sent: {}, Bytes sent: {}, Errors: {}",
                    producerName, messagesSent.get(), bytesSent.get(), errorCount.get());
        }

        public String getProducerName() {
            return producerName;
        }
    }

    /**
     * This class name exactly matches the customer's class name that was problematic.
     * It should now work correctly with the fix in the Superstream agent.
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

    /**
     * Another custom producer example with retry logic
     */
    public static class RetryKafkaProducer<K, V> extends KafkaProducer<K, V> {
        private final int maxRetries;
        private final long retryBackoffMs;
        private final Logger producerLogger = LoggerFactory.getLogger(RetryKafkaProducer.class);

        public RetryKafkaProducer(Properties properties, int maxRetries, long retryBackoffMs) {
            super(properties);
            this.maxRetries = maxRetries;
            this.retryBackoffMs = retryBackoffMs;
            producerLogger.info("Created RetryKafkaProducer with maxRetries: {}, backoff: {}ms",
                    maxRetries, retryBackoffMs);
        }

        public RetryKafkaProducer(Map<String, Object> configs, int maxRetries, long retryBackoffMs) {
            super(configs);
            this.maxRetries = maxRetries;
            this.retryBackoffMs = retryBackoffMs;
            producerLogger.info("Created RetryKafkaProducer with Map config, maxRetries: {}, backoff: {}ms",
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

    public static void main(String[] args) {
        logger.info("Starting CustomKafkaProducerExample");
        logger.info("Environment variables:");
        logger.info("  CLUSTER1_BOOTSTRAP_SERVERS: {}", CLUSTER1_SERVERS);
        logger.info("  CLUSTER2_BOOTSTRAP_SERVERS: {}", CLUSTER2_SERVERS);
        logger.info("  SUPERSTREAM_TOPICS_LIST: {}", System.getenv("SUPERSTREAM_TOPICS_LIST"));
        logger.info("  SUPERSTREAM_DISABLED: {}", System.getenv("SUPERSTREAM_DISABLED"));
        logger.info("  SUPERSTREAM_LATENCY_SENSITIVE: {}", System.getenv("SUPERSTREAM_LATENCY_SENSITIVE"));
        logger.info("  SUPERSTREAM_DEBUG: {}", System.getenv("SUPERSTREAM_DEBUG"));

        List<Producer<String, String>> producers = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(4);

        try {
            // Create custom producers for Cluster 1
            MetricsKafkaProducer<String, String> metricsProducer = createMetricsProducer(
                    "metrics-producer", CLUSTER1_SERVERS);

            PricelineKafkaProducer<String, String> pricelineProducer = createPricelineProducer(
                    "priceline-producer", CLUSTER1_SERVERS);

            // Create custom producer for Cluster 2
            RetryKafkaProducer<String, String> retryProducer = createRetryProducer(
                    "retry-producer", CLUSTER2_SERVERS, 3, 1000);

            producers.add(metricsProducer);
            producers.add(pricelineProducer);
            producers.add(retryProducer);

            logger.info("Created {} custom producers across {} clusters", producers.size(), 2);

            // Send messages from different producers
            executorService.submit(() -> sendMessages(metricsProducer, TOPIC_CLUSTER1, "metrics-producer"));
            executorService.submit(() -> sendMessages(pricelineProducer, TOPIC_CLUSTER1, "priceline-producer"));
            executorService.submit(() -> sendMessages(retryProducer, TOPIC_CLUSTER2, "retry-producer"));

            // Keep running for demonstration
            int iterations = 0;
            while (iterations < 10) { // Run for 10 iterations
                Thread.sleep(30000); // Wait 30 seconds between iterations

                // Log metrics
                metricsProducer.logMetrics();

                iterations++;
                logger.info("Completed iteration {}/10", iterations);
            }

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

    private static MetricsKafkaProducer<String, String> createMetricsProducer(
            String clientId, String bootstrapServers) {
        Properties props = createBaseProperties(clientId, bootstrapServers);
        return new MetricsKafkaProducer<>(props, clientId);
    }

    private static PricelineKafkaProducer<String, String> createPricelineProducer(
            String clientId, String bootstrapServers) {
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

    private static RetryKafkaProducer<String, String> createRetryProducer(
            String clientId, String bootstrapServers, int maxRetries, long retryBackoffMs) {
        Properties props = createBaseProperties(clientId, bootstrapServers);
        // Set lower retries in Kafka config since we handle retries in custom logic
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        return new RetryKafkaProducer<>(props, maxRetries, retryBackoffMs);
    }

    private static Properties createBaseProperties(String clientId, String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 200);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        return props;
    }

    private static void sendMessages(Producer<String, String> producer, String topic, String producerName) {
        int messageCount = 0;
        try {
            while (!Thread.currentThread().isInterrupted()) {
                for (int i = 0; i < 50; i++) {
                    String key = producerName + "-" + System.currentTimeMillis() + "-" + i;
                    String value = generateMessage(producerName);

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

    private static String generateMessage(String source) {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"source\": \"").append(source).append("\",");
        json.append("\"timestamp\": ").append(System.currentTimeMillis()).append(",");
        json.append("\"data\": {");
        json.append("\"payload\": \"");

        // Generate repeating pattern for compression
        for (int i = 0; i < 50; i++) {
            json.append("AAABBBCCCDDDEEEFFFGGGHHHIIIJJJKKKLLLMMMNNNOOOPPPQQQRRRSSSTTTUUUVVVWWWXXXYYYZZZ");
        }

        json.append("\",");
        json.append("\"checksum\": \"").append(source.hashCode()).append("\"");
        json.append("}");
        json.append("}");

        return json.toString();
    }
}