package ai.superstream.examples;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Spring Boot application that creates multiple producers to multiple clusters using Spring Kafka.
 * Uses ProducerFactory and KafkaTemplate from spring-kafka library.
 *
 * This demonstrates Superstream SDK's ability to intercept and optimize Spring Kafka producers.
 *
 * Run with:
 * java -javaagent:path/to/superstream-clients-1.0.0.jar -jar spring-kafka-multi-producer-example.jar
 *
 * Environment variables:
 * - CLUSTER1_BOOTSTRAP_SERVERS: Bootstrap servers for cluster 1 (default: localhost:9092)
 * - CLUSTER2_BOOTSTRAP_SERVERS: Bootstrap servers for cluster 2 (default: localhost:9093)
 * - CLUSTER3_BOOTSTRAP_SERVERS: Bootstrap servers for cluster 3 (default: localhost:9094)
 * - SUPERSTREAM_TOPICS_LIST: Comma-separated list of topics to optimize
 * - SUPERSTREAM_DISABLED: Set to true to disable Superstream optimization
 * - SUPERSTREAM_LATENCY_SENSITIVE: Set to true to preserve linger.ms values
 * - SUPERSTREAM_DEBUG: Set to true for detailed debug logging
 * - SPRING_KAFKA_COMPRESSION: Compression type (default: snappy)
 * - SPRING_KAFKA_BATCH_SIZE: Batch size in bytes (default: 16384)
 * - SPRING_KAFKA_LINGER_MS: Linger time in milliseconds (default: 50)
 * - ENABLE_TRANSACTIONAL: Set to true to enable transactional producers
 */
@SpringBootApplication
@EnableScheduling
public class SpringKafkaMultiProducerExample implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(SpringKafkaMultiProducerExample.class);

    @Autowired
    private KafkaMessageService kafkaMessageService;

    public static void main(String[] args) {
        // Log environment variables at startup
        logger.info("Starting SpringKafkaMultiProducerExample");
        logger.info("Environment variables:");
        logger.info("  CLUSTER1_BOOTSTRAP_SERVERS: {}", System.getenv("CLUSTER1_BOOTSTRAP_SERVERS"));
        logger.info("  CLUSTER2_BOOTSTRAP_SERVERS: {}", System.getenv("CLUSTER2_BOOTSTRAP_SERVERS"));
        logger.info("  CLUSTER3_BOOTSTRAP_SERVERS: {}", System.getenv("CLUSTER3_BOOTSTRAP_SERVERS"));
        logger.info("  SUPERSTREAM_TOPICS_LIST: {}", System.getenv("SUPERSTREAM_TOPICS_LIST"));
        logger.info("  SUPERSTREAM_DISABLED: {}", System.getenv("SUPERSTREAM_DISABLED"));
        logger.info("  SUPERSTREAM_LATENCY_SENSITIVE: {}", System.getenv("SUPERSTREAM_LATENCY_SENSITIVE"));
        logger.info("  SUPERSTREAM_DEBUG: {}", System.getenv("SUPERSTREAM_DEBUG"));
        logger.info("  ENABLE_TRANSACTIONAL: {}", System.getenv("ENABLE_TRANSACTIONAL"));

        SpringApplication.run(SpringKafkaMultiProducerExample.class, args);
    }

    @Override
    public void run(String... args) {
        logger.info("Spring Kafka application started successfully");
        logger.info("KafkaMessageService will send messages every 30 seconds");
    }

    /**
     * Configuration class that creates ProducerFactory and KafkaTemplate beans for different clusters
     */
    @Configuration
    public static class KafkaConfiguration {

        @Value("${cluster1.bootstrap.servers:localhost:9092}")
        private String cluster1Servers;

        @Value("${cluster2.bootstrap.servers:localhost:9093}")
        private String cluster2Servers;

        @Value("${cluster3.bootstrap.servers:localhost:9094}")
        private String cluster3Servers;

        @Value("${spring.kafka.compression:snappy}")
        private String compressionType;

        @Value("${spring.kafka.batch.size:16384}")
        private Integer batchSize;

        @Value("${spring.kafka.linger.ms:50}")
        private Integer lingerMs;

        // Cluster 1 - Producer Factory A
        @Bean(name = "cluster1ProducerFactoryA")
        public ProducerFactory<String, String> cluster1ProducerFactoryA() {
            logger.info("Creating ProducerFactory for cluster1 - factoryA");
            Map<String, Object> props = createProducerConfigs(cluster1Servers, "spring-kafka-cluster1-a");
            return new DefaultKafkaProducerFactory<>(props);
        }

        @Bean(name = "cluster1KafkaTemplateA")
        public KafkaTemplate<String, String> cluster1KafkaTemplateA() {
            return new KafkaTemplate<>(cluster1ProducerFactoryA());
        }

        // Cluster 1 - Producer Factory B with different configuration
        @Bean(name = "cluster1ProducerFactoryB")
        public ProducerFactory<String, String> cluster1ProducerFactoryB() {
            logger.info("Creating ProducerFactory for cluster1 - factoryB");
            Map<String, Object> props = createProducerConfigs(cluster1Servers, "spring-kafka-cluster1-b");
            // Override some properties for this producer
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
            props.put(ProducerConfig.LINGER_MS_CONFIG, 200);
            return new DefaultKafkaProducerFactory<>(props);
        }

        @Bean(name = "cluster1KafkaTemplateB")
        public KafkaTemplate<String, String> cluster1KafkaTemplateB() {
            return new KafkaTemplate<>(cluster1ProducerFactoryB());
        }

        // Cluster 2 - Standard Producer Factory
        @Bean(name = "cluster2ProducerFactory")
        public ProducerFactory<String, String> cluster2ProducerFactory() {
            logger.info("Creating ProducerFactory for cluster2");
            Map<String, Object> props = createProducerConfigs(cluster2Servers, "spring-kafka-cluster2");
            return new DefaultKafkaProducerFactory<>(props);
        }

        @Bean(name = "cluster2KafkaTemplate")
        public KafkaTemplate<String, String> cluster2KafkaTemplate() {
            return new KafkaTemplate<>(cluster2ProducerFactory());
        }

        // Cluster 3 - Transactional Producer Factory (if enabled)
        @Bean(name = "cluster3ProducerFactory")
        public ProducerFactory<String, String> cluster3ProducerFactory() {
            logger.info("Creating ProducerFactory for cluster3");
            Map<String, Object> props = createProducerConfigs(cluster3Servers, "spring-kafka-cluster3");

            // Check if transactional mode is enabled
            boolean enableTransactional = "true".equalsIgnoreCase(System.getenv("ENABLE_TRANSACTIONAL"));
            if (enableTransactional) {
                props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "spring-kafka-tx-");
                logger.info("Transactional mode enabled for cluster3");
            }

            DefaultKafkaProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(props);
            if (enableTransactional) {
                factory.setTransactionIdPrefix("spring-kafka-tx-");
            }

            return factory;
        }

        @Bean(name = "cluster3KafkaTemplate")
        public KafkaTemplate<String, String> cluster3KafkaTemplate() {
            return new KafkaTemplate<>(cluster3ProducerFactory());
        }

        private Map<String, Object> createProducerConfigs(String bootstrapServers, String clientIdPrefix) {
            Map<String, Object> props = new HashMap<>();

            // Use environment variables if available
            String servers = System.getenv(bootstrapServers.toUpperCase().replace(".", "_").replace(":", "") + "_BOOTSTRAP_SERVERS");
            if (servers == null) {
                servers = bootstrapServers;
            }

            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, clientIdPrefix + "-" + System.currentTimeMillis());
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            // Configurable properties from environment
            String compressionFromEnv = System.getenv("SPRING_KAFKA_COMPRESSION");
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
                    compressionFromEnv != null ? compressionFromEnv : compressionType);

            String batchSizeFromEnv = System.getenv("SPRING_KAFKA_BATCH_SIZE");
            props.put(ProducerConfig.BATCH_SIZE_CONFIG,
                    batchSizeFromEnv != null ? Integer.parseInt(batchSizeFromEnv) : batchSize);

            String lingerMsFromEnv = System.getenv("SPRING_KAFKA_LINGER_MS");
            props.put(ProducerConfig.LINGER_MS_CONFIG,
                    lingerMsFromEnv != null ? Integer.parseInt(lingerMsFromEnv) : lingerMs);

            // Fixed properties
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            props.put(ProducerConfig.RETRIES_CONFIG, 3);
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
            props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);

            logger.info("Created producer config for {} with compression={}, batch.size={}, linger.ms={}",
                    clientIdPrefix, props.get(ProducerConfig.COMPRESSION_TYPE_CONFIG),
                    props.get(ProducerConfig.BATCH_SIZE_CONFIG),
                    props.get(ProducerConfig.LINGER_MS_CONFIG));

            return props;
        }
    }

    /**
     * Service that sends messages using Spring Kafka templates
     */
    @Component
    public static class KafkaMessageService {
        private static final Logger logger = LoggerFactory.getLogger(KafkaMessageService.class);

        private final AtomicInteger messageCounter = new AtomicInteger(0);
        private final AtomicInteger successCounter = new AtomicInteger(0);
        private final AtomicInteger errorCounter = new AtomicInteger(0);

        @Autowired
        @Qualifier("cluster1KafkaTemplateA")
        private KafkaTemplate<String, String> cluster1TemplateA;

        @Autowired
        @Qualifier("cluster1KafkaTemplateB")
        private KafkaTemplate<String, String> cluster1TemplateB;

        @Autowired
        @Qualifier("cluster2KafkaTemplate")
        private KafkaTemplate<String, String> cluster2Template;

        @Autowired
        @Qualifier("cluster3KafkaTemplate")
        private KafkaTemplate<String, String> cluster3Template;

        @Scheduled(fixedDelay = 30000, initialDelay = 5000)
        public void sendMessagesToAllClusters() {
            logger.info("Starting to send messages to all clusters using Spring Kafka");

            // Send to cluster 1 with template A
            sendBatchWithTemplate(cluster1TemplateA, "spring-kafka-topic-1a", "cluster1-templateA", 25);

            // Send to cluster 1 with template B
            sendBatchWithTemplate(cluster1TemplateB, "spring-kafka-topic-1b", "cluster1-templateB", 30);

            // Send to cluster 2
            sendBatchWithTemplate(cluster2Template, "spring-kafka-topic-2", "cluster2-template", 35);

            // Send to cluster 3 (potentially transactional)
            boolean isTransactional = "true".equalsIgnoreCase(System.getenv("ENABLE_TRANSACTIONAL"));
            if (isTransactional) {
                sendTransactionalBatch(cluster3Template, "spring-kafka-topic-3", "cluster3-template-tx", 40);
            } else {
                sendBatchWithTemplate(cluster3Template, "spring-kafka-topic-3", "cluster3-template", 40);
            }

            logger.info("Message stats - Total: {}, Success: {}, Errors: {}",
                    messageCounter.get(), successCounter.get(), errorCounter.get());
        }

        private void sendBatchWithTemplate(KafkaTemplate<String, String> template, String topic,
                                           String source, int count) {
            try {
                for (int i = 0; i < count; i++) {
                    int msgNum = messageCounter.incrementAndGet();
                    String key = source + "-" + msgNum;
                    String value = generateSpringKafkaMessage(source, msgNum);

                    ListenableFuture<SendResult<String, String>> future = template.send(topic, key, value);

                    future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
                        @Override
                        public void onSuccess(SendResult<String, String> result) {
                            successCounter.incrementAndGet();
                            logger.debug("Message sent successfully: key={}, partition={}, offset={}",
                                    key, result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());
                        }

                        @Override
                        public void onFailure(Throwable ex) {
                            errorCounter.incrementAndGet();
                            logger.error("Failed to send message: key={}, error={}", key, ex.getMessage());
                        }
                    });
                }

                template.flush();
                logger.info("Sent {} messages to topic {} from {}", count, topic, source);

            } catch (Exception e) {
                logger.error("Error in sendBatchWithTemplate for topic {}: {}", topic, e.getMessage(), e);
            }
        }

        private void sendTransactionalBatch(KafkaTemplate<String, String> template, String topic,
                                            String source, int count) {
            try {
                template.executeInTransaction(t -> {
                    for (int i = 0; i < count; i++) {
                        int msgNum = messageCounter.incrementAndGet();
                        String key = source + "-" + msgNum;
                        String value = generateSpringKafkaMessage(source, msgNum);

                        t.send(topic, key, value);
                    }
                    logger.info("Sent {} transactional messages to topic {} from {}", count, topic, source);
                    return true;
                });

                successCounter.addAndGet(count);

            } catch (Exception e) {
                errorCounter.addAndGet(count);
                logger.error("Error in transactional batch for topic {}: {}", topic, e.getMessage(), e);
            }
        }

        private String generateSpringKafkaMessage(String source, int messageNumber) {
            return String.format(
                    "{"
                            + "\"framework\":\"spring-kafka\","
                            + "\"source\":\"%s\","
                            + "\"messageNumber\":%d,"
                            + "\"timestamp\":%d,"
                            + "\"data\":{"
                            + "\"payload\":\"%s\","
                            + "\"metadata\":{"
                            + "\"processedBy\":\"KafkaTemplate\","
                            + "\"springBootVersion\":\"2.7.x\""
                            + "}"
                            + "}"
                            + "}",
                    source,
                    messageNumber,
                    System.currentTimeMillis(),
                    generateCompressiblePayload()
            );
        }

        private String generateCompressiblePayload() {
            StringBuilder payload = new StringBuilder();
            // Generate repeating pattern for compression testing
            for (int i = 0; i < 30; i++) {
                payload.append("SpringKafkaTemplateMessage");
                payload.append("123456789012345678901234567890");
            }
            return payload.toString();
        }
    }
}