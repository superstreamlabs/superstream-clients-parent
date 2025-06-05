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
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Spring Boot application that creates multiple producers to multiple clusters using Spring Kafka.
 * Uses ProducerFactory and KafkaTemplate from spring-kafka library.
 *
 * Loads configuration from application.yml file and supports different bootstrap server formats.
 *
 * This demonstrates Superstream SDK's ability to intercept and optimize Spring Kafka producers.
 *
 * Run with:
 * java -javaagent:path/to/superstream-clients-1.0.0.jar -jar spring-kafka-multi-producer-example.jar
 *
 * Configuration is loaded from application.yml, which can be overridden by environment variables:
 * - CLUSTER1_BOOTSTRAP_SERVERS: Bootstrap servers for cluster 1
 * - CLUSTER2_BOOTSTRAP_SERVERS: Bootstrap servers for cluster 2
 * - CLUSTER3_BOOTSTRAP_SERVERS: Bootstrap servers for cluster 3
 * - SUPERSTREAM_TOPICS_LIST: Comma-separated list of topics to optimize
 * - SUPERSTREAM_DISABLED: Set to true to disable Superstream optimization
 * - SUPERSTREAM_LATENCY_SENSITIVE: Set to true to preserve linger.ms values
 * - SUPERSTREAM_DEBUG: Set to true for detailed debug logging
 * - SPRING_KAFKA_COMPRESSION: Compression type
 * - SPRING_KAFKA_BATCH_SIZE: Batch size in bytes
 * - SPRING_KAFKA_LINGER_MS: Linger time in milliseconds
 * - ENABLE_TRANSACTIONAL: Set to true to enable transactional producers
 */
@SpringBootApplication
@EnableScheduling
@PropertySource("classpath:application.yml")
public class SpringKafkaMultiProducerExample implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(SpringKafkaMultiProducerExample.class);

    @Autowired
    private KafkaMessageService kafkaMessageService;

    public static void main(String[] args) {
        // Log environment variables at startup
        logger.info("Starting SpringKafkaMultiProducerExample");
        logger.info("Loading configuration from application.yml");
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
        logger.info("Configuration loaded from application.yml");
        logger.info("KafkaMessageService will send messages every 30 seconds");
    }

    /**
     * Configuration class that creates ProducerFactory and KafkaTemplate beans for different clusters
     * Loads properties from application.yml
     */
    @Configuration
    public static class KafkaConfiguration {

        // Load from application.yml
        @Value("${cluster1.bootstrap.servers:localhost:9092}")
        private String cluster1Servers;

        @Value("${cluster2.bootstrap.servers:localhost:9095}")
        private String cluster2Servers;

        @Value("${cluster3.bootstrap.servers:localhost:9096}")
        private String cluster3Servers;

        @Value("${spring.kafka.compression:snappy}")
        private String compressionType;

        @Value("${spring.kafka.batch.size:16384}")
        private Integer batchSize;

        @Value("${spring.kafka.linger.ms:50}")
        private Integer lingerMs;

        @Value("${app.kafka.transactional.enabled:false}")
        private boolean transactionalEnabled;

        @Value("${app.kafka.transactional.id-prefix:spring-kafka-tx-}")
        private String transactionalIdPrefix;

        // Cluster 1 - Producer Factory A with String bootstrap format
        @Bean(name = "cluster1ProducerFactoryA")
        public ProducerFactory<String, String> cluster1ProducerFactoryA() {
            logger.info("Creating ProducerFactory for cluster1 - factoryA with String bootstrap format");
            Map<String, Object> props = createProducerConfigsString(cluster1Servers, "spring-kafka-cluster1-a");
            return new DefaultKafkaProducerFactory<>(props);
        }

        @Bean(name = "cluster1KafkaTemplateA")
        public KafkaTemplate<String, String> cluster1KafkaTemplateA() {
            return new KafkaTemplate<>(cluster1ProducerFactoryA());
        }

        // Cluster 1 - Producer Factory B with List bootstrap format
        @Bean(name = "cluster1ProducerFactoryB")
        public ProducerFactory<String, String> cluster1ProducerFactoryB() {
            logger.info("Creating ProducerFactory for cluster1 - factoryB with List bootstrap format");
            Map<String, Object> props = createProducerConfigsList(cluster1Servers, "spring-kafka-cluster1-b");
            // Override some properties for this producer
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
            props.put(ProducerConfig.LINGER_MS_CONFIG, 200);
            return new DefaultKafkaProducerFactory<>(props);
        }

        @Bean(name = "cluster1KafkaTemplateB")
        public KafkaTemplate<String, String> cluster1KafkaTemplateB() {
            return new KafkaTemplate<>(cluster1ProducerFactoryB());
        }

        // Cluster 2 - Standard Producer Factory with Arrays.asList() format
        @Bean(name = "cluster2ProducerFactory")
        public ProducerFactory<String, String> cluster2ProducerFactory() {
            logger.info("Creating ProducerFactory for cluster2 with Arrays.asList() bootstrap format");
            Map<String, Object> props = createProducerConfigsArraysList(cluster2Servers, "spring-kafka-cluster2");
            return new DefaultKafkaProducerFactory<>(props);
        }

        @Bean(name = "cluster2KafkaTemplate")
        public KafkaTemplate<String, String> cluster2KafkaTemplate() {
            return new KafkaTemplate<>(cluster2ProducerFactory());
        }

        // Cluster 3 - Transactional Producer Factory with List.of() format
        @Bean(name = "cluster3ProducerFactory")
        public ProducerFactory<String, String> cluster3ProducerFactory() {
            logger.info("Creating ProducerFactory for cluster3 with List.of() bootstrap format");
            Map<String, Object> props = createProducerConfigsListOf(cluster3Servers, "spring-kafka-cluster3");

            // Check if transactional mode is enabled from application.yml
            if (transactionalEnabled) {
                props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalIdPrefix);
                logger.info("Transactional mode enabled for cluster3 with prefix: {}", transactionalIdPrefix);
            }

            DefaultKafkaProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(props);
            if (transactionalEnabled) {
                factory.setTransactionIdPrefix(transactionalIdPrefix);
            }

            return factory;
        }

        @Bean(name = "cluster3KafkaTemplate")
        public KafkaTemplate<String, String> cluster3KafkaTemplate() {
            return new KafkaTemplate<>(cluster3ProducerFactory());
        }

        // Create producer configs with String format
        private Map<String, Object> createProducerConfigsString(String bootstrapServers, String clientIdPrefix) {
            Map<String, Object> props = createBaseConfigs(clientIdPrefix);
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            logger.info("Created producer config with String bootstrap format: {}", bootstrapServers);
            return props;
        }

        // Create producer configs with List format
        private Map<String, Object> createProducerConfigsList(String bootstrapServers, String clientIdPrefix) {
            Map<String, Object> props = createBaseConfigs(clientIdPrefix);
            List<String> serversList = Arrays.asList(bootstrapServers.split(","));
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serversList);
            logger.info("Created producer config with List bootstrap format: {}", serversList);
            return props;
        }

        // Create producer configs with Arrays.asList() format
        private Map<String, Object> createProducerConfigsArraysList(String bootstrapServers, String clientIdPrefix) {
            Map<String, Object> props = createBaseConfigs(clientIdPrefix);
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Arrays.asList(bootstrapServers.split(",")));
            logger.info("Created producer config with Arrays.asList() bootstrap format");
            return props;
        }

        // Create producer configs with List.of() format (Java 9+)
        private Map<String, Object> createProducerConfigsListOf(String bootstrapServers, String clientIdPrefix) {
            Map<String, Object> props = createBaseConfigs(clientIdPrefix);
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, List.of(bootstrapServers.split(",")));
            logger.info("Created producer config with List.of() bootstrap format");
            return props;
        }

        // Create base configuration from application.yml
        private Map<String, Object> createBaseConfigs(String clientIdPrefix) {
            Map<String, Object> props = new HashMap<>();

            props.put(ProducerConfig.CLIENT_ID_CONFIG, clientIdPrefix + "-" + System.currentTimeMillis());
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            // Load from application.yml with environment variable override support
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
            props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);

            // Fixed properties from application.yml
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
     * Topic names are loaded from application.yml
     */
    @Component
    public static class KafkaMessageService {
        private static final Logger logger = LoggerFactory.getLogger(KafkaMessageService.class);

        private final AtomicInteger messageCounter = new AtomicInteger(0);
        private final AtomicInteger successCounter = new AtomicInteger(0);
        private final AtomicInteger errorCounter = new AtomicInteger(0);

        @Value("${app.kafka.topics.cluster1a:spring-kafka-topic-1a}")
        private String topicCluster1a;

        @Value("${app.kafka.topics.cluster1b:spring-kafka-topic-1b}")
        private String topicCluster1b;

        @Value("${app.kafka.topics.cluster2:spring-kafka-topic-2}")
        private String topicCluster2;

        @Value("${app.kafka.topics.cluster3:spring-kafka-topic-3}")
        private String topicCluster3;

        @Value("${app.kafka.transactional.enabled:false}")
        private boolean transactionalEnabled;

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
            logger.info("Topics loaded from application.yml: {}, {}, {}, {}",
                    topicCluster1a, topicCluster1b, topicCluster2, topicCluster3);

            // Send to cluster 1 with template A
            sendBatchWithTemplate(cluster1TemplateA, topicCluster1a, "cluster1-templateA", 25);

            // Send to cluster 1 with template B
            sendBatchWithTemplate(cluster1TemplateB, topicCluster1b, "cluster1-templateB", 30);

            // Send to cluster 2
            sendBatchWithTemplate(cluster2Template, topicCluster2, "cluster2-template", 35);

            // Send to cluster 3 (potentially transactional)
            if (transactionalEnabled) {
                sendTransactionalBatch(cluster3Template, topicCluster3, "cluster3-template-tx", 40);
            } else {
                sendBatchWithTemplate(cluster3Template, topicCluster3, "cluster3-template", 40);
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
                            + "\"springBootVersion\":\"2.7.x\","
                            + "\"configSource\":\"application.yml\""
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