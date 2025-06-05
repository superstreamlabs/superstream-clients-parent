package ai.superstream.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Spring Boot application that creates multiple KafkaProducer instances to multiple clusters.
 * Uses direct KafkaProducer (kafka-clients lib) instead of Spring Kafka abstractions.
 *
 * Each producer uses a different bootstrap server format to test compatibility.
 *
 * This demonstrates Superstream SDK's ability to work with Spring-managed KafkaProducer beans.
 *
 * Run with:
 * java -javaagent:path/to/superstream-clients-1.0.0.jar -jar spring-multi-producer-example.jar
 *
 * Environment variables:
 * - CLUSTER1_BOOTSTRAP_SERVERS: Bootstrap servers for cluster 1 (default: localhost:9092)
 * - CLUSTER2_BOOTSTRAP_SERVERS: Bootstrap servers for cluster 2 (default: localhost:9095)
 * - CLUSTER3_BOOTSTRAP_SERVERS: Bootstrap servers for cluster 3 (default: localhost:9096)
 * - SUPERSTREAM_TOPICS_LIST: Comma-separated list of topics to optimize
 * - SUPERSTREAM_DISABLED: Set to true to disable Superstream optimization
 * - SUPERSTREAM_LATENCY_SENSITIVE: Set to true to preserve linger.ms values
 * - SUPERSTREAM_DEBUG: Set to true for detailed debug logging
 * - SPRING_PRODUCER_COMPRESSION: Compression type (default: gzip)
 * - SPRING_PRODUCER_BATCH_SIZE: Batch size in bytes (default: 32768)
 * - SPRING_PRODUCER_LINGER_MS: Linger time in milliseconds (default: 100)
 */
@SpringBootApplication
@EnableScheduling
public class SpringMultiProducerExample implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(SpringMultiProducerExample.class);

    @Autowired
    private MessageService messageService;

    public static void main(String[] args) {
        // Log environment variables at startup
        logger.info("Starting SpringMultiProducerExample");
        logger.info("Environment variables:");
        logger.info("  CLUSTER1_BOOTSTRAP_SERVERS: {}", System.getenv("CLUSTER1_BOOTSTRAP_SERVERS"));
        logger.info("  CLUSTER2_BOOTSTRAP_SERVERS: {}", System.getenv("CLUSTER2_BOOTSTRAP_SERVERS"));
        logger.info("  CLUSTER3_BOOTSTRAP_SERVERS: {}", System.getenv("CLUSTER3_BOOTSTRAP_SERVERS"));
        logger.info("  SUPERSTREAM_TOPICS_LIST: {}", System.getenv("SUPERSTREAM_TOPICS_LIST"));
        logger.info("  SUPERSTREAM_DISABLED: {}", System.getenv("SUPERSTREAM_DISABLED"));
        logger.info("  SUPERSTREAM_LATENCY_SENSITIVE: {}", System.getenv("SUPERSTREAM_LATENCY_SENSITIVE"));
        logger.info("  SUPERSTREAM_DEBUG: {}", System.getenv("SUPERSTREAM_DEBUG"));

        SpringApplication.run(SpringMultiProducerExample.class, args);
    }

    @Override
    public void run(String... args) {
        logger.info("Spring application started successfully");
        logger.info("Message service will send messages every 30 seconds");
        // Application will keep running due to @Scheduled tasks
    }

    /**
     * Configuration class that creates KafkaProducer beans for different clusters
     */
    @Configuration
    public static class KafkaProducerConfiguration {

        @Value("${cluster1.bootstrap.servers:localhost:9092}")
        private String cluster1Servers;

        @Value("${cluster2.bootstrap.servers:localhost:9095}")
        private String cluster2Servers;

        @Value("${cluster3.bootstrap.servers:localhost:9096}")
        private String cluster3Servers;

        @Value("${spring.producer.compression:gzip}")
        private String compressionType;

        @Value("${spring.producer.batch.size:32768}")
        private Integer batchSize;

        @Value("${spring.producer.linger.ms:100}")
        private Integer lingerMs;

        @Bean(name = "cluster1ProducerA")
        public Producer<String, String> cluster1ProducerA() {
            logger.info("Creating producer for cluster1 - producerA with String bootstrap format");
            Properties props = createProducerPropertiesString(cluster1Servers, "spring-cluster1-producer-a");
            return new KafkaProducer<>(props);
        }

        @Bean(name = "cluster1ProducerB")
        public Producer<String, String> cluster1ProducerB() {
            logger.info("Creating producer for cluster1 - producerB with List bootstrap format");
            Properties props = createProducerPropertiesList(cluster1Servers, "spring-cluster1-producer-b");
            return new KafkaProducer<>(props);
        }

        @Bean(name = "cluster2Producer")
        public Producer<String, String> cluster2Producer() {
            logger.info("Creating producer for cluster2 with Arrays.asList() bootstrap format");
            Properties props = createProducerPropertiesArraysList(cluster2Servers, "spring-cluster2-producer");
            return new KafkaProducer<>(props);
        }

        @Bean(name = "cluster3Producer")
        public Producer<String, String> cluster3Producer() {
            logger.info("Creating producer for cluster3 with List.of() bootstrap format");
            Properties props = createProducerPropertiesListOf(cluster3Servers, "spring-cluster3-producer");
            // Override some properties for this specific producer
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536); // 64KB
            return new KafkaProducer<>(props);
        }

        // Create properties with String format bootstrap servers
        private Properties createProducerPropertiesString(String bootstrapServers, String clientId) {
            Properties props = createBaseProperties(clientId);

            // Use environment variables if available, otherwise use Spring properties
            String servers = getBootstrapServers(bootstrapServers);
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);

            logger.info("Created producer properties for {} with String bootstrap format: {}", clientId, servers);
            return props;
        }

        // Create properties with List format bootstrap servers
        private Properties createProducerPropertiesList(String bootstrapServers, String clientId) {
            Properties props = createBaseProperties(clientId);

            String servers = getBootstrapServers(bootstrapServers);
            List<String> serversList = Arrays.asList(servers.split(","));
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serversList);

            logger.info("Created producer properties for {} with List bootstrap format: {}", clientId, serversList);
            return props;
        }

        // Create properties with Arrays.asList() format bootstrap servers
        private Properties createProducerPropertiesArraysList(String bootstrapServers, String clientId) {
            Properties props = createBaseProperties(clientId);

            String servers = getBootstrapServers(bootstrapServers);
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Arrays.asList(servers.split(",")));

            logger.info("Created producer properties for {} with Arrays.asList() bootstrap format", clientId);
            return props;
        }

        // Create properties with List.of() format bootstrap servers (Java 9+)
        private Properties createProducerPropertiesListOf(String bootstrapServers, String clientId) {
            Properties props = createBaseProperties(clientId);

            String servers = getBootstrapServers(bootstrapServers);
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, List.of(servers.split(",")));

            logger.info("Created producer properties for {} with List.of() bootstrap format", clientId);
            return props;
        }

        // Create base properties without bootstrap servers
        private Properties createBaseProperties(String clientId) {
            Properties props = new Properties();
            props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            // Configurable properties
            String compressionFromEnv = System.getenv("SPRING_PRODUCER_COMPRESSION");
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
                    compressionFromEnv != null ? compressionFromEnv : compressionType);

            String batchSizeFromEnv = System.getenv("SPRING_PRODUCER_BATCH_SIZE");
            props.put(ProducerConfig.BATCH_SIZE_CONFIG,
                    batchSizeFromEnv != null ? Integer.parseInt(batchSizeFromEnv) : batchSize);

            String lingerMsFromEnv = System.getenv("SPRING_PRODUCER_LINGER_MS");
            props.put(ProducerConfig.LINGER_MS_CONFIG,
                    lingerMsFromEnv != null ? Integer.parseInt(lingerMsFromEnv) : lingerMs);

            // Fixed properties
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            props.put(ProducerConfig.RETRIES_CONFIG, 3);
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

            logger.info("Created base producer properties with compression={}, batch.size={}, linger.ms={}",
                    props.get(ProducerConfig.COMPRESSION_TYPE_CONFIG),
                    props.get(ProducerConfig.BATCH_SIZE_CONFIG),
                    props.get(ProducerConfig.LINGER_MS_CONFIG));

            return props;
        }

        // Get bootstrap servers from environment or use default
        private String getBootstrapServers(String defaultServers) {
            String envKey = defaultServers.toUpperCase().replace(".", "_").replace(":", "") + "_BOOTSTRAP_SERVERS";
            String servers = System.getenv(envKey);
            return servers != null ? servers : defaultServers;
        }
    }

    /**
     * Service that sends messages using the configured producers
     */
    @Component
    public static class MessageService {
        private static final Logger logger = LoggerFactory.getLogger(MessageService.class);

        private final AtomicInteger messageCounter = new AtomicInteger(0);

        @Autowired
        @Qualifier("cluster1ProducerA")
        private Producer<String, String> cluster1ProducerA;

        @Autowired
        @Qualifier("cluster1ProducerB")
        private Producer<String, String> cluster1ProducerB;

        @Autowired
        @Qualifier("cluster2Producer")
        private Producer<String, String> cluster2Producer;

        @Autowired
        @Qualifier("cluster3Producer")
        private Producer<String, String> cluster3Producer;

        @Scheduled(fixedDelay = 30000, initialDelay = 5000)
        public void sendMessagesToAllClusters() {
            logger.info("Sending batch of messages to all clusters");

            // Send to cluster 1 with producer A
            sendBatch(cluster1ProducerA, "spring-topic-1a", "cluster1-producerA", 20);

            // Send to cluster 1 with producer B
            sendBatch(cluster1ProducerB, "spring-topic-1b", "cluster1-producerB", 25);

            // Send to cluster 2
            sendBatch(cluster2Producer, "spring-topic-2", "cluster2-producer", 30);

            // Send to cluster 3
            sendBatch(cluster3Producer, "spring-topic-3", "cluster3-producer", 35);

            logger.info("Total messages sent so far: {}", messageCounter.get());
        }

        private void sendBatch(Producer<String, String> producer, String topic, String source, int count) {
            try {
                for (int i = 0; i < count; i++) {
                    int msgNum = messageCounter.incrementAndGet();
                    String key = source + "-" + msgNum;
                    String value = generateMessage(source, msgNum);

                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            logger.error("Error sending message: {}", exception.getMessage());
                        } else {
                            logger.debug("Message sent to partition {} with offset {}",
                                    metadata.partition(), metadata.offset());
                        }
                    });
                }

                producer.flush();
                logger.info("Sent {} messages to topic {} from {}", count, topic, source);

            } catch (Exception e) {
                logger.error("Error in sendBatch for topic {}: {}", topic, e.getMessage(), e);
            }
        }

        private String generateMessage(String source, int messageNumber) {
            return String.format(
                    "{"
                            + "\"source\":\"%s\","
                            + "\"messageNumber\":%d,"
                            + "\"timestamp\":%d,"
                            + "\"data\":{"
                            + "\"content\":\"%s\","
                            + "\"checksum\":\"%s\""
                            + "}"
                            + "}",
                    source,
                    messageNumber,
                    System.currentTimeMillis(),
                    generateCompressibleContent(),
                    String.valueOf(source.hashCode() + messageNumber)
            );
        }

        private String generateCompressibleContent() {
            StringBuilder content = new StringBuilder();
            // Generate repeating pattern for good compression
            for (int i = 0; i < 20; i++) {
                content.append("SpringBootKafkaProducerTestMessageContent");
                content.append("AAABBBCCCDDDEEEFFFGGGHHHIIIJJJKKKLLLMMMNNN");
            }
            return content.toString();
        }
    }
}