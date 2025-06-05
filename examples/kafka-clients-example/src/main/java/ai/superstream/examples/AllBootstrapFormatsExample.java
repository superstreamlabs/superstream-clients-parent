package ai.superstream.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

/**
 * Example application that demonstrates ALL possible formats for bootstrap.servers configuration.
 * This comprehensive test ensures Superstream SDK handles any format a developer might use.
 *
 * Run with:
 * java -javaagent:path/to/superstream-clients-1.0.0.jar -jar all-bootstrap-formats-example.jar
 *
 * Environment variables:
 * - KAFKA_BOOTSTRAP_SERVERS: Bootstrap servers (default: localhost:9092)
 * - KAFKA_BOOTSTRAP_SERVERS_2: Additional servers for multi-server tests (default: localhost:9095)
 * - SUPERSTREAM_DEBUG: Set to true for detailed debug logging
 * - TEST_TOPIC: Topic name for testing (default: bootstrap-formats-test)
 */
public class AllBootstrapFormatsExample {
    private static final Logger logger = LoggerFactory.getLogger(AllBootstrapFormatsExample.class);

    private static final String BOOTSTRAP_SERVER_1 = System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null ?
            System.getenv("KAFKA_BOOTSTRAP_SERVERS") : "localhost:9092";
    private static final String BOOTSTRAP_SERVER_2 = System.getenv("KAFKA_BOOTSTRAP_SERVERS_2") != null ?
            System.getenv("KAFKA_BOOTSTRAP_SERVERS_2") : "localhost:9095";

    private static final String TEST_TOPIC = System.getenv("TEST_TOPIC") != null ?
            System.getenv("TEST_TOPIC") : "bootstrap-formats-test";

    // Configuration for small batch size to see optimization
    private static final Integer INITIAL_BATCH_SIZE = 8192; // 8KB
    private static final String COMPRESSION_TYPE = "gzip";

    public static void main(String[] args) {
        logger.info("Starting AllBootstrapFormatsExample");
        logger.info("This example tests ALL possible bootstrap.servers formats");
        logger.info("Bootstrap Server 1: {}", BOOTSTRAP_SERVER_1);
        logger.info("Bootstrap Server 2: {}", BOOTSTRAP_SERVER_2);
        logger.info("============================================================");

        try {
            // Test with Properties
            testPropertiesFormats();

            Thread.sleep(1000);
            logger.info("\n============================================================");

            // Test with Map/HashMap
            testMapFormats();

            Thread.sleep(1000);
            logger.info("\n============================================================");

            // Test edge cases
            testEdgeCases();

        } catch (Exception e) {
            logger.error("Error in main execution", e);
        }
    }

    /**
     * Test all formats using Properties
     */
    private static void testPropertiesFormats() {
        logger.info("TESTING PROPERTIES-BASED CONFIGURATIONS");
        logger.info("========================================");

        // Format 1: String with single server
        testFormat("Properties + String (single server)", () -> {
            Properties props = createBaseProperties("props-string-single");
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_1);
            return new KafkaProducer<>(props);
        });

        // Format 2: String with multiple servers
        testFormat("Properties + String (multiple servers)", () -> {
            Properties props = createBaseProperties("props-string-multiple");
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    BOOTSTRAP_SERVER_1 + "," + BOOTSTRAP_SERVER_2);
            return new KafkaProducer<>(props);
        });

        // Format 3: ArrayList
        testFormat("Properties + ArrayList", () -> {
            Properties props = createBaseProperties("props-arraylist");
            List<String> servers = new ArrayList<>();
            servers.add(BOOTSTRAP_SERVER_1);
            servers.add(BOOTSTRAP_SERVER_2);
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            return new KafkaProducer<>(props);
        });

        // Format 4: Arrays.asList()
        testFormat("Properties + Arrays.asList()", () -> {
            Properties props = createBaseProperties("props-arrays-aslist");
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    Arrays.asList(BOOTSTRAP_SERVER_1, BOOTSTRAP_SERVER_2));
            return new KafkaProducer<>(props);
        });

        // Format 5: List.of() - Java 9+
        testFormat("Properties + List.of()", () -> {
            Properties props = createBaseProperties("props-list-of");
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    List.of(BOOTSTRAP_SERVER_1, BOOTSTRAP_SERVER_2));
            return new KafkaProducer<>(props);
        });

        // Format 6: LinkedList
        testFormat("Properties + LinkedList", () -> {
            Properties props = createBaseProperties("props-linkedlist");
            List<String> servers = new LinkedList<>();
            servers.add(BOOTSTRAP_SERVER_1);
            servers.add(BOOTSTRAP_SERVER_2);
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            return new KafkaProducer<>(props);
        });

        // Format 7: Vector (legacy)
        testFormat("Properties + Vector", () -> {
            Properties props = createBaseProperties("props-vector");
            Vector<String> servers = new Vector<>();
            servers.add(BOOTSTRAP_SERVER_1);
            servers.add(BOOTSTRAP_SERVER_2);
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            return new KafkaProducer<>(props);
        });
    }

    /**
     * Test all formats using Map/HashMap
     */
    private static void testMapFormats() {
        logger.info("TESTING MAP-BASED CONFIGURATIONS");
        logger.info("================================");

        // Format 1: Map + String (single server)
        testFormat("Map + String (single server)", () -> {
            Map<String, Object> configs = createBaseMap("map-string-single");
            configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_1);
            return new KafkaProducer<>(configs);
        });

        // Format 2: Map + String (multiple servers)
        testFormat("Map + String (multiple servers)", () -> {
            Map<String, Object> configs = createBaseMap("map-string-multiple");
            configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    BOOTSTRAP_SERVER_1 + "," + BOOTSTRAP_SERVER_2);
            return new KafkaProducer<>(configs);
        });

        // Format 3: HashMap + ArrayList
        testFormat("HashMap + ArrayList", () -> {
            Map<String, Object> configs = new HashMap<>();
            copyBaseConfig(configs, "hashmap-arraylist");
            List<String> servers = new ArrayList<>();
            servers.add(BOOTSTRAP_SERVER_1);
            servers.add(BOOTSTRAP_SERVER_2);
            configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            return new KafkaProducer<>(configs);
        });

        // Format 4: Map + Arrays.asList()
        testFormat("Map + Arrays.asList()", () -> {
            Map<String, Object> configs = createBaseMap("map-arrays-aslist");
            configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    Arrays.asList(BOOTSTRAP_SERVER_1, BOOTSTRAP_SERVER_2));
            return new KafkaProducer<>(configs);
        });

        // Format 5: Map + List.of()
        testFormat("Map + List.of()", () -> {
            Map<String, Object> configs = createBaseMap("map-list-of");
            configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    List.of(BOOTSTRAP_SERVER_1, BOOTSTRAP_SERVER_2));
            return new KafkaProducer<>(configs);
        });

        // Format 6: Map + Collections.singletonList()
        testFormat("Map + Collections.singletonList()", () -> {
            Map<String, Object> configs = createBaseMap("map-singleton-list");
            configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    Collections.singletonList(BOOTSTRAP_SERVER_1));
            return new KafkaProducer<>(configs);
        });
    }

    /**
     * Test edge cases and special scenarios
     */
    private static void testEdgeCases() {
        logger.info("TESTING EDGE CASES");
        logger.info("==================");

        // Edge case 1: Spaces in string
        testFormat("String with spaces", () -> {
            Properties props = createBaseProperties("edge-spaces");
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    " " + BOOTSTRAP_SERVER_1 + " , " + BOOTSTRAP_SERVER_2 + " ");
            return new KafkaProducer<>(props);
        });

        // Edge case 2: Empty list (should fail)
        testFormat("Empty list (should fail)", () -> {
            try {
                Properties props = createBaseProperties("edge-empty-list");
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, new ArrayList<String>());
                return new KafkaProducer<>(props);
            } catch (Exception e) {
                logger.error("Expected failure for empty list: {}", e.getMessage());
                return null;
            }
        });

        // Edge case 3: Mixed collection types
        testFormat("Properties copied to Map", () -> {
            Properties props = createBaseProperties("edge-props-to-map");
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    Arrays.asList(BOOTSTRAP_SERVER_1, BOOTSTRAP_SERVER_2));

            // Copy Properties to Map
            Map<String, Object> configs = new HashMap<>();
            props.forEach((key, value) -> configs.put(key.toString(), value));

            return new KafkaProducer<>(configs);
        });

        // Edge case 4: Immutable list
        testFormat("Immutable list", () -> {
            Map<String, Object> configs = createBaseMap("edge-immutable");
            List<String> servers = Collections.unmodifiableList(
                    Arrays.asList(BOOTSTRAP_SERVER_1, BOOTSTRAP_SERVER_2)
            );
            configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            return new KafkaProducer<>(configs);
        });
    }

    /**
     * Test a specific format and log results
     */
    private static void testFormat(String formatName, ProducerSupplier supplier) {
        logger.info("\nTesting format: {}", formatName);
        logger.info("----------------------------------------");

        Producer<String, String> producer = null;
        try {
            producer = supplier.get();

            if (producer != null) {
                // Send a test message
                String key = "format-test";
                String value = String.format("{\"format\":\"%s\",\"timestamp\":%d}",
                        formatName, System.currentTimeMillis());

                ProducerRecord<String, String> record = new ProducerRecord<>(TEST_TOPIC, key, value);
                producer.send(record).get(); // Synchronous for testing

                logger.info("✅ SUCCESS: Producer created and message sent for format: {}", formatName);
            }

        } catch (Exception e) {
            logger.error("❌ FAILED: Format {} - Error: {}", formatName, e.getMessage());
        } finally {
            if (producer != null) {
                try {
                    producer.close();
                } catch (Exception e) {
                    logger.error("Error closing producer: {}", e.getMessage());
                }
            }
        }
    }

    /**
     * Create base Properties with common configurations
     */
    private static Properties createBaseProperties(String clientId) {
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, INITIAL_BATCH_SIZE);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return props;
    }

    /**
     * Create base Map with common configurations
     */
    private static Map<String, Object> createBaseMap(String clientId) {
        Map<String, Object> configs = new HashMap<>();
        copyBaseConfig(configs, clientId);
        return configs;
    }

    /**
     * Copy base configuration to a Map
     */
    private static void copyBaseConfig(Map<String, Object> configs, String clientId) {
        configs.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.BATCH_SIZE_CONFIG, INITIAL_BATCH_SIZE);
        configs.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        configs.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE);
        configs.put(ProducerConfig.ACKS_CONFIG, "all");
        configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    }

    /**
     * Functional interface for producer creation
     */
    @FunctionalInterface
    private interface ProducerSupplier {
        Producer<String, String> get() throws Exception;
    }
}