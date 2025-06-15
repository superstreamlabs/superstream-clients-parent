package ai.superstream.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Example application that demonstrates the issue with Properties passed by value vs by reference.
 * This shows when Superstream CAN and CANNOT optimize producer configurations.
 *
 * IMPORTANT: This example shows a limitation where Superstream cannot optimize if properties
 * are copied or made unmodifiable before passing to KafkaProducer constructor.
 *
 * Run with:
 * java -javaagent:path/to/superstream-clients-1.0.0.jar -jar producer-props-passing-example.jar
 *
 * Environment variables:
 * - KAFKA_BOOTSTRAP_SERVERS: Bootstrap servers (default: localhost:9092)
 * - SUPERSTREAM_DEBUG: Set to true for detailed debug logging
 */
public class ProducerPropsPassingExample {
    private static final Logger logger = LoggerFactory.getLogger(ProducerPropsPassingExample.class);

    private static final String BOOTSTRAP_SERVERS = System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null ?
            System.getenv("KAFKA_BOOTSTRAP_SERVERS") : "localhost:9092";

    private static final String TOPIC_NAME = "props-passing-test-topic";
    private static final Integer INITIAL_BATCH_SIZE = 16384; // 16KB - small size to see optimization
    private static final Integer INITIAL_LINGER_MS = 100;

    public static void main(String[] args) {
        logger.info("Starting ProducerPropsPassingExample");
        logger.info("This example demonstrates when Superstream CAN and CANNOT optimize properties");
        logger.info("Initial batch.size: {}, Initial linger.ms: {}", INITIAL_BATCH_SIZE, INITIAL_LINGER_MS);
        logger.info("Superstream should optimize these to larger values if enabled");
        logger.info("============================================================");

        try {
            // Test Case 1: Properties passed by reference (WORKS)
            testPropertiesByReference();

            Thread.sleep(2000);
            logger.info("============================================================");

            // Test Case 2: Properties copied before passing (DOESN'T WORK)
            testPropertiesCopied();

            Thread.sleep(2000);
            logger.info("============================================================");

            // Test Case 3: Unmodifiable Map (DOESN'T WORK)
            testUnmodifiableMap();

            Thread.sleep(2000);
            logger.info("============================================================");

            // Test Case 4: Properties object reused (WORKS)
            testPropertiesReused();

            Thread.sleep(2000);
            logger.info("============================================================");

            // Test Case 5: Map passed by reference (WORKS)
            testMapByReference();

        } catch (Exception e) {
            logger.error("Error in main execution", e);
        }
    }

    /**
     * Test Case 1: Properties passed by reference - SUPERSTREAM CAN OPTIMIZE
     */
    private static void testPropertiesByReference() {
        logger.info("TEST CASE 1: Properties passed by reference");
        logger.info("Expected: Superstream WILL optimize these properties");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-by-reference");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, INITIAL_BATCH_SIZE);
        props.put(ProducerConfig.LINGER_MS_CONFIG, INITIAL_LINGER_MS);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        logger.info("Before creating producer - batch.size: {}, linger.ms: {}",
                props.get(ProducerConfig.BATCH_SIZE_CONFIG),
                props.get(ProducerConfig.LINGER_MS_CONFIG));

        // Pass the same Properties object to KafkaProducer
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Check if Superstream modified the properties
        logger.info("After creating producer - batch.size: {}, linger.ms: {}",
                props.get(ProducerConfig.BATCH_SIZE_CONFIG),
                props.get(ProducerConfig.LINGER_MS_CONFIG));

        if (!props.get(ProducerConfig.BATCH_SIZE_CONFIG).equals(INITIAL_BATCH_SIZE)) {
            logger.info("✅ SUCCESS: Superstream optimized batch.size from {} to {}",
                    INITIAL_BATCH_SIZE, props.get(ProducerConfig.BATCH_SIZE_CONFIG));
        } else {
            logger.warn("❌ Properties were NOT optimized (Superstream might be disabled)");
        }

        sendTestMessage(producer, "test1-reference");
        producer.close();
    }

    /**
     * Test Case 2: Properties copied before passing - SUPERSTREAM CANNOT OPTIMIZE
     */
    private static void testPropertiesCopied() {
        logger.info("TEST CASE 2: Properties copied before passing");
        logger.info("Expected: Superstream CANNOT optimize because properties are copied");

        Properties originalProps = new Properties();
        originalProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        originalProps.put(ProducerConfig.CLIENT_ID_CONFIG, "test-copied");
        originalProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        originalProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        originalProps.put(ProducerConfig.BATCH_SIZE_CONFIG, INITIAL_BATCH_SIZE);
        originalProps.put(ProducerConfig.LINGER_MS_CONFIG, INITIAL_LINGER_MS);
        originalProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        // Create a copy of properties
        Properties copiedProps = new Properties();
        copiedProps.putAll(originalProps);

        logger.info("Before creating producer - original batch.size: {}, copied batch.size: {}",
                originalProps.get(ProducerConfig.BATCH_SIZE_CONFIG),
                copiedProps.get(ProducerConfig.BATCH_SIZE_CONFIG));

        // Pass the copied Properties to KafkaProducer
        Producer<String, String> producer = new KafkaProducer<>(copiedProps);

        // Check both original and copied properties
        logger.info("After creating producer - original batch.size: {}, copied batch.size: {}",
                originalProps.get(ProducerConfig.BATCH_SIZE_CONFIG),
                copiedProps.get(ProducerConfig.BATCH_SIZE_CONFIG));

        if (originalProps.get(ProducerConfig.BATCH_SIZE_CONFIG).equals(INITIAL_BATCH_SIZE)) {
            logger.info("❌ EXPECTED: Original properties were NOT modified (we passed a copy)");
        }

        // Note: Superstream might still optimize the copied props, but the user won't see it
        // because they're checking the original props
        logger.info("⚠️  WARNING: If user checks original props, they won't see optimizations!");

        sendTestMessage(producer, "test2-copied");
        producer.close();
    }

    /**
     * Test Case 3: Unmodifiable Map - SUPERSTREAM CANNOT OPTIMIZE
     */
    private static void testUnmodifiableMap() {
        logger.info("TEST CASE 3: Unmodifiable Map");
        logger.info("Expected: Superstream CANNOT optimize because map is unmodifiable");

        Map<String, Object> mutableMap = new HashMap<>();
        mutableMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        mutableMap.put(ProducerConfig.CLIENT_ID_CONFIG, "test-unmodifiable");
        mutableMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        mutableMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        mutableMap.put(ProducerConfig.BATCH_SIZE_CONFIG, INITIAL_BATCH_SIZE);
        mutableMap.put(ProducerConfig.LINGER_MS_CONFIG, INITIAL_LINGER_MS);
        mutableMap.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        // Make the map unmodifiable
        Map<String, Object> unmodifiableMap = Collections.unmodifiableMap(mutableMap);

        logger.info("Before creating producer - batch.size: {}",
                unmodifiableMap.get(ProducerConfig.BATCH_SIZE_CONFIG));

        try {
            // Pass unmodifiable map to KafkaProducer
            Producer<String, String> producer = new KafkaProducer<>(unmodifiableMap);

            logger.info("After creating producer - batch.size: {}",
                    unmodifiableMap.get(ProducerConfig.BATCH_SIZE_CONFIG));

            logger.info("❌ EXPECTED: Unmodifiable map prevents Superstream optimization");
            logger.info("Producer was created but without optimizations");

            sendTestMessage(producer, "test3-unmodifiable");
            producer.close();

        } catch (Exception e) {
            logger.error("⚠️  Exception occurred (might be due to Superstream trying to modify unmodifiable map): {}",
                    e.getMessage());
        }
    }

    /**
     * Test Case 4: Properties object reused - SUPERSTREAM CAN OPTIMIZE
     */
    private static void testPropertiesReused() {
        logger.info("TEST CASE 4: Properties object reused for multiple producers");
        logger.info("Expected: Superstream WILL optimize, and changes affect all producers using same props");

        Properties sharedProps = new Properties();
        sharedProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        sharedProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        sharedProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        sharedProps.put(ProducerConfig.BATCH_SIZE_CONFIG, INITIAL_BATCH_SIZE);
        sharedProps.put(ProducerConfig.LINGER_MS_CONFIG, INITIAL_LINGER_MS);
        sharedProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        // Create first producer
        sharedProps.put(ProducerConfig.CLIENT_ID_CONFIG, "test-reused-1");
        logger.info("Creating first producer - batch.size: {}",
                sharedProps.get(ProducerConfig.BATCH_SIZE_CONFIG));
        Producer<String, String> producer1 = new KafkaProducer<>(sharedProps);

        logger.info("After first producer - batch.size: {}",
                sharedProps.get(ProducerConfig.BATCH_SIZE_CONFIG));

        // Create second producer with same props (different client.id)
        sharedProps.put(ProducerConfig.CLIENT_ID_CONFIG, "test-reused-2");
        logger.info("Creating second producer with SAME props object");
        Producer<String, String> producer2 = new KafkaProducer<>(sharedProps);

        logger.info("After second producer - batch.size: {}",
                sharedProps.get(ProducerConfig.BATCH_SIZE_CONFIG));

        logger.info("⚠️  WARNING: Reusing Properties object can lead to unexpected behavior!");
        logger.info("Both producers might have different configs than expected");

        sendTestMessage(producer1, "test4-reused-1");
        sendTestMessage(producer2, "test4-reused-2");

        producer1.close();
        producer2.close();
    }

    /**
     * Test Case 5: Map passed by reference - SUPERSTREAM CAN OPTIMIZE
     */
    private static void testMapByReference() {
        logger.info("TEST CASE 5: Map passed by reference");
        logger.info("Expected: Superstream WILL optimize these properties");

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configMap.put(ProducerConfig.CLIENT_ID_CONFIG, "test-map-reference");
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.BATCH_SIZE_CONFIG, INITIAL_BATCH_SIZE);
        configMap.put(ProducerConfig.LINGER_MS_CONFIG, INITIAL_LINGER_MS);
        configMap.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        logger.info("Before creating producer - batch.size: {}, linger.ms: {}",
                configMap.get(ProducerConfig.BATCH_SIZE_CONFIG),
                configMap.get(ProducerConfig.LINGER_MS_CONFIG));

        // Pass the Map to KafkaProducer
        Producer<String, String> producer = new KafkaProducer<>(configMap);

        // Check if Superstream modified the map
        logger.info("After creating producer - batch.size: {}, linger.ms: {}",
                configMap.get(ProducerConfig.BATCH_SIZE_CONFIG),
                configMap.get(ProducerConfig.LINGER_MS_CONFIG));

        if (!configMap.get(ProducerConfig.BATCH_SIZE_CONFIG).equals(INITIAL_BATCH_SIZE)) {
            logger.info("✅ SUCCESS: Superstream optimized batch.size from {} to {}",
                    INITIAL_BATCH_SIZE, configMap.get(ProducerConfig.BATCH_SIZE_CONFIG));
        } else {
            logger.warn("❌ Map was NOT optimized (Superstream might be disabled)");
        }

        sendTestMessage(producer, "test5-map-reference");
        producer.close();
    }

    private static void sendTestMessage(Producer<String, String> producer, String testCase) {
        try {
            String key = testCase + "-key";
            String value = String.format("{\"testCase\":\"%s\",\"timestamp\":%d,\"message\":\"Testing props passing\"}",
                    testCase, System.currentTimeMillis());

            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);
            producer.send(record).get(); // Synchronous send for testing

            logger.info("Sent test message for case: {}", testCase);
        } catch (Exception e) {
            logger.error("Error sending test message for case {}: {}", testCase, e.getMessage());
        }
    }
}