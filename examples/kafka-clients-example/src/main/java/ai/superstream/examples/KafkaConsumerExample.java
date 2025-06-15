package ai.superstream.examples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Example application that uses the Kafka Clients API to consume messages.
 * Run with:
 * java -javaagent:path/to/superstream-clients-1.0.0.jar
 * -Dlogback.configurationFile=logback.xml -jar
 * kafka-clients-example-1.0.0-jar-with-dependencies.jar
 *
 * Prerequisites:
 * 1. A Kafka server with the following topics:
 * - superstream.metadata_v1 - with a configuration message
 * - superstream.clients - for client reports
 * - example-topic - for test messages (created by KafkaProducerExample)
 *
 * Environment variables:
 * - KAFKA_BOOTSTRAP_SERVERS: The Kafka bootstrap servers (default:
 * localhost:9092)
 * - SUPERSTREAM_TOPICS_LIST: Comma-separated list of topics to optimize for
 * (default: example-topic)
 */
public class KafkaConsumerExample {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerExample.class);

    // === Configuration Constants ===
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

    private static final String CLIENT_ID = "superstream-example-consumer";
    private static final String GROUP_ID = "superstream-example-consumer-group";
    private static final String AUTO_OFFSET_RESET = "earliest"; // Start from beginning of topic
    private static final long POLL_TIMEOUT_MS = 1000; // 1 second poll timeout

    private static final String TOPIC_NAME = "example-topic";

    public static void main(String[] args) {
        logger.info("Starting Kafka Consumer Example");
        logger.info("This example will consume messages from topic: {}", TOPIC_NAME);
        logger.info("Watch for the 'hello world - KafkaConsumer intercepted!' message from the Superstream agent");

        Properties props = createConsumerProperties();
        
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // The Superstream Agent should have intercepted the consumer creation
            // and logged "hello world - KafkaConsumer intercepted!" to the console
            
            logger.info("KafkaConsumer created successfully with client.id: {}", CLIENT_ID);
            logger.info("Consumer group: {}", GROUP_ID);
            
            // Subscribe to the topic
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            logger.info("Subscribed to topic: {}", TOPIC_NAME);
            
            int messageCount = 0;
            long startTime = System.currentTimeMillis();
            
            logger.info("Starting message consumption loop...");
            
            while (true) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
                    
                    if (records.isEmpty()) {
                        logger.debug("No messages received in this poll cycle");
                        continue;
                    }
                    
                    logger.info("Received {} messages in this batch", records.count());
                    
                    for (ConsumerRecord<String, String> record : records) {
                        messageCount++;
                        
                        logger.info("Message {}: Key={}, Value length={} bytes, Partition={}, Offset={}", 
                                   messageCount, 
                                   record.key(), 
                                   record.value() != null ? record.value().length() : 0,
                                   record.partition(),
                                   record.offset());
                        
                        // Log first few characters of the message for verification
                        if (record.value() != null && record.value().length() > 0) {
                            String preview = record.value().length() > 100 
                                ? record.value().substring(0, 100) + "..." 
                                : record.value();
                            logger.debug("Message preview: {}", preview);
                        }
                        
                        // Simulate message processing time
                        Thread.sleep(100);
                    }
                    
                    // Commit offsets after processing the batch
                    consumer.commitSync();
                    logger.debug("Committed offsets for {} messages", records.count());
                    
                    // Log progress every 10 messages
                    if (messageCount % 10 == 0) {
                        long elapsedTime = System.currentTimeMillis() - startTime;
                        double messagesPerSecond = (messageCount * 1000.0) / elapsedTime;
                        logger.info("Progress: {} messages consumed in {} ms ({:.2f} msg/sec)", 
                                   messageCount, elapsedTime, messagesPerSecond);
                    }
                    
                } catch (Exception e) {
                    logger.error("Error processing messages: {}", e.getMessage(), e);
                    // Continue consuming despite errors
                }
            }
            
        } catch (Exception e) {
            logger.error("Fatal error in consumer", e);
        }
    }

    /**
     * Create consumer properties with optimal settings for the example.
     */
    private static Properties createConsumerProperties() {
        Properties props = new Properties();
        
        // Basic Kafka configuration
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        
        // Serialization
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        // Consumer behavior
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Manual commit for better control
        
        // Performance tuning
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024); // 1KB minimum fetch
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500); // Wait up to 500ms for minimum bytes
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100); // Process up to 100 records per poll
        
        // Session and heartbeat configuration
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000); // 30 seconds
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000); // 10 seconds
        
        logger.info("Consumer configuration:");
        props.stringPropertyNames().forEach(key -> {
            if (!isSensitiveProperty(key)) {
                logger.info("  {} = {}", key, props.getProperty(key));
            }
        });
        
        return props;
    }
    
    /**
     * Check if a property contains sensitive information that shouldn't be logged.
     */
    private static boolean isSensitiveProperty(String propertyName) {
        String lowerName = propertyName.toLowerCase();
        return lowerName.contains("password") || 
               lowerName.contains("secret") || 
               lowerName.contains("key") ||
               lowerName.contains("token") ||
               lowerName.contains("credential");
    }
} 