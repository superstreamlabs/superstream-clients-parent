package ai.superstream.examples;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Universal Kafka Producer Example that can connect to any Kafka vendor. Reads
 * configuration from /tmp/cluster.conf.env file.
 *
 * Run with: java -javaagent:path/to/superstream-clients-1.0.0.jar
 * -Dlogback.configurationFile=logback.xml -jar
 * kafka-clients-example-1.0.0-jar-with-dependencies.jar
 *
 * Prerequisites: 1. A Kafka cluster configuration in /tmp/cluster.conf.env 2.
 * Topics required: - superstream.metadata_v1 - with a configuration message -
 * superstream.clients - for client reports - example-topic (or as configured) -
 * for test messages
 */
public class UniversalKafkaProducer {

    private static final Logger logger = LoggerFactory.getLogger(UniversalKafkaProducer.class);

    // Default configuration file location
    private static final String CONFIG_FILE_PATH = "/tmp/cluster.conf.env";

    // Default values
    private static final String DEFAULT_CLIENT_ID = "superstream-example-producer";
    private static final String DEFAULT_TOPIC_NAME = "example-topic";
    private static final String DEFAULT_MESSAGE_KEY = "test-key";
    private static final String DEFAULT_MESSAGE_VALUE = "Hello, Superstream!";

    // Message sending frequency in seconds
    private static final int MESSAGE_FREQUENCY_SECONDS = 30;

    // Whether to run indefinitely
    private static final boolean RUN_INDEFINITELY = true;

    public static void main(String[] args) {
        logger.info("Starting Universal Kafka Producer");
        logger.info("JVM version: {}", System.getProperty("java.version"));
        logger.info("Available processors: {}", Runtime.getRuntime().availableProcessors());
        logger.info("Max memory: {} MB", Runtime.getRuntime().maxMemory() / (1024 * 1024));

        try {
            // Print all environment variables
            logger.info("Environment variables:");
            System.getenv().forEach((key, value) -> {
                if (!key.toLowerCase().contains("password") && !key.toLowerCase().contains("secret")) {
                    logger.info("  {} = {}", key, value);
                }
            });

            // Print all system properties
            logger.info("System properties:");
            System.getProperties().forEach((key, value) -> {
                logger.info("  {} = {}", key, value);
            });

            // Check if config file exists
            if (!Files.exists(Paths.get(CONFIG_FILE_PATH))) {
                logger.error("Configuration file not found at: {}", CONFIG_FILE_PATH);
                return;
            }

            // Load configuration
            Properties configProps = loadConfiguration();
            if (configProps == null) {
                logger.error("Failed to load configuration. Exiting.");
                return;
            }

            // Get producer configuration
            logger.info("Creating producer configuration");
            Properties producerProps = createProducerConfig(configProps);
            if (producerProps == null) {
                logger.error("Failed to create producer configuration. Exiting.");
                return;
            }

            // Get topic and message details
            String topicName = configProps.getProperty("TOPIC_NAME", DEFAULT_TOPIC_NAME);
            String messageKey = configProps.getProperty("MESSAGE_KEY", DEFAULT_MESSAGE_KEY);
            String messageValue = configProps.getProperty("MESSAGE_VALUE", DEFAULT_MESSAGE_VALUE);

            // Log the configuration (but mask sensitive info)
            logger.info("Creating producer with bootstrap servers: {}",
                    producerProps.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
            logger.info("Original producer configuration:");
            producerProps.stringPropertyNames().stream()
                    .filter(key -> !isSensitiveProperty(key))
                    .forEach(key -> logger.info("  {} = {}", key, producerProps.getProperty(key)));

            // Create the Kafka producer
            logger.info("Initializing Kafka producer");
            final AtomicInteger messageCounter = new AtomicInteger(0);

            try (Producer<String, String> producer = new KafkaProducer<>(producerProps)) {
                // Log actual configuration after potential Superstream optimization
                logger.info("Actual producer configuration (after potential Superstream optimization):");
                logActualProducerConfig(producer);

                // Send messages
                if (RUN_INDEFINITELY) {
                    logger.info("Starting continuous message sending every {} seconds", MESSAGE_FREQUENCY_SECONDS);
                    runContinuously(producer, topicName, messageKey, messageValue, messageCounter);
                } else {
                    logger.info("Sending a single test message");
                    sendTestMessage(producer, topicName, messageKey, messageValue);
                }
            } catch (Exception e) {
                logger.error("Fatal error with Kafka producer", e);
            }
        } catch (Exception e) {
            logger.error("Unexpected error in main method", e);
        }
    }

    /**
     * Sends test messages continuously at fixed intervals
     */
    private static void runContinuously(Producer<String, String> producer, String topicName,
            String messageKey, String messageValue, AtomicInteger counter) {
        // Send initial message immediately
        sendTestMessage(producer, topicName, messageKey, messageValue + " #" + counter.incrementAndGet());

        // Schedule periodic message sending
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        Runnable messageSender = () -> {
            try {
                String message = messageValue + " #" + counter.incrementAndGet();
                logger.info("Sending scheduled message: {}", message);
                producer.send(new ProducerRecord<>(topicName, messageKey, message)).get();
                logger.info("Scheduled message sent successfully!");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Interrupted while sending scheduled message", e);
            } catch (ExecutionException e) {
                logger.error("Error sending scheduled message", e);
            } catch (Exception e) {
                logger.error("Unexpected error in scheduled task", e);
            }
        };

        // Schedule the task to run periodically
        scheduler.scheduleAtFixedRate(messageSender, MESSAGE_FREQUENCY_SECONDS, MESSAGE_FREQUENCY_SECONDS, TimeUnit.SECONDS);

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down producer...");
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.warn("Scheduler did not terminate in time");
                }
            } catch (InterruptedException e) {
                logger.error("Shutdown interrupted", e);
                Thread.currentThread().interrupt();
            }
            logger.info("Producer shutdown complete");
        }));

        logger.info("Producer will run indefinitely. Press Ctrl+C to stop.");

        // Keep the main thread alive
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("Main thread interrupted, exiting");
        }
    }

    /**
     * Sends a single test message
     */
    private static void sendTestMessage(Producer<String, String> producer, String topicName,
            String messageKey, String messageValue) {
        try {
            logger.info("Sending message to topic {}: key={}, value={}", topicName, messageKey, messageValue);
            producer.send(new ProducerRecord<>(topicName, messageKey, messageValue)).get();
            logger.info("Message sent successfully!");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while sending message", e);
        } catch (ExecutionException e) {
            logger.error("Error sending message: {}", e.getMessage(), e);
            Throwable cause = e.getCause();
            if (cause != null) {
                logger.error("Caused by: {}", cause.getMessage(), cause);
            }
        } catch (Exception e) {
            logger.error("Unexpected error sending message", e);
        }
    }

    /**
     * Loads configuration from the config file
     */
    private static Properties loadConfiguration() {
        Properties props = new Properties();

        try (FileInputStream fis = new FileInputStream(CONFIG_FILE_PATH)) {
            props.load(fis);
            logger.info("Loaded configuration from {}", CONFIG_FILE_PATH);
            logger.info("Configuration contains {} properties", props.size());

            // Log non-sensitive properties
            props.stringPropertyNames().stream()
                    .filter(key -> !isSensitiveProperty(key))
                    .forEach(key -> logger.debug("  Config: {} = {}", key, props.getProperty(key)));

            return props;
        } catch (IOException e) {
            logger.error("Failed to load configuration from {}: {}", CONFIG_FILE_PATH, e.getMessage(), e);
            return null;
        }
    }

    /**
     * Creates Kafka producer configuration from the loaded properties
     */
    private static Properties createProducerConfig(Properties configProps) {
        Properties producerProps = new Properties();

        if (configProps == null) {
            logger.error("Configuration properties are null, cannot create producer config");
            return null;
        }

        // Log all available keys to help with debugging
        logger.info("Available configuration keys:");
        configProps.stringPropertyNames().forEach(key -> logger.info("  {}", key));

        // Required configurations
        String bootstrapServers = configProps.getProperty("BOOTSTRAP_SERVERS");
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            logger.error("BOOTSTRAP_SERVERS is not defined in configuration");
            return null;
        }

        logger.info("Using bootstrap servers: {}", bootstrapServers);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Client ID
        String clientId = configProps.getProperty("CLIENT_ID", DEFAULT_CLIENT_ID);
        producerProps.put("client.id", clientId);
        logger.info("Using client ID: {}", clientId);

        // Common producer configurations
        if (configProps.containsKey("COMPRESSION_TYPE")) {
            producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, configProps.getProperty("COMPRESSION_TYPE"));
        }

        if (configProps.containsKey("BATCH_SIZE")) {
            try {
                producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.parseInt(configProps.getProperty("BATCH_SIZE")));
            } catch (NumberFormatException e) {
                logger.warn("Invalid BATCH_SIZE value: {}", configProps.getProperty("BATCH_SIZE"));
            }
        }

        // Handle security configuration based on SECURITY_PROTOCOL
        String securityProtocol = configProps.getProperty("SECURITY_PROTOCOL");
        if (securityProtocol != null) {
            logger.info("Configuring security with protocol: {}", securityProtocol);
            producerProps.put("security.protocol", securityProtocol);

            // Configure SASL if needed
            if (securityProtocol.contains("SASL")) {
                configureSasl(producerProps, configProps);
            }

            // Configure SSL if needed
            if (securityProtocol.contains("SSL")) {
                configureSsl(producerProps, configProps);
            }
        }

        // Copy any additional producer properties with "PRODUCER_" prefix
        logger.info("Looking for additional PRODUCER_ properties");
        configProps.stringPropertyNames().stream()
                .filter(key -> key.startsWith("PRODUCER_"))
                .forEach(key -> {
                    String producerKey = key.substring("PRODUCER_".length()).toLowerCase().replace('_', '.');
                    producerProps.put(producerKey, configProps.getProperty(key));
                    logger.info("  Added: {} = {}", producerKey, configProps.getProperty(key));
                });

        return producerProps;
    }

    /**
     * Configures SASL authentication
     */
    private static void configureSasl(Properties producerProps, Properties configProps) {
        // Get SASL mechanism
        String saslMechanism = configProps.getProperty("SASL_MECHANISM");
        if (saslMechanism != null) {
            logger.info("Configuring SASL with mechanism: {}", saslMechanism);
            producerProps.put("sasl.mechanism", saslMechanism);

            // Configure JAAS based on mechanism
            switch (saslMechanism) {
                case "PLAIN":
                    // For Confluent Cloud
                    String username = configProps.getProperty("SASL_USERNAME");
                    String password = configProps.getProperty("SASL_PASSWORD");
                    if (username != null && password != null) {
                        logger.info("Configuring PLAIN authentication with username: {}", username);
                        producerProps.put("sasl.jaas.config", String.format(
                                "org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';",
                                username, password
                        ));
                    } else {
                        logger.error("SASL_USERNAME and/or SASL_PASSWORD not provided for PLAIN mechanism");
                    }
                    break;

                case "AWS_MSK_IAM":
                    // For AWS MSK with IAM
                    logger.info("Configuring AWS MSK IAM authentication");
                    producerProps.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
                    producerProps.put("sasl.client.callback.handler.class",
                            "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

                    // Set AWS credentials if provided
                    String awsAccessKey = configProps.getProperty("AWS_ACCESS_KEY_ID");
                    String awsSecretKey = configProps.getProperty("AWS_SECRET_ACCESS_KEY");
                    if (awsAccessKey != null && awsSecretKey != null) {
                        logger.info("Setting AWS credentials");
                        System.setProperty("aws.accessKeyId", awsAccessKey);
                        System.setProperty("aws.secretKey", awsSecretKey);
                    } else {
                        logger.info("AWS credentials not provided, using instance profile or environment variables");
                    }
                    break;

                default:
                    // Use custom JAAS config if provided
                    logger.info("Using custom SASL configuration for mechanism: {}", saslMechanism);
                    String jaasConfig = configProps.getProperty("SASL_JAAS_CONFIG");
                    if (jaasConfig != null) {
                        producerProps.put("sasl.jaas.config", jaasConfig);
                    } else {
                        logger.warn("No SASL_JAAS_CONFIG provided for mechanism: {}", saslMechanism);
                    }

                    String callbackHandler = configProps.getProperty("SASL_CALLBACK_HANDLER");
                    if (callbackHandler != null) {
                        producerProps.put("sasl.client.callback.handler.class", callbackHandler);
                    }
            }
        } else {
            logger.error("SASL_MECHANISM not provided for SASL authentication");
        }
    }

    /**
     * Configures SSL/TLS settings
     */
    private static void configureSsl(Properties producerProps, Properties configProps) {
        logger.info("Configuring SSL/TLS settings");

        // Truststore settings
        String truststoreLocation = configProps.getProperty("SSL_TRUSTSTORE_LOCATION");
        String truststorePassword = configProps.getProperty("SSL_TRUSTSTORE_PASSWORD");
        String truststoreType = configProps.getProperty("SSL_TRUSTSTORE_TYPE", "JKS");

        if (truststoreLocation != null) {
            logger.info("Setting SSL truststore: {}", truststoreLocation);

            // Check if truststore file exists
            if (!Files.exists(Paths.get(truststoreLocation))) {
                logger.error("Truststore file does not exist: {}", truststoreLocation);
            }

            producerProps.put("ssl.truststore.location", truststoreLocation);
            if (truststorePassword != null) {
                producerProps.put("ssl.truststore.password", truststorePassword);
            } else {
                logger.warn("SSL_TRUSTSTORE_PASSWORD not provided");
            }
            producerProps.put("ssl.truststore.type", truststoreType);
        } else {
            logger.warn("SSL_TRUSTSTORE_LOCATION not provided");
        }

        // Keystore settings
        String keystoreLocation = configProps.getProperty("SSL_KEYSTORE_LOCATION");
        String keystorePassword = configProps.getProperty("SSL_KEYSTORE_PASSWORD");
        String keystoreType = configProps.getProperty("SSL_KEYSTORE_TYPE", "PKCS12");

        if (keystoreLocation != null) {
            logger.info("Setting SSL keystore: {}", keystoreLocation);

            // Check if keystore file exists
            if (!Files.exists(Paths.get(keystoreLocation))) {
                logger.error("Keystore file does not exist: {}", keystoreLocation);
            }

            producerProps.put("ssl.keystore.location", keystoreLocation);
            if (keystorePassword != null) {
                producerProps.put("ssl.keystore.password", keystorePassword);
            } else {
                logger.warn("SSL_KEYSTORE_PASSWORD not provided");
            }
            producerProps.put("ssl.keystore.type", keystoreType);
        } else {
            logger.debug("SSL_KEYSTORE_LOCATION not provided, client authentication not enabled");
        }
    }

    /**
     * Logs the actual producer configuration after potential optimization
     */
    private static void logActualProducerConfig(Producer<String, String> producer) {
        try {
            java.lang.reflect.Field configField = producer.getClass().getDeclaredField("producerConfig");
            configField.setAccessible(true);
            org.apache.kafka.clients.producer.ProducerConfig actualConfig
                    = (org.apache.kafka.clients.producer.ProducerConfig) configField.get(producer);

            logger.info("  compression.type = {}", actualConfig.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG));
            logger.info("  batch.size = {}", actualConfig.getInt(ProducerConfig.BATCH_SIZE_CONFIG));
            logger.info("  buffer.memory = {}", actualConfig.getString(ProducerConfig.BUFFER_MEMORY_CONFIG));
            logger.info("  linger.ms = {}", actualConfig.getString(ProducerConfig.LINGER_MS_CONFIG));
            logger.info("  max.in.flight.requests.per.connection = {}",
                    actualConfig.getString(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION));
            logger.info("  metadata.max.age.ms = {}", actualConfig.getString(ProducerConfig.METADATA_MAX_AGE_CONFIG));
            logger.info("  acks = {}", actualConfig.getString(ProducerConfig.ACKS_CONFIG));
        } catch (Exception e) {
            logger.warn("Failed to access actual producer configuration: {}", e.getMessage());
        }
    }

    /**
     * Checks if a property contains sensitive information
     */
    private static boolean isSensitiveProperty(String propertyName) {
        return propertyName.toLowerCase().contains("password")
                || propertyName.toLowerCase().contains("secret")
                || propertyName.toLowerCase().contains("key")
                || propertyName.toLowerCase().contains("token");
    }
}
