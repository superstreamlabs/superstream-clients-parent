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
 * Environment variables: - DEBUG_CLIENTS: Set to "true" to enable verbose
 * logging (default: false)
 *
 * Prerequisites: 1. A Kafka cluster configuration in /tmp/cluster.conf.env 2.
 * Topics required: - superstream.metadata_v1 - with a configuration message -
 * superstream.clients - for client reports - example-topic (or as configured) -
 * for test messages
 */
public class UniversalKafkaProducer {

    private static final Logger logger = LoggerFactory.getLogger(UniversalKafkaProducer.class);

    // Logging control flag - checks DEBUG_CLIENTS environment variable
    private static final boolean DEBUG_MODE = Boolean.parseBoolean(
            System.getenv().getOrDefault("DEBUG_CLIENTS", "false"));

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

    // Message counter for stats
    private static final AtomicInteger TOTAL_MESSAGES_SENT = new AtomicInteger(0);
    private static final AtomicInteger TOTAL_ERRORS = new AtomicInteger(0);

    public static void main(String[] args) {
        info("Starting Universal Kafka Producer" + (DEBUG_MODE ? " in DEBUG mode" : ""));
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
                error("Configuration file not found at: {}", CONFIG_FILE_PATH);
                return;
            }

            // Load configuration
            Properties configProps = loadConfiguration();
            if (configProps == null) {
                error("Failed to load configuration. Exiting.");
                return;
            }

            // Get producer configuration
            logger.info("Creating producer configuration");
            Properties producerProps = createProducerConfig(configProps);
            if (producerProps == null) {
                error("Failed to create producer configuration. Exiting.");
                return;
            }

            // Get topic and message details
            String topicName = configProps.getProperty("TOPIC_NAME", DEFAULT_TOPIC_NAME);
            String messageKey = configProps.getProperty("MESSAGE_KEY", DEFAULT_MESSAGE_KEY);
            String messageValue = configProps.getProperty("MESSAGE_VALUE", DEFAULT_MESSAGE_VALUE);

            // Log essential configuration
            logger.info("Connecting to bootstrap servers: {}",
                    producerProps.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
            logger.info("Target topic: {}", topicName);

            // Create the Kafka producer
            logger.info("Initializing Kafka producer");
            final AtomicInteger messageCounter = new AtomicInteger(0);

            try (Producer<String, String> producer = new KafkaProducer<>(producerProps)) {
                // Log actual configuration after potential Superstream optimization
                debug("Producer configuration after Superstream optimization:");
                logActualProducerConfig(producer);

                // Send messages
                if (RUN_INDEFINITELY) {
                    logger.info("Starting continuous message sending every {} seconds", MESSAGE_FREQUENCY_SECONDS);
                    runContinuously(producer, topicName, messageKey, messageValue, messageCounter);
                } else {
                    logger.info("Sending a single test message");
                    sendTestMessage(producer, topicName, messageKey, messageValue);
                }
            } catch (RuntimeException e) {
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

        // Also schedule periodic status reports
        ScheduledExecutorService statsReporter = Executors.newScheduledThreadPool(1);
        statsReporter.scheduleAtFixedRate(() -> {
            info("Status: {} messages sent successfully, {} errors occurred",
                    TOTAL_MESSAGES_SENT.get(), TOTAL_ERRORS.get());
        }, 60, 60, TimeUnit.SECONDS);

        Runnable messageSender = () -> {
            try {
                String message = messageValue + " #" + counter.incrementAndGet();
                debug("Sending scheduled message #{}", counter.get());
                producer.send(new ProducerRecord<>(topicName, messageKey, message)).get();
                TOTAL_MESSAGES_SENT.incrementAndGet();
                debug("Message #{} sent successfully", counter.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                error("Interrupted while sending scheduled message", e);
                TOTAL_ERRORS.incrementAndGet();
            } catch (ExecutionException e) {
                error("Error sending scheduled message: {}", e.getMessage(), e);
                TOTAL_ERRORS.incrementAndGet();
            } catch (Exception e) {
                error("Unexpected error in scheduled task", e);
                TOTAL_ERRORS.incrementAndGet();
            }
        };

        // Schedule the task to run periodically
        scheduler.scheduleAtFixedRate(messageSender, MESSAGE_FREQUENCY_SECONDS, MESSAGE_FREQUENCY_SECONDS, TimeUnit.SECONDS);

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down producer...");
            scheduler.shutdown();
            statsReporter.shutdown();

            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.warn("Scheduler did not terminate in time");
                }
                if (!statsReporter.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.warn("Stats reporter did not terminate in time");
                }
            } catch (InterruptedException e) {
                logger.error("Shutdown interrupted", e);
                Thread.currentThread().interrupt();
            }

            logger.info("Producer shutdown complete. Final stats: {} messages sent, {} errors occurred",
                    TOTAL_MESSAGES_SENT.get(), TOTAL_ERRORS.get());
        }));

        logger.info("Producer is running indefinitely. Press Ctrl+C to stop.");

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
            debug("Sending message to topic {}: {}", topicName, messageValue);
            producer.send(new ProducerRecord<>(topicName, messageKey, messageValue)).get();
            TOTAL_MESSAGES_SENT.incrementAndGet();
            debug("Message sent successfully!");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            error("Interrupted while sending message", e);
            TOTAL_ERRORS.incrementAndGet();
        } catch (ExecutionException e) {
            error("Error sending message: {}", e.getMessage());
            TOTAL_ERRORS.incrementAndGet();
            Throwable cause = e.getCause();
            if (cause != null) {
                error("Caused by: {}", cause.getMessage());
            }
        } catch (Exception e) {
            error("Unexpected error sending message", e);
            TOTAL_ERRORS.incrementAndGet();
        }
    }

    /**
     * Loads configuration from the config file
     */
    private static Properties loadConfiguration() {
        Properties props = new Properties();

        try (FileInputStream fis = new FileInputStream(CONFIG_FILE_PATH)) {
            props.load(fis);
            debug("Loaded configuration from {}", CONFIG_FILE_PATH);
            debug("Configuration contains {} properties", props.size());

            // Log non-sensitive properties
            props.stringPropertyNames().stream()
                    .filter(key -> !isSensitiveProperty(key))
                    .forEach(key -> logger.debug("  Config: {} = {}", key, props.getProperty(key)));

            return props;
        } catch (IOException e) {
            error("Failed to load configuration from {}: {}", CONFIG_FILE_PATH, e.getMessage());
            return null;
        }
    }

    /**
     * Creates Kafka producer configuration from the loaded properties
     */
    private static Properties createProducerConfig(Properties configProps) {
        Properties producerProps = new Properties();

        if (configProps == null) {
            error("Configuration properties are null, cannot create producer config");
            return null;
        }

        // Log all available keys to help with debugging
        logger.info("Available configuration keys:");
        configProps.stringPropertyNames().forEach(key -> logger.info("  {}", key));

        // Required configurations
        String bootstrapServers = configProps.getProperty("BOOTSTRAP_SERVERS");
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            error("BOOTSTRAP_SERVERS is not defined in configuration");
            return null;
        }

        debug("Using bootstrap servers: {}", bootstrapServers);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Client ID
        String clientId = configProps.getProperty("CLIENT_ID", DEFAULT_CLIENT_ID);
        producerProps.put("client.id", clientId);
        logger.info("Using client ID: {}", clientId);

        // Common producer configurations
        if (configProps.containsKey("COMPRESSION_TYPE")) {
            try {
                producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, configProps.getProperty("COMPRESSION_TYPE"));
                debug("Using compression: {}", configProps.getProperty("COMPRESSION_TYPE"));
            } catch (NumberFormatException e) {
                warn("Invalid COMPRESSION_TYPE value: {}", configProps.getProperty("COMPRESSION_TYPE"));
            }
        }

        if (configProps.containsKey("BATCH_SIZE")) {
            try {
                producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.valueOf(configProps.getProperty("BATCH_SIZE")));
                debug("Using batch size: {}", configProps.getProperty("BATCH_SIZE"));
            } catch (NumberFormatException e) {
                warn("Invalid BATCH_SIZE value: {}", configProps.getProperty("BATCH_SIZE"));
            }
        }

        // Handle security configuration based on SECURITY_PROTOCOL
        String securityProtocol = configProps.getProperty("SECURITY_PROTOCOL");
        if (securityProtocol != null) {
            // Fix for "No authentication" value - use PLAINTEXT instead
            if (securityProtocol.equals("No authentication")) {
                debug("Security protocol 'No authentication' detected, using PLAINTEXT instead");
                securityProtocol = "PLAINTEXT";
            }

            debug("Configuring security with protocol: {}", securityProtocol);
            producerProps.put("security.protocol", securityProtocol);

            // Configure SASL if needed
            if (securityProtocol.contains("SASL")) {
                configureSasl(producerProps, configProps);
            }

            // Configure SSL if needed
            if (securityProtocol.contains("SSL")) {
                configureSsl(producerProps, configProps);
            }
        } else {
            // Default to PLAINTEXT for local development with no authentication
            debug("No security protocol specified, defaulting to PLAINTEXT");
            producerProps.put("security.protocol", "PLAINTEXT");
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
            debug("Configuring SASL with mechanism: {}", saslMechanism);
            producerProps.put("sasl.mechanism", saslMechanism);

            // Configure JAAS based on mechanism
            switch (saslMechanism) {
                case "PLAIN":
                    // For Confluent Cloud
                    String username = configProps.getProperty("SASL_USERNAME");
                    String password = configProps.getProperty("SASL_PASSWORD");
                    if (username != null && password != null) {
                        debug("Configuring PLAIN authentication");
                        producerProps.put("sasl.jaas.config", String.format(
                                "org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';",
                                username, password
                        ));
                    } else {
                        error("SASL_USERNAME and/or SASL_PASSWORD not provided for PLAIN mechanism");
                    }
                    break;

                case "AWS_MSK_IAM":
                    // For AWS MSK with IAM
                    debug("Configuring AWS MSK IAM authentication");

                    // When using AWS MSK IAM, the SASL_USERNAME is the AWS access key and
                    // SASL_PASSWORD is the AWS secret key
                    String awsAccessKey = configProps.getProperty("SASL_USERNAME");
                    String awsSecretKey = configProps.getProperty("SASL_PASSWORD");

                    if (awsAccessKey != null && !awsAccessKey.isEmpty()
                            && awsSecretKey != null && !awsSecretKey.isEmpty()) {
                        info("Using provided AWS credentials for IAM authentication");

                        // Direct AWS SDK system properties for credentials
                        System.setProperty("aws.accessKeyId", awsAccessKey);
                        System.setProperty("aws.secretKey", awsSecretKey);

                        // Also set as environment variables for the AWS SDK
                        setenv("AWS_ACCESS_KEY_ID", awsAccessKey);
                        setenv("AWS_SECRET_ACCESS_KEY", awsSecretKey);

                        debug("AWS credentials set from SASL_USERNAME/PASSWORD");
                    } else {
                        // Try alternative property names
                        awsAccessKey = configProps.getProperty("AWS_ACCESS_KEY_ID");
                        awsSecretKey = configProps.getProperty("AWS_SECRET_ACCESS_KEY");

                        if (awsAccessKey != null && !awsAccessKey.isEmpty()
                                && awsSecretKey != null && !awsSecretKey.isEmpty()) {
                            info("Using provided AWS credentials from AWS_* properties");

                            // Direct AWS SDK system properties for credentials
                            System.setProperty("aws.accessKeyId", awsAccessKey);
                            System.setProperty("aws.secretKey", awsSecretKey);

                            // Also set as environment variables for the AWS SDK
                            setenv("AWS_ACCESS_KEY_ID", awsAccessKey);
                            setenv("AWS_SECRET_ACCESS_KEY", awsSecretKey);

                            debug("AWS credentials set from AWS_* properties");
                        } else {
                            warn("No AWS credentials provided. Authentication will likely fail unless running in an AWS environment with instance profile.");
                        }
                    }

                    // Check if credentials are available in the environment
                    if (System.getenv("AWS_ACCESS_KEY_ID") != null
                            && System.getenv("AWS_SECRET_ACCESS_KEY") != null) {
                        debug("AWS credentials found in environment variables");
                    }

                    // Add AWS Region if available
                    String awsRegion = configProps.getProperty("AWS_REGION");
                    if (awsRegion != null && !awsRegion.isEmpty()) {
                        debug("Setting AWS region: {}", awsRegion);
                        System.setProperty("aws.region", awsRegion);
                    }

                    // Standard MSK IAM configuration
                    producerProps.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
                    producerProps.put("sasl.client.callback.handler.class",
                            "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

                    // Enable AWS SDK logging if in debug mode
                    if (DEBUG_MODE) {
                        System.setProperty("org.apache.commons.logging.Log",
                                "org.apache.commons.logging.impl.SimpleLog");
                        System.setProperty("org.apache.commons.logging.simplelog.showdatetime", "true");
                        System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http", "DEBUG");
                        System.setProperty("org.apache.commons.logging.simplelog.log.com.amazonaws", "DEBUG");
                    }
                    break;

                default:
                    // Use custom JAAS config if provided
                    String jaasConfig = configProps.getProperty("SASL_JAAS_CONFIG");
                    if (jaasConfig != null) {
                        producerProps.put("sasl.jaas.config", jaasConfig);
                    } else {
                        warn("No SASL_JAAS_CONFIG provided for mechanism: {}", saslMechanism);
                    }

                    String callbackHandler = configProps.getProperty("SASL_CALLBACK_HANDLER");
                    if (callbackHandler != null) {
                        producerProps.put("sasl.client.callback.handler.class", callbackHandler);
                    }
            }
        } else {
            error("SASL_MECHANISM not provided for SASL authentication");
        }
    }

    /**
     * Configures SSL/TLS settings
     */
    private static void configureSsl(Properties producerProps, Properties configProps) {
        debug("Configuring SSL/TLS settings");

        // Truststore settings
        String truststoreLocation = configProps.getProperty("SSL_TRUSTSTORE_LOCATION");
        String truststorePassword = configProps.getProperty("SSL_TRUSTSTORE_PASSWORD");
        String truststoreType = configProps.getProperty("SSL_TRUSTSTORE_TYPE", "JKS");

        if (truststoreLocation != null) {
            debug("Setting SSL truststore: {}", truststoreLocation);

            // Check if truststore file exists
            if (!Files.exists(Paths.get(truststoreLocation))) {
                error("Truststore file does not exist: {}", truststoreLocation);
            }

            producerProps.put("ssl.truststore.location", truststoreLocation);
            if (truststorePassword != null) {
                producerProps.put("ssl.truststore.password", truststorePassword);
            } else {
                warn("SSL_TRUSTSTORE_PASSWORD not provided");
            }
            producerProps.put("ssl.truststore.type", truststoreType);
        } else {
            warn("SSL_TRUSTSTORE_LOCATION not provided");
        }

        // Keystore settings
        String keystoreLocation = configProps.getProperty("SSL_KEYSTORE_LOCATION");
        String keystorePassword = configProps.getProperty("SSL_KEYSTORE_PASSWORD");
        String keystoreType = configProps.getProperty("SSL_KEYSTORE_TYPE", "PKCS12");

        if (keystoreLocation != null) {
            debug("Setting SSL keystore: {}", keystoreLocation);

            // Check if keystore file exists
            if (!Files.exists(Paths.get(keystoreLocation))) {
                error("Keystore file does not exist: {}", keystoreLocation);
            }

            producerProps.put("ssl.keystore.location", keystoreLocation);
            if (keystorePassword != null) {
                producerProps.put("ssl.keystore.password", keystorePassword);
            } else {
                warn("SSL_KEYSTORE_PASSWORD not provided");
            }
            producerProps.put("ssl.keystore.type", keystoreType);
        } else {
            debug("SSL_KEYSTORE_LOCATION not provided, client authentication not enabled");
        }
    }

    /**
     * Logs the actual producer configuration after potential optimization
     */
    private static void logActualProducerConfig(Producer<String, String> producer) {
        try {
            if (!DEBUG_MODE) {
                return;
            }

            java.lang.reflect.Field configField = producer.getClass().getDeclaredField("producerConfig");
            configField.setAccessible(true);
            org.apache.kafka.clients.producer.ProducerConfig actualConfig
                    = (org.apache.kafka.clients.producer.ProducerConfig) configField.get(producer);

            logger.info("  compression.type = {}", actualConfig.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG));
            logger.info("  batch.size = {}", actualConfig.getInt(ProducerConfig.BATCH_SIZE_CONFIG));
            logger.info("  linger.ms = {}", actualConfig.getString(ProducerConfig.LINGER_MS_CONFIG));
            logger.info("  acks = {}", actualConfig.getString(ProducerConfig.ACKS_CONFIG));
        } catch (NoSuchFieldException | IllegalAccessException | ClassCastException e) {
            warn("Could not access producer configuration details: {}", e.getMessage());
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

    // Logging utility methods that respect the DEBUG_MODE flag
    /**
     * Log info message - these are always shown
     */
    private static void info(String message, Object... args) {
        logger.info(message, args);
    }

    /**
     * Log debug message - only shown when DEBUG_MODE is true
     */
    private static void debug(String message, Object... args) {
        if (DEBUG_MODE) {
            logger.info(message, args);
        }
    }

    /**
     * Log warning message - always shown
     */
    private static void warn(String message, Object... args) {
        logger.warn(message, args);
    }

    /**
     * Log error message - always shown
     */
    private static void error(String message, Object... args) {
        logger.error(message, args);
        // If the last argument is an exception, don't count it for formatting
        if (args.length > 0 && args[args.length - 1] instanceof Throwable) {
            TOTAL_ERRORS.incrementAndGet();
        } else {
            TOTAL_ERRORS.incrementAndGet();
        }
    }

    // Add a helper method to set environment variables, since System.setenv() doesn't exist
    private static void setenv(String key, String value) {
        try {
            java.lang.reflect.Field field = System.class.getDeclaredField("env");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            java.util.Map<String, String> env = (java.util.Map<String, String>) field.get(null);
            java.util.Map<String, String> writableEnv = new java.util.HashMap<>(env);
            writableEnv.put(key, value);
            field.set(null, writableEnv);
        } catch (NoSuchFieldException | IllegalAccessException | SecurityException e) {
            warn("Could not set environment variable {}: {}", key, e.getMessage());
        }
    }
}
