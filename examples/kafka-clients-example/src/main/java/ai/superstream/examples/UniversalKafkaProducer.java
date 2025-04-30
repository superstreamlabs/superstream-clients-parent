package ai.superstream.examples;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

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

    public static void main(String[] args) {
        // Load configuration
        Properties configProps = loadConfiguration();
        if (configProps == null) {
            logger.error("Failed to load configuration. Exiting.");
            return;
        }

        // Get producer configuration
        Properties producerProps = createProducerConfig(configProps);

        // Get topic and message details
        String topicName = configProps.getProperty("TOPIC_NAME", DEFAULT_TOPIC_NAME);
        String messageKey = configProps.getProperty("MESSAGE_KEY", DEFAULT_MESSAGE_KEY);
        String messageValue = configProps.getProperty("MESSAGE_VALUE", DEFAULT_MESSAGE_VALUE);

        // Log the configuration (but mask sensitive info)
        logger.info("Creating producer with bootstrap servers: {}", producerProps.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        logger.info("Original producer configuration:");
        producerProps.stringPropertyNames().stream()
                .filter(key -> !isSensitiveProperty(key))
                .forEach(key -> logger.info("  {} = {}", key, producerProps.getProperty(key)));

        // Create and use producer
        try (Producer<String, String> producer = new KafkaProducer<>(producerProps)) {
            // Log actual configuration after potential Superstream optimization
            logger.info("Actual producer configuration (after potential Superstream optimization):");
            logActualProducerConfig(producer);

            // Send a test message
            logger.info("Sending message to topic {}: key={}, value={}", topicName, messageKey, messageValue);
            producer.send(new ProducerRecord<>(topicName, messageKey, messageValue)).get();
            logger.info("Message sent successfully!");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while sending message", e);
        } catch (ExecutionException e) {
            logger.error("Error sending message", e);
        } catch (Exception e) {
            logger.error("Unexpected error", e);
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
            return props;
        } catch (IOException e) {
            logger.error("Failed to load configuration from {}: {}", CONFIG_FILE_PATH, e.getMessage());
            return null;
        }
    }

    /**
     * Creates Kafka producer configuration from the loaded properties
     */
    private static Properties createProducerConfig(Properties configProps) {
        Properties producerProps = new Properties();

        // Required configurations
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                configProps.getProperty("BOOTSTRAP_SERVERS"));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Client ID
        producerProps.put("client.id", configProps.getProperty("CLIENT_ID", DEFAULT_CLIENT_ID));

        // Common producer configurations
        if (configProps.containsKey("COMPRESSION_TYPE")) {
            producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, configProps.getProperty("COMPRESSION_TYPE"));
        }

        if (configProps.containsKey("BATCH_SIZE")) {
            producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.parseInt(configProps.getProperty("BATCH_SIZE")));
        }

        // Handle security configuration based on SECURITY_PROTOCOL
        String securityProtocol = configProps.getProperty("SECURITY_PROTOCOL");
        if (securityProtocol != null) {
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
        configProps.stringPropertyNames().stream()
                .filter(key -> key.startsWith("PRODUCER_"))
                .forEach(key -> {
                    String producerKey = key.substring("PRODUCER_".length()).toLowerCase().replace('_', '.');
                    producerProps.put(producerKey, configProps.getProperty(key));
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
            producerProps.put("sasl.mechanism", saslMechanism);

            // Configure JAAS based on mechanism
            switch (saslMechanism) {
                case "PLAIN":
                    // For Confluent Cloud
                    String username = configProps.getProperty("SASL_USERNAME");
                    String password = configProps.getProperty("SASL_PASSWORD");
                    if (username != null && password != null) {
                        producerProps.put("sasl.jaas.config", String.format(
                                "org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';",
                                username, password
                        ));
                    }
                    break;

                case "AWS_MSK_IAM":
                    // For AWS MSK with IAM
                    producerProps.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
                    producerProps.put("sasl.client.callback.handler.class",
                            "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

                    // Set AWS credentials if provided
                    String awsAccessKey = configProps.getProperty("AWS_ACCESS_KEY_ID");
                    String awsSecretKey = configProps.getProperty("AWS_SECRET_ACCESS_KEY");
                    if (awsAccessKey != null && awsSecretKey != null) {
                        System.setProperty("aws.accessKeyId", awsAccessKey);
                        System.setProperty("aws.secretKey", awsSecretKey);
                    }
                    break;

                default:
                    // Use custom JAAS config if provided
                    String jaasConfig = configProps.getProperty("SASL_JAAS_CONFIG");
                    if (jaasConfig != null) {
                        producerProps.put("sasl.jaas.config", jaasConfig);
                    }

                    String callbackHandler = configProps.getProperty("SASL_CALLBACK_HANDLER");
                    if (callbackHandler != null) {
                        producerProps.put("sasl.client.callback.handler.class", callbackHandler);
                    }
            }
        }
    }

    /**
     * Configures SSL/TLS settings
     */
    private static void configureSsl(Properties producerProps, Properties configProps) {
        // Truststore settings
        String truststoreLocation = configProps.getProperty("SSL_TRUSTSTORE_LOCATION");
        String truststorePassword = configProps.getProperty("SSL_TRUSTSTORE_PASSWORD");
        String truststoreType = configProps.getProperty("SSL_TRUSTSTORE_TYPE", "JKS");

        if (truststoreLocation != null) {
            producerProps.put("ssl.truststore.location", truststoreLocation);
            if (truststorePassword != null) {
                producerProps.put("ssl.truststore.password", truststorePassword);
            }
            producerProps.put("ssl.truststore.type", truststoreType);
        }

        // Keystore settings
        String keystoreLocation = configProps.getProperty("SSL_KEYSTORE_LOCATION");
        String keystorePassword = configProps.getProperty("SSL_KEYSTORE_PASSWORD");
        String keystoreType = configProps.getProperty("SSL_KEYSTORE_TYPE", "PKCS12");

        if (keystoreLocation != null) {
            producerProps.put("ssl.keystore.location", keystoreLocation);
            if (keystorePassword != null) {
                producerProps.put("ssl.keystore.password", keystorePassword);
            }
            producerProps.put("ssl.keystore.type", keystoreType);
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
