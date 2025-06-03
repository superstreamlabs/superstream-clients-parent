package ai.superstream.core;

import ai.superstream.model.ClientMessage;
import ai.superstream.util.NetworkUtils;
import ai.superstream.util.SuperstreamLogger;
import ai.superstream.util.KafkaPropertiesUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ai.superstream.agent.KafkaProducerInterceptor;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Reports client information to the superstream.clients topic.
 */
public class ClientReporter {
    private static final SuperstreamLogger logger = SuperstreamLogger.getLogger(ClientReporter.class);
    private static final String CLIENTS_TOPIC = "superstream.clients";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String CLIENT_VERSION = getClientVersion();
    private static final String LANGUAGE = "Java";
    private static final String CLIENT_TYPE = "producer"; // for now support only producers

    /**
     * Report client information to the superstream.clients topic.
     *
     * @param bootstrapServers The Kafka bootstrap servers
     * @param superstreamClusterId The superstream cluster ID
     * @param active Whether the superstream optimization is active
     * @param clientId The client ID
     * @param originalConfiguration The original configuration
     * @param optimizedConfiguration The optimized configuration
     * @param mostImpactfulTopic The most impactful topic
     * @param producerUuid The producer UUID to include in the report
     * @param error The error string to include in the report
     * @return True if the message was sent successfully, false otherwise
     */
    public boolean reportClient(String bootstrapServers, Properties originalClientProperties, int superstreamClusterId, boolean active,
                                String clientId, Map<String, Object> originalConfiguration,
                                Map<String, Object> optimizedConfiguration,
                                String mostImpactfulTopic, String producerUuid,
                                String error) {
        Properties properties = new Properties();

        // Copy essential client configuration properties from the original client
        KafkaPropertiesUtils.copyClientConfigurationProperties(originalClientProperties, properties);

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaProducerInterceptor.SUPERSTREAM_LIBRARY_PREFIX + "client-reporter");

        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);  // 16KB batch size
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1000);

        // Log the configuration before creating producer
        if (SuperstreamLogger.isDebugEnabled()) {
            StringBuilder configLog = new StringBuilder("Creating internal ClientReporter producer with configuration: ");
            properties.forEach((key, value) -> {
                // Mask sensitive values
                if (key.toString().toLowerCase().contains("password") || 
                    key.toString().toLowerCase().contains("sasl.jaas.config")) {
                    configLog.append(key).append("=[MASKED], ");
                } else {
                    configLog.append(key).append("=").append(value).append(", ");
                }
            });
            // Remove trailing comma and space
            if (configLog.length() > 2) {
                configLog.setLength(configLog.length() - 2);
            }
            logger.debug(configLog.toString());
        }

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            // Create the client message
            ClientMessage message = new ClientMessage(
                    superstreamClusterId,
                    active,
                    clientId,
                    NetworkUtils.getLocalIpAddress(),
                    CLIENT_VERSION,
                    LANGUAGE,
                    CLIENT_TYPE,
                    getCompleteProducerConfig(originalConfiguration),
                    optimizedConfiguration,
                    mostImpactfulTopic,
                    NetworkUtils.getHostname(),
                    producerUuid,
                    error
            );

            // Convert the message to JSON
            String json = objectMapper.writeValueAsString(message);

            // Send the message
            ProducerRecord<String, String> record = new ProducerRecord<>(CLIENTS_TOPIC, json);
            producer.send(record);
            producer.flush();
            producer.close();

            logger.debug("Successfully reported client information to {}", CLIENTS_TOPIC);
            return true;
        } catch (Exception e) {
            // Convert stack trace to string
            java.io.StringWriter sw = new java.io.StringWriter();
            java.io.PrintWriter pw = new java.io.PrintWriter(sw);
            e.printStackTrace(pw);
            String stackTrace = sw.toString().replaceAll("\\r?\\n", " ");
            logger.error("[ERR-026] Error reporting client information. Error: {} - {}. Stack trace: {}", 
                e.getClass().getName(), e.getMessage(), stackTrace);
            return false;
        }
    }

    /**
     * Get the complete producer configuration including default values.
     * @param explicitConfig The explicitly set configuration
     * @return A map containing all configurations including defaults
     */
    private Map<String, Object> getCompleteProducerConfig(Map<String, Object> explicitConfig) {
        Map<String, Object> completeConfig = new HashMap<>();

        try {
            // Get the ProducerConfig class via reflection
            Class<?> producerConfigClass = Class.forName("org.apache.kafka.clients.producer.ProducerConfig");

            // Get access to the CONFIG static field which contains all default configurations
            Field configField = producerConfigClass.getDeclaredField("CONFIG");
            configField.setAccessible(true);
            Object configDef = configField.get(null);

            // Get the map of ConfigKey objects
            Field configKeysField = configDef.getClass().getDeclaredField("configKeys");
            configKeysField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, Object> configKeys = (Map<String, Object>) configKeysField.get(configDef);

            // For each config key, extract the default value
            for (Map.Entry<String, Object> entry : configKeys.entrySet()) {
                String configName = entry.getKey();
                Object configKey = entry.getValue();

                // Get the default value from the ConfigKey
                Field defaultValueField = configKey.getClass().getDeclaredField("defaultValue");
                defaultValueField.setAccessible(true);
                Object defaultValue = defaultValueField.get(configKey);

                if (defaultValue != null) {
                    completeConfig.put(configName, defaultValue);
                }
            }

            // Override defaults with explicitly set configurations
            completeConfig.putAll(explicitConfig);

            // Remove sensitive authentication information
            completeConfig.remove("ssl.keystore.password");
            completeConfig.remove("ssl.key.password");
            completeConfig.remove("ssl.truststore.password");
            completeConfig.remove("sasl.jaas.config");
            completeConfig.remove("sasl.client.callback.handler.class");
            completeConfig.remove("sasl.login.callback.handler.class");

        } catch (Exception e) {
            logger.warn("Failed to extract default producer configs: " + e.getMessage(), e);
        }

        // If we couldn't get any defaults, just use the explicit config
        if (completeConfig.isEmpty()) {
            return new HashMap<>(explicitConfig);
        }

        return completeConfig;
    }

    /**
     * Get the version of the Superstream Clients library.
     * @return The version string
     */
    public static String getClientVersion() {
        // Option 1: Get version from package information (MANIFEST Implementation-Version)
        Package pkg = ClientReporter.class.getPackage();
        String version = (pkg != null) ? pkg.getImplementationVersion() : null;

        // Option 2: If option 1 returns null (e.g., when running from IDE), try to read from a properties file
        if (version == null) {
            // First attempt: properties file located under META-INF (the path used during packaging)
            version = readVersionFromProperties("/META-INF/superstream-version.properties");

            // Second attempt: fallback to root path (older builds)
            if (version == null) {
                version = readVersionFromProperties("/superstream-version.properties");
            }
        }

        // Option 3: If still null, use a hard-coded fallback
        if (version == null) {
            version = ""; // Default version if not found
        }

        return version;
    }

    /**
     * Helper that tries to load the version property from the given resource path.
     *
     * @param resourcePath classpath resource path, e.g. "/META-INF/superstream-version.properties"
     * @return the version value or null if not found / unreadable
     */
    private static String readVersionFromProperties(String resourcePath) {
        try (InputStream input = ClientReporter.class.getResourceAsStream(resourcePath)) {
            if (input != null) {
                Properties props = new Properties();
                props.load(input);
                String v = props.getProperty("version");
                if (v != null && !v.trim().isEmpty()) {
                    return v.trim();
                }
            }
        } catch (IOException ignored) {
            // ignore and let caller handle fallback
        }
        return null;
    }
}