package ai.superstream.core;

import ai.superstream.model.MetadataMessage;
import ai.superstream.util.SuperstreamLogger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Main manager class for the Superstream library.
 */
public class SuperstreamManager {
    private static final SuperstreamLogger logger = SuperstreamLogger.getLogger(SuperstreamManager.class);
    private static final String TOPICS_ENV_VAR = "SUPERSTREAM_TOPICS_LIST";
    private static final String DISABLED_ENV_VAR = "SUPERSTREAM_DISABLED";
    private static final ThreadLocal<Boolean> OPTIMIZATION_IN_PROGRESS = new ThreadLocal<>();
    private static volatile SuperstreamManager instance;

    private final MetadataConsumer metadataConsumer;
    private final ClientReporter clientReporter;
    private final ConfigurationOptimizer configurationOptimizer;
    private final Map<String, MetadataMessage> metadataCache;
    private final boolean disabled;

    private SuperstreamManager() {
        this.metadataConsumer = new MetadataConsumer();
        this.clientReporter = new ClientReporter();
        this.configurationOptimizer = new ConfigurationOptimizer();
        this.metadataCache = new ConcurrentHashMap<>();
        this.disabled = Boolean.parseBoolean(System.getenv(DISABLED_ENV_VAR));

        if (disabled) {
            logger.info("Superstream optimization is disabled via environment variable");
        }
    }

    /**
     * Check if optimization is already in progress for the current thread.
     *
     * @return true if optimization is in progress, false otherwise
     */
    public static boolean isOptimizationInProgress() {
        return Boolean.TRUE.equals(OPTIMIZATION_IN_PROGRESS.get());
    }

    /**
     * Set the optimization in progress flag for the current thread.
     *
     * @param inProgress true if optimization is in progress, false otherwise
     */
    public static void setOptimizationInProgress(boolean inProgress) {
        if (inProgress) {
            OPTIMIZATION_IN_PROGRESS.set(Boolean.TRUE);
        } else {
            OPTIMIZATION_IN_PROGRESS.remove();
        }
    }

    /**
     * Get the singleton instance of the SuperstreamManager.
     *
     * @return The SuperstreamManager instance
     */
    public static SuperstreamManager getInstance() {
        if (instance == null) {
            synchronized (SuperstreamManager.class) {
                if (instance == null) {
                    instance = new SuperstreamManager();
                }
            }
        }
        return instance;
    }

    public static Map<String, Object> convertPropertiesToMap(Properties properties) {
        Map<String, Object> map = new HashMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            map.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        return map;
    }

    /**
     * Optimize the producer properties for a given Kafka cluster.
     *
     * @param bootstrapServers The Kafka bootstrap servers
     * @param clientId The client ID
     * @param properties The producer properties to optimize
     * @return True if the optimization was successful, false otherwise
     */
    public boolean optimizeProducer(String bootstrapServers, String clientId, Properties properties) {
        if (disabled) {
            return false;
        }

        // Skip if already optimizing (prevents infinite recursion)
        if (isOptimizationInProgress()) {
            logger.debug("Skipping optimization for producer {} as optimization is already in progress", clientId);
            return false;
        }

        try {
            // Mark optimization as in progress for this thread
            setOptimizationInProgress(true);

            // Get or fetch the metadata message
            MetadataMessage metadataMessage = getOrFetchMetadataMessage(bootstrapServers, properties);
            if (metadataMessage == null) {
                logger.warn("No metadata message available for {}, skipping optimization", bootstrapServers);
                return false;
            }

            // Create a copy of the original configuration for reporting
            Properties originalProperties = new Properties();
            originalProperties.putAll(properties);

            // Check if optimization is active
            if (!metadataMessage.isActive()) {
                logger.info("Superstream optimization is not active for this kafka cluster, please head to the Superstream console and activate it.");
                reportClientInformation(bootstrapServers, properties, metadataMessage, clientId, originalProperties, Collections.emptyMap());
                return false;
            }

            // Get the application topics
            List<String> applicationTopics = getApplicationTopics();

            // Get the optimal configuration
            Map<String, Object> optimalConfiguration = configurationOptimizer.getOptimalConfiguration(
                    metadataMessage, applicationTopics);

            // Apply the optimal configuration
            List<String> modifiedKeys = configurationOptimizer.applyOptimalConfiguration(properties, optimalConfiguration);

            if (modifiedKeys.isEmpty()) {
                logger.info("No configuration parameters were modified");
                reportClientInformation(bootstrapServers, properties, metadataMessage, clientId, originalProperties, Collections.emptyMap());
                return false;
            }

            // Build optimized configuration map to report: include every key that was considered for optimisation.
            Map<String, Object> optimizedProperties = new HashMap<>();
            for (String key : optimalConfiguration.keySet()) {
                // After applyOptimalConfiguration, 'properties' holds the final value (either overridden or original).
                Object finalVal = properties.get(key);
                if (finalVal == null) {
                    // If not present in current props, fall back to original value (may be null as well)
                    finalVal = originalProperties.get(key);
                }
                optimizedProperties.put(key, finalVal);
            }

            // Build original filtered configuration limited to optimal keys
            Map<String,Object> originalFiltered = new java.util.HashMap<>();
            Map<String,Object> originalMap = convertPropertiesToMap(originalProperties);
            for (String key : optimalConfiguration.keySet()) {
                originalFiltered.put(key, originalMap.get(key));
            }

            // Pass configuration info via ThreadLocal to interceptor's onExit
            ai.superstream.agent.KafkaProducerInterceptor.TL_CFG_STACK.get()
                    .push(new ai.superstream.agent.KafkaProducerInterceptor.ConfigInfo(originalFiltered, optimizedProperties));

            // Report client information
            reportClientInformation(
                    bootstrapServers,
                    properties,
                    metadataMessage,
                    clientId,
                    originalProperties,
                    optimizedProperties
            );

            logger.info("Successfully optimized producer configuration for {}", clientId);
            return true;
        } catch (Exception e) {
            logger.error("Failed to optimize producer configuration", e);
            return false;
        } finally {
            // Always clear the flag when done
            setOptimizationInProgress(false);
        }
    }

    /**
     * Get the metadata message for a given Kafka cluster.
     *
     * @param bootstrapServers The Kafka bootstrap servers
     * @return The metadata message, or null if it couldn't be retrieved
     */
    private MetadataMessage getOrFetchMetadataMessage(String bootstrapServers, Properties originalProperties) {
        // Check the cache first
        if (metadataCache.containsKey(bootstrapServers)) {
            return metadataCache.get(bootstrapServers);
        }

        // Fetch the metadata
        MetadataMessage metadataMessage = metadataConsumer.getMetadataMessage(bootstrapServers, originalProperties);

        if (metadataMessage != null) {
            metadataCache.put(bootstrapServers, metadataMessage);
        }

        return metadataMessage;
    }

    /**
     * Get the list of application topics from the environment variable.
     *
     * @return The list of application topics
     */
    private List<String> getApplicationTopics() {
        String topicsString = System.getenv(TOPICS_ENV_VAR);
        if (topicsString == null || topicsString.trim().isEmpty()) {
            return Collections.emptyList();
        }

        return Arrays.stream(topicsString.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(java.util.stream.Collectors.toList());
    }

    /**
     * Report client information to the superstream.clients topic.
     *
     * @param bootstrapServers The Kafka bootstrap servers
     * @param metadataMessage The metadata message
     * @param clientId The client ID
     * @param originalConfiguration The original configuration
     * @param optimizedConfiguration The optimized configuration
     */
    private void reportClientInformation(String bootstrapServers, Properties originalProperties, MetadataMessage metadataMessage,
                                         String clientId, Properties originalConfiguration,
                                         Map<String, Object> optimizedConfiguration) {
        try {
            Map<String, Object> originalConfiguration1 = convertPropertiesToMap(originalConfiguration);
            List<String> topics = getApplicationTopics();
            String mostImpactfulTopic = configurationOptimizer.getMostImpactfulTopicName(metadataMessage, topics);
            boolean success = clientReporter.reportClient(
                    bootstrapServers,
                    originalProperties,
                    metadataMessage.getSuperstreamClusterId(),
                    metadataMessage.isActive(),
                    clientId,
                    originalConfiguration1,
                    optimizedConfiguration,
                    topics,
                    mostImpactfulTopic
            );

            if (!success) {
                logger.warn("Failed to report client information to the superstream.clients topic");
            }
        } catch (Exception e) {
            logger.error("Error reporting client information", e);
        }
    }
}