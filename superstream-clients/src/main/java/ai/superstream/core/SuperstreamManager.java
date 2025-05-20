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

            // Extract the optimized configuration for reporting
            Map<String, Object> optimizedProperties = new HashMap<>();
            for (String key : modifiedKeys) {
                optimizedProperties.put(key, properties.get(key));
            }

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

    /**
     * Get the superstream cluster ID for a specific bootstrap servers entry in the metadata cache,
     * or 0 if no metadata is available for this servers string.
     * 
     * @param bootstrapServers The bootstrap servers to look up
     * @return The superstream cluster ID for the specified bootstrap servers
     */
    public int getSuperstreamClusterId(String bootstrapServers) {
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            logger.debug("Bootstrap servers is null or empty, returning default superstream_cluster_id of 0");
            return 0;
        }
        
        // Normalize the bootstrap servers string for lookup
        String normalizedServers = normalizeBootstrapServers(bootstrapServers);
        
        // First try exact match
        MetadataMessage metadata = metadataCache.get(normalizedServers);
        if (metadata != null) {
            int id = metadata.getSuperstreamClusterId();
            logger.debug("Found superstream_cluster_id: {} for bootstrap servers: {}", id, bootstrapServers);
            return id;
        }
        
        // If no exact match, look for partial match (as servers might be specified in different formats/orders)
        for (Map.Entry<String, MetadataMessage> entry : metadataCache.entrySet()) {
            if (serversOverlap(normalizedServers, entry.getKey())) {
                int id = entry.getValue().getSuperstreamClusterId();
                logger.debug("Found superstream_cluster_id: {} for related bootstrap servers: {}", id, bootstrapServers);
                return id;
            }
        }
        
        logger.debug("No metadata found for bootstrap servers: {}, returning default superstream_cluster_id of 0", bootstrapServers);
        return 0;
    }
    
    /**
     * Normalize a bootstrap servers string for consistent lookup.
     * Converts a comma-separated list to a sorted, de-duplicated, normalized form.
     * 
     * @param bootstrapServers The bootstrap servers string
     * @return Normalized bootstrap servers string
     */
    private String normalizeBootstrapServers(String bootstrapServers) {
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            return "";
        }
        
        // Split the servers, trim each entry, and sort them
        Set<String> uniqueServers = new TreeSet<>();
        for (String server : bootstrapServers.split(",")) {
            String trimmed = server.trim();
            if (!trimmed.isEmpty()) {
                uniqueServers.add(trimmed);
            }
        }
        
        // Join them back with commas
        return String.join(",", uniqueServers);
    }
    
    /**
     * Check if two bootstrap servers strings overlap in terms of actual servers.
     * 
     * @param servers1 First bootstrap servers string (normalized)
     * @param servers2 Second bootstrap servers string (normalized)
     * @return True if there is at least one common server
     */
    private boolean serversOverlap(String servers1, String servers2) {
        if (servers1 == null || servers2 == null || servers1.isEmpty() || servers2.isEmpty()) {
            return false;
        }
        
        Set<String> set1 = new HashSet<>(Arrays.asList(servers1.split(",")));
        Set<String> set2 = new HashSet<>(Arrays.asList(servers2.split(",")));
        
        // Find intersection
        set1.retainAll(set2);
        return !set1.isEmpty();
    }
}