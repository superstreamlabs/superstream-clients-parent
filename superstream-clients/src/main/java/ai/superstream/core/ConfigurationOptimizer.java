package ai.superstream.core;

import ai.superstream.model.MetadataMessage;
import ai.superstream.model.TopicConfiguration;
import ai.superstream.util.SuperstreamLogger;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Optimizes Kafka producer configurations based on metadata.
 */
public class ConfigurationOptimizer {
    private static final SuperstreamLogger logger = SuperstreamLogger.getLogger(ConfigurationOptimizer.class);

    /**
     * Get the optimal configuration for a set of topics.
     *
     * @param metadataMessage The metadata message
     * @param applicationTopics The list of topics that the application might produce to
     * @return The optimal configuration, or an empty map if no optimization is possible
     */
    public Map<String, Object> getOptimalConfiguration(MetadataMessage metadataMessage, List<String> applicationTopics) {
        // Get all matching topic configurations
        List<TopicConfiguration> matchingConfigurations = metadataMessage.getTopicsConfiguration().stream()
                .filter(config -> applicationTopics.contains(config.getTopicName()))
                .collect(Collectors.toList());

        if (matchingConfigurations.isEmpty()) {
            if (applicationTopics.isEmpty()) {
                logger.info("SUPERSTREAM_TOPICS_LIST environment variable contains no topics. Applying default optimizations.");
            } else {
                logger.info("No matching topic configurations found for the application topics. Applying default optimizations.");
            }

            // Apply default optimizations when no matching topics found
            Map<String, Object> defaultOptimizations = new HashMap<>();
            defaultOptimizations.put("compression.type", "zstd");
            defaultOptimizations.put("batch.size", 16384);  // 16KB

            logger.info("Default optimizations will be applied: compression.type=zstd, batch.size=16384");
            return defaultOptimizations;
        }

        // Find the most impactful topic
        TopicConfiguration mostImpactfulTopic = matchingConfigurations.stream()
                .max(Comparator.comparing(TopicConfiguration::calculateImpactScore))
                .orElse(null);

        return mostImpactfulTopic.getOptimizedConfiguration();
    }

    /**
     * Apply the optimal configuration to the producer properties.
     *
     * @param properties The producer properties to modify
     * @param optimalConfiguration The optimal configuration to apply
     * @return The list of configuration keys that were modified
     */
    public List<String> applyOptimalConfiguration(Properties properties, Map<String, Object> optimalConfiguration) {
        if (optimalConfiguration == null || optimalConfiguration.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> modifiedKeys = new ArrayList<>();

        for (Map.Entry<String, Object> entry : optimalConfiguration.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            // Validate the configuration before applying
            if (!isValidConfiguration(key, value)) {
                logger.warn("Invalid configuration value for {}: {}. Skipping this parameter.", key, value);
                continue;
            }

            // Store the original value for logging
            Object originalValue = properties.get(key);

            // Apply the optimization
            properties.put(key, value);
            modifiedKeys.add(key);

            logger.info("Overriding configuration: {}={} (was: {})", key, value, originalValue);
        }

        return modifiedKeys;
    }

    private boolean isValidConfiguration(String key, Object value) {
        try {
            if ("compression.type".equals(key)) {
                String compressionType = value.toString();
                // Valid compression types in Kafka
                return Arrays.asList("none", "gzip", "snappy", "lz4", "zstd").contains(compressionType);
            }
            // Add validation for other key types as needed
            return true;
        } catch (Exception e) {
            logger.warn("Error validating configuration {}: {}", key, value, e);
            return false;
        }
    }
}