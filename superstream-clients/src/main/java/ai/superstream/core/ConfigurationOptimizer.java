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
    private static final String LATENCY_SENSITIVE_ENV_VAR = "SUPERSTREAM_LATENCY_SENSITIVE";

    /**
     * Get the optimal configuration for a set of topics.
     *
     * @param metadataMessage The metadata message
     * @param applicationTopics The list of topics that the application might produce to
     * @return The optimal configuration, or an empty map if no optimization is possible
     */
    public Map<String, Object> getOptimalConfiguration(MetadataMessage metadataMessage, List<String> applicationTopics) {
        // Check if the application is latency-sensitive
        boolean isLatencySensitive = isLatencySensitive();
        if (isLatencySensitive) {
            logger.info("Application is marked as latency-sensitive, linger.ms will not be modified");
        }

        // Get all matching topic configurations
        List<TopicConfiguration> matchingConfigurations = metadataMessage.getTopicsConfiguration().stream()
                .filter(config -> applicationTopics.contains(config.getTopicName()))
                .collect(Collectors.toList());

        Map<String, Object> optimalConfiguration;

        if (matchingConfigurations.isEmpty()) {
            if (applicationTopics.isEmpty()) {
                logger.info("SUPERSTREAM_TOPICS_LIST environment variable contains no topics. Applying default optimizations.");
            } else {
                logger.info("No matching topic configurations found for the application topics. Applying default optimizations.");
            }

            // Apply default optimizations when no matching topics found
            optimalConfiguration = new HashMap<>();
            optimalConfiguration.put("compression.type", "zstd");
            optimalConfiguration.put("batch.size", 16384);  // 16KB

            // Only add linger if not latency-sensitive
            if (!isLatencySensitive) {
                optimalConfiguration.put("linger.ms", 5000);  // 5 seconds default
                logger.info("Default optimizations will be applied: compression.type=zstd, batch.size=16384, linger.ms=5000");
            } else {
                logger.info("Default optimizations will be applied: compression.type=zstd, batch.size=16384 (linger.ms unchanged)");
            }

            return optimalConfiguration;
        }

        // Find the most impactful topic
        TopicConfiguration mostImpactfulTopic = findMostImpactfulTopic(matchingConfigurations);

        optimalConfiguration = new HashMap<>(mostImpactfulTopic.getOptimizedConfiguration());

        // If latency sensitive, remove linger.ms setting
        if (isLatencySensitive && optimalConfiguration.containsKey("linger.ms")) {
            optimalConfiguration.remove("linger.ms");
            logger.info("Ignore linger.ms from optimizations due to latency-sensitive configuration");
        }

        return optimalConfiguration;
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

            // Special handling for linger.ms
            if ("linger.ms".equals(key)) {
                // Skip linger.ms if the application is latency-sensitive
                if (isLatencySensitive()) {
                    logger.info("Skipping linger.ms optimization due to latency-sensitive configuration");
                    continue;
                }

                int optimalLinger = value instanceof Number ?
                        ((Number) value).intValue() : Integer.parseInt(value.toString());

                // Check if there's an existing linger setting
                Object existingValue = properties.get(key);
                if (existingValue != null) {
                    int existingLinger;
                    if (existingValue instanceof Number) {
                        existingLinger = ((Number) existingValue).intValue();
                    } else {
                        try {
                            existingLinger = Integer.parseInt(existingValue.toString());
                        } catch (NumberFormatException e) {
                            logger.warn("Invalid existing linger.ms value: {}. Will use optimal value.", existingValue);
                            existingLinger = 0;
                        }
                    }

                    // Use the greater of optimal and existing linger values
                    if (existingLinger > optimalLinger) {
                        logger.info("Keeping existing linger.ms value {} as it's greater than optimal value {}",
                                existingLinger, optimalLinger);
                        continue; // Skip this key, keeping the existing value
                    }
                }
            }

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

            if (originalValue == null) {
                logger.info("Setting configuration: {}={} (was not previously set)", key, value);
            } else {
                logger.info("Overriding configuration: {}={} (was: {})", key, value, originalValue);
            }
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

    /**
     * Determine if the application is latency-sensitive based on environment variable.
     *
     * @return true if the application is latency-sensitive, false otherwise
     */
    private boolean isLatencySensitive() {
        String latencySensitiveStr = System.getenv(LATENCY_SENSITIVE_ENV_VAR);
        if (latencySensitiveStr != null && !latencySensitiveStr.trim().isEmpty()) {
            return Boolean.parseBoolean(latencySensitiveStr.trim());
        }
        return false; // Default to not latency-sensitive
    }

    /**
     * Helper to find the most impactful topic from a list of matching configurations.
     */
    private TopicConfiguration findMostImpactfulTopic(List<TopicConfiguration> matchingConfigurations) {
        return matchingConfigurations.stream()
                .max(Comparator.comparing(TopicConfiguration::calculateImpactScore))
                .orElse(null);
    }

    /**
     * Get the most impactful topic name for a set of topics.
     *
     * @param metadataMessage The metadata message
     * @param applicationTopics The list of topics that the application might produce to
     * @return The name of the most impactful topic, or null if none found
     */
    public String getMostImpactfulTopicName(MetadataMessage metadataMessage, List<String> applicationTopics) {
        List<TopicConfiguration> matchingConfigurations = metadataMessage.getTopicsConfiguration().stream()
                .filter(config -> applicationTopics.contains(config.getTopicName()))
                .collect(Collectors.toList());
        if (matchingConfigurations.isEmpty()) {
            return null;
        }
        TopicConfiguration mostImpactfulTopic = findMostImpactfulTopic(matchingConfigurations);
        return mostImpactfulTopic != null ? mostImpactfulTopic.getTopicName() : null;
    }
}