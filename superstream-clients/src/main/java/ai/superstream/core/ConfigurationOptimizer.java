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
    private static final String EXPLICIT_LINGER_ENV_VAR = "SUPERSTREAM_LINGER_MS";
    private static final int DEFAULT_LINGER_MS = 5000; // 5 seconds default

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

            // Use default linger value since no metadata configuration was found
            handleLingerConfiguration(optimalConfiguration, null);

            logger.info("Default optimizations will be applied: compression.type=zstd, batch.size=16384, linger.ms={}",
                    optimalConfiguration.get("linger.ms"));
            return optimalConfiguration;
        }

        // Find the most impactful topic
        TopicConfiguration mostImpactfulTopic = matchingConfigurations.stream()
                .max(Comparator.comparing(TopicConfiguration::calculateImpactScore))
                .orElse(null);

        optimalConfiguration = new HashMap<>(mostImpactfulTopic.getOptimizedConfiguration());

        // Handle linger configuration using metadata value as reference
        Integer metadataLinger = null;
        if (mostImpactfulTopic.getOptimizedConfiguration().containsKey("linger.ms")) {
            Object lingerValue = mostImpactfulTopic.getOptimizedConfiguration().get("linger.ms");
            if (lingerValue instanceof Number) {
                metadataLinger = ((Number) lingerValue).intValue();
            } else if (lingerValue instanceof String) {
                try {
                    metadataLinger = Integer.parseInt(lingerValue.toString());
                } catch (NumberFormatException e) {
                    logger.warn("Invalid linger.ms value in metadata: {}", lingerValue);
                }
            }
        }

        handleLingerConfiguration(optimalConfiguration, metadataLinger);

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

    /**
     * Process linger.ms configuration based on environment variable and metadata.
     *
     * @param configuration The configuration map to modify
     * @param metadataLinger The linger value from metadata, or null if not present
     */
    private void handleLingerConfiguration(Map<String, Object> configuration, Integer metadataLinger) {
        // Check for explicit linger from environment variable
        String explicitLingerStr = System.getenv(EXPLICIT_LINGER_ENV_VAR);
        Integer explicitLinger = null;

        if (explicitLingerStr != null && !explicitLingerStr.trim().isEmpty()) {
            try {
                explicitLinger = Integer.parseInt(explicitLingerStr.trim());
                logger.info("Using explicit linger.ms value from environment variable: {}", explicitLinger);
            } catch (NumberFormatException e) {
                logger.warn("Invalid {} value: {}. Will use default or metadata value.",
                        EXPLICIT_LINGER_ENV_VAR, explicitLingerStr);
            }
        }

        // Determine which linger value to use
        if (explicitLinger != null) {
            // Use explicit value from env var
            configuration.put("linger.ms", explicitLinger);
        } else if (metadataLinger != null) {
            // Use value from metadata
            configuration.put("linger.ms", metadataLinger);
        } else {
            // Use default value
            configuration.put("linger.ms", DEFAULT_LINGER_MS);
            logger.info("No linger.ms specified in metadata or environment variable. Using default: {}", DEFAULT_LINGER_MS);
        }
    }
}