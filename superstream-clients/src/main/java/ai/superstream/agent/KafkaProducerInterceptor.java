package ai.superstream.agent;

import ai.superstream.core.SuperstreamManager;
import ai.superstream.util.SuperstreamLogger;
import net.bytebuddy.asm.Advice;

import java.util.Properties;
import java.util.Map;


/**
 * Intercepts KafkaProducer constructor calls to optimize configurations.
 */
public class KafkaProducerInterceptor {
    public static final SuperstreamLogger logger = SuperstreamLogger.getLogger(KafkaProducerInterceptor.class);

    /**
     * Called before the KafkaProducer constructor.
     *
     * @param properties The producer properties
     */
    @Advice.OnMethodEnter
    public static void onEnter(@Advice.AllArguments Object[] args) {
        // Check if this is a direct call from application code or an internal delegation
        if (!isInitialProducerCreation()) {
            logger.debug("Skipping internal constructor delegation");
            return;
        }

        // Extract Properties or Map from the arguments
        Properties properties = extractProperties(args);
        if (properties == null) {
            logger.debug("Could not extract properties from constructor arguments");
            return;
        }

        // Make a copy of the original properties in case we need to restore them
        Properties originalProperties = new Properties();
        originalProperties.putAll(properties);

        try {
            // Skip if we're already in the process of optimizing
            if (SuperstreamManager.isOptimizationInProgress()) {
                logger.debug("Skipping interception as optimization is already in progress");
                return;
            }

            if (properties == null || properties.isEmpty()) {
                logger.warn("Could not extract properties from properties");
                return;
            }

            // Skip producers created by the Superstream library
            String clientId = properties.getProperty("client.id", "");
            if (clientId.startsWith("superstreamlib-")) {
                logger.debug("Skipping optimization for Superstream internal producer: {}", clientId);
                return;
            }

            logger.info("Intercepted KafkaProducer constructor");

            // Extract bootstrap servers and client id
            String bootstrapServers = properties.getProperty("bootstrap.servers");
            if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
                logger.warn("bootstrap.servers is not set, cannot optimize");
                return;
            }

            // Optimize the producer
            boolean success = SuperstreamManager.getInstance().optimizeProducer(bootstrapServers, clientId, properties);
            if (!success) {
                // Restore original properties if optimization failed
                properties.clear();
                properties.putAll(originalProperties);
            }
        } catch (Exception e) {
            // Restore original properties on any exception
            properties.clear();
            properties.putAll(originalProperties);
            logger.error("Error during producer optimization, restored original properties", e);
        }
    }

    /**
     * Extract Properties object from constructor arguments.
     */
    public static Properties extractProperties(Object[] args) {
        // Look for Properties or Map in the arguments
        for (Object arg : args) {
            if (arg == null) continue;

            if (arg instanceof Properties) {
                return (Properties) arg;
            }

            if (arg instanceof Map) {
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> map = (Map<String, Object>) arg;
                    Properties props = new Properties();
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        if (entry.getValue() != null) {
                            props.put(entry.getKey(), entry.getValue());
                        }
                    }
                    return props;
                } catch (ClassCastException e) {
                    // Not the map type we expected
                    logger.debug("Could not cast Map to Map<String, Object>");
                }
            }

            // Handle ProducerConfig object which contains properties
            String className = arg.getClass().getName();
            if (className.endsWith("ProducerConfig")) {
                try {
                    // Try multiple possible field names
                    String[] fieldNames = {"originals", "values", "props", "properties", "configs"};

                    for (String fieldName : fieldNames) {
                        try {
                            java.lang.reflect.Field field = arg.getClass().getDeclaredField(fieldName);
                            field.setAccessible(true);
                            Object fieldValue = field.get(arg);

                            if (fieldValue instanceof Map) {
                                @SuppressWarnings("unchecked")
                                Map<String, Object> map = (Map<String, Object>) fieldValue;

                                Properties props = new Properties();
                                for (Map.Entry<String, Object> entry : map.entrySet()) {
                                    if (entry.getValue() != null) {
                                        props.put(entry.getKey(), entry.getValue());
                                    }
                                }
                                return props;
                            } else if (fieldValue instanceof Properties) {
                                return (Properties) fieldValue;
                            }
                        } catch (NoSuchFieldException e) {
                            // Field doesn't exist, try the next one
                            continue;
                        }
                    }

                    // Try to call getters if field access failed
                    for (java.lang.reflect.Method method : arg.getClass().getMethods()) {
                        if ((method.getName().equals("originals") ||
                                method.getName().equals("values") ||
                                method.getName().equals("configs") ||
                                method.getName().equals("properties") ||
                                method.getName().equals("getOriginals") ||
                                method.getName().equals("getValues") ||
                                method.getName().equals("getConfigs") ||
                                method.getName().equals("getProperties")) &&
                                method.getParameterCount() == 0) {

                            Object result = method.invoke(arg);
                            if (result instanceof Map) {
                                @SuppressWarnings("unchecked")
                                Map<String, Object> map = (Map<String, Object>) result;

                                Properties props = new Properties();
                                for (Map.Entry<String, Object> entry : map.entrySet()) {
                                    if (entry.getValue() != null) {
                                        props.put(entry.getKey(), entry.getValue());
                                    }
                                }
                                return props;
                            } else if (result instanceof Properties) {
                                return (Properties) result;
                            }
                        }
                    }

                    // Last resort: Try to get the ProducerConfig's bootstrap.servers value
                    // and create a minimal Properties object
                    for (java.lang.reflect.Method method : arg.getClass().getMethods()) {
                        if (method.getName().equals("getString") && method.getParameterCount() == 1) {
                            try {
                                String bootstrapServers = (String) method.invoke(arg, "bootstrap.servers");
                                String clientId = (String) method.invoke(arg, "client.id");

                                if (bootstrapServers != null) {
                                    Properties minProps = new Properties();
                                    minProps.put("bootstrap.servers", bootstrapServers);
                                    if (clientId != null) {
                                        minProps.put("client.id", clientId);
                                    }
                                    return minProps;
                                }
                            } catch (Exception e) {
                                logger.debug("Failed to get bootstrap.servers from ProducerConfig");
                            }
                        }
                    }

                } catch (Exception e) {
                    logger.debug("Failed to extract properties from ProducerConfig: " + e.getMessage());
                }
            }
        }

        return null;
    }

    /**
     * Determines if this constructor call is the initial creation from application code
     * rather than an internal delegation between constructors.
     */
    public static boolean isInitialProducerCreation() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();

        // Start from index 1 to skip getStackTrace() itself
        boolean foundKafkaProducer = false;
        int kafkaProducerCount = 0;

        for (int i = 1; i < stackTrace.length; i++) {
            String className = stackTrace[i].getClassName();

            // Look for KafkaProducer in the class name
            if (className.endsWith("KafkaProducer")) {
                foundKafkaProducer = true;
                kafkaProducerCount++;

                // If we find more than one KafkaProducer in the stack, it's a delegation
                if (kafkaProducerCount > 1) {
                    return false;
                }
            }
            // Once we've seen KafkaProducer and then see a different class,
            // we've found the actual caller
            else if (foundKafkaProducer) {
                // Skip certain framework classes that might wrap the call
                if (className.startsWith("java.") ||
                        className.startsWith("javax.") ||
                        className.startsWith("sun.") ||
                        className.startsWith("com.sun.")) {
                    continue;
                }

                // We've found the application class that called KafkaProducer
                logger.debug("Detected initial producer creation from: " + className);
                return true;
            }
        }

        // If we make it here with exactly one KafkaProducer in the stack, it's likely
        // the initial creation (first constructor being called)
        return kafkaProducerCount == 1;
    }
}
