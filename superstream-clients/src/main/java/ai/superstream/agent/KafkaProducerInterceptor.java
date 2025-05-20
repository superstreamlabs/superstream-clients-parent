package ai.superstream.agent;

import ai.superstream.core.ClientStatsReporter;
import ai.superstream.core.SuperstreamManager;
import ai.superstream.util.SuperstreamLogger;
import net.bytebuddy.asm.Advice;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.lang.ThreadLocal;

/**
 * Intercepts KafkaProducer constructor calls to optimize configurations and
 * collect metrics.
 */
public class KafkaProducerInterceptor {
    public static final SuperstreamLogger logger = SuperstreamLogger.getLogger(KafkaProducerInterceptor.class);

    // Constant for Superstream library prefix
    public static final String SUPERSTREAM_LIBRARY_PREFIX = "superstreamlib-";

    // Environment variable to check if Superstream is disabled
    private static final String DISABLED_ENV_VAR = "SUPERSTREAM_DISABLED";
    public static final boolean DISABLED = Boolean.parseBoolean(System.getenv(DISABLED_ENV_VAR));

    // Map to store client stats reporters for each producer
    public static final ConcurrentHashMap<String, ClientStatsReporter> clientStatsReporters = new ConcurrentHashMap<>();

    // Map to store producer metrics info by producer ID
    public static final ConcurrentHashMap<String, ProducerMetricsInfo> producerMetricsMap = new ConcurrentHashMap<>();

    // Single shared metrics collector for all producers
    public static final SharedMetricsCollector sharedCollector = new SharedMetricsCollector();

    // ThreadLocal stack to track nested KafkaProducer constructor calls per thread
    // This ensures that when the Superstream optimization logic creates its own producer
    // inside the application's producer construction, we still match the correct
    // properties object on exit (LIFO order).
    public static final ThreadLocal<java.util.Deque<Properties>> TL_PROPS_STACK =
            ThreadLocal.withInitial(java.util.ArrayDeque::new);

    // Static initializer to start the shared collector if enabled
    static {
        if (!DISABLED) {
            try {
                sharedCollector.start();
                logger.info("Superstream metrics collector initialized and started successfully");
            } catch (Exception e) {
                logger.error("Failed to start metrics collector: " + e.getMessage(), e);
            }
        } else {
            logger.info("Superstream metrics collection is disabled via SUPERSTREAM_DISABLED environment variable");
        }
    }

    /**
     * Check if Superstream is disabled via environment variable.
     * 
     * @return true if Superstream is disabled, false otherwise
     */
    public static boolean isDisabled() {
        return DISABLED;
    }

    /**
     * Called before the KafkaProducer constructor.
     * Used to optimize producer configurations.
     *
     * @param args The producer properties
     */
    @Advice.OnMethodEnter
    public static void onEnter(@Advice.AllArguments Object[] args) {
        // Skip if Superstream is disabled via environment variable
        if (isDisabled()) {
            return;
        }

        // Check if this is a direct call from application code or an internal
        // delegation
        if (!isInitialProducerCreation()) {
            logger.debug("Skipping internal constructor delegation");
            return;
        }

        // Extract Properties or Map from the arguments and push onto the stack
        Properties properties = extractProperties(args);
        if (properties != null) {
            TL_PROPS_STACK.get().push(properties);
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
                logger.error("Could not extract properties from properties");
                return;
            }

            // Skip producers created by the Superstream library
            String clientId = properties.getProperty("client.id", "");
            if (clientId.startsWith(SUPERSTREAM_LIBRARY_PREFIX)) {
                logger.debug("Skipping optimization for Superstream internal producer: {}", clientId);
                return;
            }

            logger.info("Intercepted KafkaProducer constructor");

            // Extract bootstrap servers
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
     * Called after the KafkaProducer constructor.
     * Used to register the producer for metrics collection.
     * 
     * @param producer The KafkaProducer instance that was just created
     */
    @Advice.OnMethodExit
    public static void onExit(@Advice.This Object producer) {
        // Skip if Superstream is disabled via environment variable
        if (isDisabled()) {
            return;
        }

        try {
            // Process only for the outer-most constructor call
            if (!isInitialProducerCreation()) {
                logger.debug("Skipping internal constructor delegation");
                return;
            }

            java.util.Deque<Properties> stack = TL_PROPS_STACK.get();
            if (stack.isEmpty()) {
                logger.error("No captured properties for this producer constructor; skipping stats reporter setup");
                return;
            }

            Properties producerProps = stack.pop();

            // Clean up ThreadLocal when outer-most constructor finishes
            if (stack.isEmpty()) {
                TL_PROPS_STACK.remove();
            }

            String bootstrapServers = producerProps.getProperty("bootstrap.servers");
            if (bootstrapServers == null || bootstrapServers.isEmpty()) {
                logger.error("bootstrap.servers missing in captured properties; skipping reporter setup");
                return;
            }

            String rawClientId = producerProps.getProperty("client.id"); // may be null or empty

            // Skip internal library producers (identified by client.id prefix)
            if (rawClientId != null && rawClientId.startsWith(SUPERSTREAM_LIBRARY_PREFIX)) {
                logger.debug("Skipping Superstream internal producer: {}", rawClientId);
                return;
            }

            // Use the JVM identity hash to create a unique key per producer instance
            String producerId = "producer-" + System.identityHashCode(producer);

            // The client ID to be reported is the raw value (may be null or empty, that's OK)
            String clientIdForStats = rawClientId != null ? rawClientId : "";

            // Only register if we don't already have this producer instance
            if (!producerMetricsMap.containsKey(producerId)) {
                logger.debug("Registering producer with metrics collector: {} (client.id='{}')", producerId, clientIdForStats);

                // Create a reporter for this producer instance – pass the original client.id
                ClientStatsReporter reporter = new ClientStatsReporter(bootstrapServers, producerProps, clientIdForStats);

                // Create metrics info for this producer
                ProducerMetricsInfo metricsInfo = new ProducerMetricsInfo(producer, reporter);

                // Register with the shared collector
                producerMetricsMap.put(producerId, metricsInfo);
                clientStatsReporters.put(producerId, reporter);

                logger.debug("Producer {} registered with shared metrics collector", producerId);
            }
        } catch (Exception e) {
            logger.error("Error registering producer with metrics collector: " + e.getMessage(), e);
        }
    }

    /**
     * Extract Properties object from constructor arguments.
     */
    public static Properties extractProperties(Object[] args) {
        // Look for Properties or Map in the arguments
        for (Object arg : args) {
            if (arg == null)
                continue;

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
                            // Handle bootstrap.servers when passed as a List
                            if (entry.getKey().equals("bootstrap.servers") && entry.getValue() instanceof List) {
                                @SuppressWarnings("unchecked")
                                List<String> servers = (List<String>) entry.getValue();
                                props.put(entry.getKey(), String.join(",", servers));
                            } else {
                                props.put(entry.getKey(), entry.getValue());
                            }
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
                    String[] fieldNames = { "originals", "values", "props", "properties", "configs" };

                    for (String fieldName : fieldNames) {
                        try {
                            Field field = arg.getClass().getDeclaredField(fieldName);
                            field.setAccessible(true);
                            Object fieldValue = field.get(arg);

                            if (fieldValue instanceof Map) {
                                @SuppressWarnings("unchecked")
                                Map<String, Object> map = (Map<String, Object>) fieldValue;

                                Properties props = new Properties();
                                for (Map.Entry<String, Object> entry : map.entrySet()) {
                                    if (entry.getValue() != null) {
                                        // Handle bootstrap.servers when passed as a List
                                        if (entry.getKey().equals("bootstrap.servers")
                                                && entry.getValue() instanceof List) {
                                            @SuppressWarnings("unchecked")
                                            List<String> servers = (List<String>) entry.getValue();
                                            props.put(entry.getKey(), String.join(",", servers));
                                        } else {
                                            props.put(entry.getKey(), entry.getValue());
                                        }
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
                    for (Method method : arg.getClass().getMethods()) {
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
                                        // Handle bootstrap.servers when passed as a List
                                        if (entry.getKey().equals("bootstrap.servers")
                                                && entry.getValue() instanceof List) {
                                            @SuppressWarnings("unchecked")
                                            List<String> servers = (List<String>) entry.getValue();
                                            props.put(entry.getKey(), String.join(",", servers));
                                        } else {
                                            props.put(entry.getKey(), entry.getValue());
                                        }
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
                    for (Method method : arg.getClass().getMethods()) {
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
     * Determines if this constructor call is the initial creation from application
     * code
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

    /**
     * Helper utility to extract a field value using reflection.
     */
    public static Object extractFieldValue(Object obj, String... fieldNames) {
        if (obj == null)
            return null;

        for (String fieldName : fieldNames) {
            try {
                Field field = findField(obj.getClass(), fieldName);
                if (field != null) {
                    field.setAccessible(true);
                    Object value = field.get(obj);
                    if (value != null) {
                        return value;
                    }
                }
            } catch (Exception e) {
                // Ignore and try next field
            }
        }

        return null;
    }

    /**
     * Find a field in a class or its superclasses.
     */
    public static Field findField(Class<?> clazz, String fieldName) {
        if (clazz == null || clazz == Object.class)
            return null;

        try {
            return clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            return findField(clazz.getSuperclass(), fieldName);
        }
    }

    /**
     * Find a method in a class or its superclasses.
     */
    public static Method findMethod(Class<?> clazz, String methodName) {
        if (clazz == null || clazz == Object.class)
            return null;

        try {
            return clazz.getDeclaredMethod(methodName);
        } catch (NoSuchMethodException e) {
            return findMethod(clazz.getSuperclass(), methodName);
        }
    }

    /**
     * Find a method in a class or its superclasses, trying multiple method names.
     */
    public static Method findMethod(Class<?> clazz, String... methodNames) {
        for (String methodName : methodNames) {
            Method method = findMethod(clazz, methodName);
            if (method != null) {
                return method;
            }
        }
        return null;
    }

    /**
     * Holds metrics information for a single producer.
     */
    public static class ProducerMetricsInfo {
        private final Object producer;
        private final ClientStatsReporter reporter;
        private final AtomicReference<CompressionStats> lastStats = new AtomicReference<>(new CompressionStats(0, 0));
        private final AtomicBoolean isActive = new AtomicBoolean(true);

        public ProducerMetricsInfo(Object producer, ClientStatsReporter reporter) {
            this.producer = producer;
            this.reporter = reporter;
        }

        public Object getProducer() {
            return producer;
        }

        public ClientStatsReporter getReporter() {
            return reporter;
        }

        public CompressionStats getLastStats() {
            return lastStats.get();
        }

        public void updateLastStats(CompressionStats stats) {
            lastStats.set(stats);
        }

        public boolean isActive() {
            return isActive.get();
        }
    }

    /**
     * A singleton class that collects Kafka metrics periodically for all registered
     * producers using a single shared thread. This is more efficient than having
     * one thread
     * per producer.
     */
    public static class SharedMetricsCollector {
        private final ScheduledExecutorService scheduler;
        private final AtomicBoolean running = new AtomicBoolean(false);
        private static final long COLLECTION_INTERVAL_MS = 30000; // 30 seconds

        public SharedMetricsCollector() {
            this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "superstream-kafka-metrics-collector");
                t.setDaemon(true);
                return t;
            });
        }

        /**
         * Start collecting metrics periodically for all registered producers.
         */
        public void start() {
            if (running.compareAndSet(false, true)) {
                logger.debug("Starting shared Kafka metrics collector with interval {} ms", COLLECTION_INTERVAL_MS);
                try {
                    scheduler.scheduleAtFixedRate(this::collectAllMetrics,
                            COLLECTION_INTERVAL_MS / 2, // Start sooner for first collection
                            COLLECTION_INTERVAL_MS,
                            TimeUnit.MILLISECONDS);
                    logger.debug("Metrics collection scheduler started successfully");
                } catch (Exception e) {
                    logger.error("Failed to schedule metrics collection: " + e.getMessage(), e);
                    running.set(false);
                }
            } else {
                logger.info("Metrics collector already running");
            }
        }

        /**
         * Collect metrics from all registered producers.
         */
        public void collectAllMetrics() {
            try {
                // Skip if disabled
                if (isDisabled()) {
                    return;
                }

                int totalProducers = producerMetricsMap.size();
                if (totalProducers == 0) {
                    logger.debug("No producers registered for metrics collection");
                    return;
                }

                logger.debug("Starting metrics collection cycle for {} producers", totalProducers);
                int successCount = 0;
                int skippedCount = 0;

                // Iterate through all registered producers
                for (Map.Entry<String, ProducerMetricsInfo> entry : producerMetricsMap.entrySet()) {
                    String producerId = entry.getKey();
                    ProducerMetricsInfo info = entry.getValue();

                    // Skip inactive producers
                    if (!info.isActive()) {
                        skippedCount++;
                        continue;
                    }

                    try {
                        boolean success = collectMetricsForProducer(producerId, info);
                        if (success) {
                            successCount++;
                        } else {
                            skippedCount++;
                        }
                    } catch (Exception e) {
                        logger.error("Error collecting metrics for producer {}: {}", producerId, e.getMessage());
                    }
                }

                logger.debug(
                        "Completed metrics collection cycle: {} producers processed, {} reported stats, {} skipped",
                        totalProducers, successCount, skippedCount);

            } catch (Exception e) {
                logger.error("Error in metrics collection cycle: " + e.getMessage(), e);
            }
        }

        /**
         * Collect metrics for a single producer.
         * 
         * @return true if metrics were successfully collected and reported, false if
         *         skipped
         */
        public boolean collectMetricsForProducer(String producerId, ProducerMetricsInfo info) {
            try {
                Object producer = info.getProducer();
                ClientStatsReporter reporter = info.getReporter();

                // Get the metrics object from the producer
                Object metrics = extractFieldValue(producer, "metrics");
                if (metrics == null) {
                    logger.debug("No metrics object found in producer {}", producerId);
                    return false;
                }

                // Try to get the compression ratio metric; fall back to 1.0 (no compression)
                double compressionRatio = getCompressionRatio(metrics);
                if (compressionRatio <= 0) {
                    logger.debug("No compression ratio metric found; assuming ratio 1.0 for producer {}", producerId);
                    compressionRatio = 1.0;
                }

                // Get the outgoing-byte-total metrics - this is a per-node metric that
                // represents compressed bytes
                // We need to sum it across all nodes, and it's cumulative over time
                long totalOutgoingBytes = getOutgoingBytesTotal(metrics);

                if (totalOutgoingBytes <= 0) {
                    logger.debug("No outgoing bytes found for producer {}", producerId);
                    return false;
                }

                // Calculate the delta since the last collection cycle
                CompressionStats prevStats = info.getLastStats();
                if (totalOutgoingBytes <= prevStats.compressedBytes) {
                    logger.debug("No new data since last collection for producer {} (current: {}, previous: {})",
                            producerId, totalOutgoingBytes, prevStats.compressedBytes);
                    return false;
                }

                // The compressed bytes is the delta since the last collection
                long compressedBytes = totalOutgoingBytes - prevStats.compressedBytes;

                // Use the compression ratio to calculate the uncompressed size
                // compression_ratio = compressed_size / uncompressed_size
                // Therefore: uncompressed_size = compressed_size / compression_ratio
                long uncompressedBytes = compressionRatio > 0 ? (long) (compressedBytes / compressionRatio)
                        : compressedBytes;

                // Update the last stats with the new total (cumulative) bytes
                // For uncompressed, add the new delta to the previous total
                info.updateLastStats(
                        new CompressionStats(totalOutgoingBytes, prevStats.uncompressedBytes + uncompressedBytes));

                // Report the compression statistics for this interval (delta)
                reporter.recordBatch(uncompressedBytes, compressedBytes);

                logger.debug("Producer {} compression collected: before={} bytes, after={} bytes, ratio={}",
                        producerId, uncompressedBytes, compressedBytes, String.format("%.4f", compressionRatio));

                return true;
            } catch (Exception e) {
                logger.error("Error collecting Kafka metrics for producer {}: {}", producerId, e.getMessage(), e);
                return false;
            }
        }

        /**
         * Get the compression ratio from the metrics object.
         */
        public double getCompressionRatio(Object metrics) {
            try {
                // Extract the metrics map from the Metrics object
                Map<?, ?> metricsMap = extractMetricsMap(metrics);
                if (metricsMap != null) {
                    logger.debug("Metrics map size: {}", metricsMap.size());

                    // Look for direct compression metrics only
                    double compressionRatio = findDirectCompressionMetric(metricsMap);
                    if (compressionRatio > 0) {
                        return compressionRatio;
                    }
                } else {
                    logger.debug("Could not extract metrics map from: {}", metrics.getClass().getName());
                }
            } catch (Exception e) {
                logger.debug("Error getting compression ratio: " + e.getMessage(), e);
            }

            return 0;
        }

        /**
         * Find direct compression metrics in the metrics map.
         */
        private double findDirectCompressionMetric(Map<?, ?> metricsMap) {
            // Look for compression metrics directly in the map
            for (Map.Entry<?, ?> entry : metricsMap.entrySet()) {
                Object key = entry.getKey();

                // Handle keys that are MetricName objects
                if (key.getClass().getName().endsWith("MetricName")) {
                    try {
                        Method nameMethod = findMethod(key.getClass(), "name");
                        Method groupMethod = findMethod(key.getClass(), "group");

                        if (nameMethod != null && groupMethod != null) {
                            nameMethod.setAccessible(true);
                            groupMethod.setAccessible(true);

                            String name = nameMethod.invoke(key).toString();
                            String group = groupMethod.invoke(key).toString();

                            // Check for common compression metrics
                            if ((group.equals("producer-metrics") || group.equals("producer-topic-metrics")) &&
                                    (name.equals("compression-rate-avg") || name.equals("record-compression-rate") ||
                                            name.equals("compression-ratio"))) {

                                logger.debug("Found compression metric: {}.{}", group, name);
                                double value = extractMetricValue(entry.getValue());
                                if (value > 0) {
                                    logger.debug("Compression ratio value: {}", value);
                                    return value;
                                }
                            }
                        }
                    } catch (Exception e) {
                        // Ignore and continue checking other keys
                    }
                }
                // Handle String keys
                else if (key instanceof String) {
                    String keyStr = (String) key;
                    if ((keyStr.contains("producer-metrics") || keyStr.contains("producer-topic-metrics")) &&
                            (keyStr.contains("compression-rate") || keyStr.contains("compression-ratio"))) {

                        logger.debug("Found compression metric with string key: {}", keyStr);
                        double value = extractMetricValue(entry.getValue());
                        if (value > 0) {
                            logger.debug("Compression ratio value: {}", value);
                            return value;
                        }
                    }
                }
            }
            return 0;
        }

        /**
         * Get the total outgoing bytes across all nodes from the metrics object.
         * This metric exists per broker node and represents bytes after compression.
         */
        private long getOutgoingBytesTotal(Object metrics) {
            try {
                // Extract the metrics map from the Metrics object
                Map<?, ?> metricsMap = extractMetricsMap(metrics);
                if (metricsMap != null) {
                    // The outgoing-byte-total is in the producer-node-metrics group
                    String targetGroup = "producer-node-metrics";
                    String targetMetric = "outgoing-byte-total";
                    long totalBytes = 0;
                    boolean foundAnyNodeMetric = false;

                    // Iterate through all metrics
                    for (Map.Entry<?, ?> entry : metricsMap.entrySet()) {
                        Object key = entry.getKey();

                        // Handle MetricName objects
                        if (key.getClass().getName().endsWith("MetricName")) {
                            try {
                                Method nameMethod = findMethod(key.getClass(), "name");
                                Method groupMethod = findMethod(key.getClass(), "group");
                                Method tagsMethod = findMethod(key.getClass(), "tags");

                                if (nameMethod != null && groupMethod != null) {
                                    nameMethod.setAccessible(true);
                                    groupMethod.setAccessible(true);

                                    String name = nameMethod.invoke(key).toString();
                                    String group = groupMethod.invoke(key).toString();

                                    // If this is a node metric with outgoing bytes
                                    if (group.equals(targetGroup) && name.equals(targetMetric)) {
                                        foundAnyNodeMetric = true;

                                        double value = extractMetricValue(entry.getValue());

                                        // Get the node-id from tags if possible
                                        String nodeId = "unknown";
                                        if (tagsMethod != null) {
                                            tagsMethod.setAccessible(true);
                                            Object tags = tagsMethod.invoke(key);
                                            if (tags instanceof Map) {
                                                Object nodeIdObj = ((Map<?, ?>) tags).get("node-id");
                                                if (nodeIdObj != null) {
                                                    nodeId = nodeIdObj.toString();
                                                }
                                            }
                                        }

                                        logger.debug("Found outgoing bytes for node {}: {}", nodeId, value);
                                        totalBytes += (long) value;
                                    }

                                    // Fall back to producer-metrics.byte-total if needed
                                    if (!foundAnyNodeMetric && group.equals("producer-metrics") &&
                                            (name.equals("byte-total") || name.equals("outgoing-byte-total"))) {
                                        double value = extractMetricValue(entry.getValue());
                                        logger.debug("Found fallback byte metric: {}={}", name, value);
                                        // Save this value but keep looking for node-specific metrics
                                        if (totalBytes == 0) {
                                            totalBytes = (long) value;
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                logger.debug("Error extracting metrics: {}", e.getMessage());
                            }
                        }
                        // Handle String keys
                        else if (key instanceof String) {
                            String keyStr = (String) key;
                            if (keyStr.contains(targetGroup) && keyStr.contains(targetMetric)) {
                                foundAnyNodeMetric = true;
                                double value = extractMetricValue(entry.getValue());
                                logger.debug("Found outgoing bytes with string key {}: {}", keyStr, value);
                                totalBytes += (long) value;
                            }
                        }
                    }

                    if (totalBytes > 0) {
                        if (foundAnyNodeMetric) {
                            logger.debug("Total outgoing bytes across all nodes: {}", totalBytes);
                        } else {
                            logger.debug("Using fallback byte metric total: {}", totalBytes);
                        }
                        return totalBytes;
                    }
                } else {
                    logger.debug("Could not extract metrics map from: {}", metrics.getClass().getName());
                }
            } catch (Exception e) {
                logger.debug("Error getting outgoing bytes total: {}", e.getMessage(), e);
            }

            return 0;
        }

        /**
         * Extract the metrics map from a Metrics object.
         * Handles both cases where metrics is a Map directly or a Metrics object
         * with an internal 'metrics' field.
         */
        private Map<?, ?> extractMetricsMap(Object metrics) {
            if (metrics == null) {
                return null;
            }

            try {
                // If it's already a Map, just cast it
                if (metrics instanceof Map) {
                    return (Map<?, ?>) metrics;
                }

                // Try to extract the internal metrics map field
                Field metricsField = findField(metrics.getClass(), "metrics");
                if (metricsField != null) {
                    metricsField.setAccessible(true);
                    Object metricsValue = metricsField.get(metrics);
                    if (metricsValue instanceof Map) {
                        logger.debug("Successfully extracted metrics map from Metrics object");
                        return (Map<?, ?>) metricsValue;
                    }
                }

                // Try to get metrics through a method
                Method getMetricsMethod = findMethod(metrics.getClass(), "metrics", "getMetrics");
                if (getMetricsMethod != null) {
                    getMetricsMethod.setAccessible(true);
                    Object metricsValue = getMetricsMethod.invoke(metrics);
                    if (metricsValue instanceof Map) {
                        logger.debug("Successfully extracted metrics map via method");
                        return (Map<?, ?>) metricsValue;
                    }
                }

                logger.debug("Object is neither a Map nor has a metrics field/method: {}",
                        metrics.getClass().getName());
            } catch (Exception e) {
                logger.debug("Error extracting metrics map: {}", e.getMessage());
            }

            return null;
        }

        /**
         * Extract a numeric value from a metric object.
         */
        public double extractMetricValue(Object metric) {
            if (metric == null) {
                return 0;
            }

            try {
                // Try value() method (common in metrics libraries)
                Method valueMethod = findMethod(metric.getClass(), "metricValue");
                if (valueMethod != null) {
                    valueMethod.setAccessible(true);
                    Object value = valueMethod.invoke(metric);
                    if (value instanceof Number) {
                        return ((Number) value).doubleValue();
                    }
                }
            } catch (Exception e) {
                logger.debug("Error extracting metric value: " + e.getMessage());
            }

            return 0;
        }
    }

    /**
     * Simple class to track compression statistics over time.
     */
    public static class CompressionStats {
        public final long compressedBytes;
        public final long uncompressedBytes;

        public CompressionStats(long compressedBytes, long uncompressedBytes) {
            this.compressedBytes = compressedBytes;
            this.uncompressedBytes = uncompressedBytes;
        }
    }
}
