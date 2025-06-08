package ai.superstream.core;

import ai.superstream.agent.KafkaProducerInterceptor;
import ai.superstream.model.ClientStatsMessage;
import ai.superstream.util.NetworkUtils;
import ai.superstream.util.SuperstreamLogger;
import ai.superstream.util.KafkaPropertiesUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Reports client statistics to the superstream.clients topic periodically.
 */
public class ClientStatsReporter {
    private static final SuperstreamLogger logger = SuperstreamLogger.getLogger(ClientStatsReporter.class);
    private static final String CLIENTS_TOPIC = "superstream.clients";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final long REPORT_INTERVAL_MS = 300000; // 5 minutes
    private static final String DISABLED_ENV_VAR = "SUPERSTREAM_DISABLED";

    // Shared scheduler for all reporters to minimize thread usage
    private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "superstream-client-stats-reporter");
        t.setDaemon(true);
        return t;
    });

    // Coordinator per cluster to minimise producer usage
    private static final ConcurrentHashMap<String, ClusterStatsCoordinator> coordinators = new ConcurrentHashMap<>();

    private final ClientStatsCollector statsCollector;
    private final Properties producerProperties;
    private final String clientId;
    private final AtomicBoolean registered = new AtomicBoolean(false);
    private final boolean disabled;
    private final String producerUuid;

    private final AtomicReference<java.util.Map<String, Double>> latestMetrics = new AtomicReference<>(
            new java.util.HashMap<>());
    private java.util.Map<String, java.util.Map<String, Double>> latestTopicMetrics = new java.util.HashMap<>();
    private java.util.Map<String, java.util.Map<String, Double>> latestNodeMetrics = new java.util.HashMap<>();
    private java.util.Map<String, String> latestAppInfoMetrics = new java.util.HashMap<>();
    private final ConcurrentSkipListSet<String> topicsWritten = new ConcurrentSkipListSet<>();
    private volatile java.util.Map<String, Object> originalConfig = null;
    private volatile java.util.Map<String, Object> optimizedConfig = null;
    private String mostImpactfulTopic;
    private String error;

    /**
     * Creates a new client stats reporter.
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @param clientProperties Producer properties to use for authentication
     * @param clientId         The client ID to include in reports
     * @param producerUuid     The producer UUID
     */
    public ClientStatsReporter(String bootstrapServers, Properties clientProperties, String clientId, String producerUuid) {
        this.clientId = clientId;
        this.disabled = Boolean.parseBoolean(System.getenv(DISABLED_ENV_VAR));
        this.producerUuid = producerUuid;

        if (this.disabled) {
            logger.debug("Superstream stats reporting is disabled via environment variable");
        }

        this.statsCollector = new ClientStatsCollector();

        // Copy essential client configuration properties from the original client
        this.producerProperties = new Properties();
        KafkaPropertiesUtils.copyClientConfigurationProperties(clientProperties, this.producerProperties);

        // Set up basic producer properties
        this.producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG,
                KafkaProducerInterceptor.SUPERSTREAM_LIBRARY_PREFIX + "client-stats-reporter");

        // Use efficient compression settings for the reporter itself
        this.producerProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        this.producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        this.producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, 100);

        // Mark as registered for recordBatch logic
        this.registered.set(true);

        // Register with per-cluster coordinator
        String clusterKey = normalizeBootstrapServers(bootstrapServers);
        ClusterStatsCoordinator coord = coordinators.computeIfAbsent(clusterKey,
                k -> new ClusterStatsCoordinator(bootstrapServers, producerProperties));
        coord.addReporter(this);
    }

    /**
     * Records compression statistics for a batch of messages.
     * This method should be called by the producer each time it sends a batch.
     *
     * @param uncompressedSize Size of batch before compression (in bytes)
     * @param compressedSize   Size of batch after compression (in bytes)
     */
    public void recordBatch(long uncompressedSize, long compressedSize) {
        // Only record if we're actually running and not disabled
        if (registered.get() && !disabled) {
            statsCollector.recordBatch(uncompressedSize, compressedSize);
        }
    }

    // Drain stats into producer, called by coordinator
    void drainInto(Producer<String, String> producer) {
        if (disabled || !registered.get()) {
            return; // Do not send stats when disabled or deactivated
        }

        try {
            ClientStatsCollector.Stats stats = statsCollector.captureAndReset();
            long totalBytesBefore = stats.getBytesBeforeCompression();
            long totalBytesAfter = stats.getBytesAfterCompression();

            ClientStatsMessage message = new ClientStatsMessage(
                    clientId,
                    NetworkUtils.getLocalIpAddress(),
                    totalBytesBefore,
                    totalBytesAfter,
                    ClientReporter.getClientVersion(),
                    NetworkUtils.getHostname(),
                    producerUuid);

            // Always attach metric snapshots, defaulting to empty maps if no data available
            java.util.Map<String, Double> metricsSnapshot = latestMetrics.get();
            message.setProducerMetrics(metricsSnapshot != null ? metricsSnapshot : new java.util.HashMap<>());

            java.util.Map<String, java.util.Map<String, Double>> topicMetricsSnapshot = latestTopicMetrics;
            message.setTopicMetrics(topicMetricsSnapshot != null ? topicMetricsSnapshot : new java.util.HashMap<>());

            java.util.Map<String, java.util.Map<String, Double>> nodeMetricsSnapshot = latestNodeMetrics;
            message.setNodeMetrics(nodeMetricsSnapshot != null ? nodeMetricsSnapshot : new java.util.HashMap<>());

            java.util.Map<String, String> appInfoMetricsSnapshot = latestAppInfoMetrics;
            message.setAppInfoMetrics(appInfoMetricsSnapshot != null ? appInfoMetricsSnapshot : new java.util.HashMap<>());

            // Always set originalConfig and optimizedConfig, defaulting to empty map if null
            message.setOriginalConfiguration(originalConfig != null ? originalConfig : new java.util.HashMap<>());
            message.setOptimizedConfiguration(optimizedConfig != null ? optimizedConfig : new java.util.HashMap<>());

            // Attach topics list
            if (!topicsWritten.isEmpty()) {
                message.setTopics(new java.util.ArrayList<>(topicsWritten));
            }

            // When building the ClientStatsMessage, set the most impactful topic if available
            if (mostImpactfulTopic != null) {
                message.setMostImpactfulTopic(mostImpactfulTopic);
            }

            // Set language and error fields
            message.setLanguage("Java");
            message.setError(error != null ? error : "");

            String json = objectMapper.writeValueAsString(message);
            ProducerRecord<String, String> record = new ProducerRecord<>(CLIENTS_TOPIC, json);
            producer.send(record);

            // Log at INFO level that stats have been sent for this producer
            logger.debug("Producer {} stats sent: before={} bytes, after={} bytes",
                    clientId, totalBytesBefore, totalBytesAfter);
        } catch (Exception e) {
            // Convert stack trace to string
            java.io.StringWriter sw = new java.io.StringWriter();
            java.io.PrintWriter pw = new java.io.PrintWriter(sw);
            e.printStackTrace(pw);
            String stackTrace = sw.toString().replaceAll("\\r?\\n", " ");
            logger.error("[ERR-021] Failed to drain stats for client {}. Error: {} - {}. Stack trace: {}", 
                clientId, e.getClass().getName(), e.getMessage(), stackTrace);
        }
    }

    private static String normalizeBootstrapServers(String servers) {
        if (servers == null)
            return "";
        String[] parts = servers.split(",");
        java.util.Arrays.sort(parts);
        return String.join(",", parts).trim();
    }

    /**
     * Merge the latest producer-level metrics into the cached snapshot.
     * <p>
     * We <strong>merge</strong> instead of replacing the whole map so that metrics which are
     * temporarily absent (e.g. become NaN / not reported while the producer is idle)
     * still appear in the next heartbeat with their <em>last known</em> value.  Any key
     * present in {@code metrics} overwrites the previous value – even when the new
     * value is {@code 0.0}, negative or otherwise – but keys that are <em>missing</em>
     * are left untouched.
     */
    public void updateProducerMetrics(java.util.Map<String, Double> metrics) {
        if (!disabled && metrics != null) {
            latestMetrics.updateAndGet(prev -> {
                java.util.Map<String, Double> merged = new java.util.HashMap<>(prev);
                merged.putAll(metrics);
                return merged;
            });
        }
    }

    /**
     * Merge the latest per-topic metrics.  Same rationale as above, but we first
     * locate / create the nested map for each topic, then merge its individual
     * metric values.
     */
    public void updateTopicMetrics(java.util.Map<String, java.util.Map<String, Double>> topicMetrics) {
        if (!disabled && topicMetrics != null) {
            topicMetrics.forEach((topic, metricMap) -> {
                java.util.Map<String, Double> existing = latestTopicMetrics.computeIfAbsent(topic, k -> new java.util.HashMap<>());
                existing.putAll(metricMap);
            });
        }
    }

    /**
     * Merge the latest per-node metrics (broker-level statistics).  Behaviour is
     * analogous to {@link #updateTopicMetrics}.
     */
    public void updateNodeMetrics(java.util.Map<String, java.util.Map<String, Double>> nodeMetrics) {
        if (!disabled && nodeMetrics != null) {
            nodeMetrics.forEach((node, metricMap) -> {
                java.util.Map<String, Double> existing = latestNodeMetrics.computeIfAbsent(node, k -> new java.util.HashMap<>());
                existing.putAll(metricMap);
            });
        }
    }

    /**
     * Merge the latest <code>app-info</code> gauge values. These are string
     * properties (Kafka version, client id, etc.), so we store them as
     * {@code Map&lt;String,String&gt;}.  As with the numeric maps we merge to retain
     * previously-seen keys that might be absent in the current snapshot.
     */
    public void updateAppInfoMetrics(java.util.Map<String, String> appInfoMetrics) {
        if (appInfoMetrics != null && !disabled) {
            if (this.latestAppInfoMetrics == null) {
                this.latestAppInfoMetrics = new java.util.HashMap<>();
            }
            this.latestAppInfoMetrics.putAll(appInfoMetrics);
        }
    }

    public void addTopics(java.util.Collection<String> topics) {
        if (!disabled && topics != null) {
            topicsWritten.addAll(topics);
        }
    }

    public void setConfigurations(java.util.Map<String, Object> originalCfg,
            java.util.Map<String, Object> optimizedCfg) {
        if (!disabled) {
            this.originalConfig = (originalCfg != null) ? originalCfg : new java.util.HashMap<>();
            this.optimizedConfig = (optimizedCfg != null) ? optimizedCfg : new java.util.HashMap<>();
        }
    }

    public void updateMostImpactfulTopic(String topic) {
        this.mostImpactfulTopic = topic;
    }

    public void updateError(String error) {
        this.error = error;
    }

    // Coordinator class per cluster
    private static class ClusterStatsCoordinator {
        private final String bootstrapServers;
        private final Properties baseProps;
        private final CopyOnWriteArrayList<ClientStatsReporter> reporters = new CopyOnWriteArrayList<>();
        private final AtomicBoolean scheduled = new AtomicBoolean(false);

        ClusterStatsCoordinator(String bootstrapServers, Properties baseProps) {
            this.bootstrapServers = bootstrapServers;
            this.baseProps = baseProps;
        }

        void addReporter(ClientStatsReporter r) {
            reporters.add(r);
            if (scheduled.compareAndSet(false, true)) {
                scheduler.scheduleAtFixedRate(this::run, REPORT_INTERVAL_MS, REPORT_INTERVAL_MS, TimeUnit.MILLISECONDS);
            }
        }

        private void run() {
            if (reporters.isEmpty())
                return;
            
            // Log the configuration before creating producer
            if (SuperstreamLogger.isDebugEnabled()) {
                StringBuilder configLog = new StringBuilder("Creating internal ClientStatsReporter producer with configuration: ");
                baseProps.forEach((key, value) -> {
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
            
            try (Producer<String, String> producer = new KafkaProducer<>(baseProps)) {
                for (ClientStatsReporter r : reporters) {
                    r.drainInto(producer);
                }
                producer.flush();
            } catch (Exception e) {
                // Convert stack trace to string
                java.io.StringWriter sw = new java.io.StringWriter();
                java.io.PrintWriter pw = new java.io.PrintWriter(sw);
                e.printStackTrace(pw);
                String stackTrace = sw.toString().replaceAll("\\r?\\n", " ");
                logger.error("[ERR-022] Cluster stats coordinator failed for {}, please make sure the Kafka user has read/write/describe permissions on superstream.* topics. Error: {} - {}. Stack trace: {}", 
                    bootstrapServers, e.getClass().getName(), e.getMessage(), stackTrace);
            }
        }
    }

    /**
     * Deactivate this reporter so that it no longer emits statistics.  The reporter
     * remains registered in the coordinator list but {@link #drainInto} becomes a
     * no-op which is inexpensive and avoids extra allocations.
     */
    public void deactivate() {
        registered.set(false);
    }
}