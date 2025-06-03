package ai.superstream.util;

import java.util.Properties;

/**
 * Utility class for Kafka properties management.
 */
public class KafkaPropertiesUtils {
    private static final SuperstreamLogger logger = SuperstreamLogger.getLogger(KafkaPropertiesUtils.class);

    /**
     * Copies essential client configuration properties from source to destination.
     * This ensures internal Kafka clients have the same security, network, and connection
     * configurations as the user's Kafka clients.
     *
     * @param source Source properties to copy from
     * @param destination Destination properties to copy to
     */
    public static void copyClientConfigurationProperties(Properties source, Properties destination) {
        if (source == null || destination == null) {
            logger.warn("Cannot copy client configuration properties: source or destination is null");
            return;
        }
        
        // Client configuration properties to copy
        String[] configProps = {
                // Security protocol
                "security.protocol",

                // SSL properties
                "ssl.truststore.location", "ssl.truststore.password",
                "ssl.keystore.location", "ssl.keystore.password",
                "ssl.key.password", "ssl.endpoint.identification.algorithm",
                "ssl.truststore.type", "ssl.keystore.type", "ssl.secure.random.implementation",
                "ssl.enabled.protocols", "ssl.cipher.suites", "ssl.protocol",

                // SASL properties
                "sasl.mechanism", "sasl.jaas.config",
                "sasl.client.callback.handler.class", "sasl.login.callback.handler.class",
                "sasl.login.class", "sasl.kerberos.service.name",
                "sasl.kerberos.kinit.cmd", "sasl.kerberos.ticket.renew.window.factor",
                "sasl.kerberos.ticket.renew.jitter", "sasl.kerberos.min.time.before.relogin",
                "sasl.login.refresh.window.factor", "sasl.login.refresh.window.jitter",
                "sasl.login.refresh.min.period.seconds", "sasl.login.refresh.buffer.seconds",

                // Other important properties to preserve
                "request.timeout.ms", "retry.backoff.ms", "connections.max.idle.ms",
                "reconnect.backoff.ms", "reconnect.backoff.max.ms",
                
                // DNS lookup configuration - critical for Consul/proxy scenarios
                "client.dns.lookup",
                
                // Socket timeout properties - important for proxy/load balancer scenarios
                "socket.connection.setup.timeout.ms", "socket.connection.setup.timeout.max.ms",
                
                // Metadata refresh properties - important for dynamic broker discovery
                "metadata.max.age.ms", "metadata.max.idle.ms",
                
                // Retry and delivery timeout - important for handling transient failures
                "retries", "delivery.timeout.ms"
        };

        // Copy all properties if they exist in the source
        for (String prop : configProps) {
            if (source.containsKey(prop)) {
                destination.put(prop, source.get(prop));
            }
        }
    }
} 