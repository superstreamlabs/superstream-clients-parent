package ai.superstream.core;

import ai.superstream.model.MetadataMessage;
import ai.superstream.util.SuperstreamLogger;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import ai.superstream.agent.KafkaProducerInterceptor;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

/**
 * Consumes messages from the superstream.metadata_v1 topic.
 */
public class MetadataConsumer {
    private static final SuperstreamLogger logger = SuperstreamLogger.getLogger(MetadataConsumer.class);
    private static final String METADATA_TOPIC = "superstream.metadata_v1";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Get the metadata message from the Kafka cluster.
     *
     * @param bootstrapServers The Kafka bootstrap servers
     * @return The metadata message, or null if there was an error
     */
    public MetadataMessage getMetadataMessage(String bootstrapServers, Properties originalClientProperties) {
        Properties properties = new Properties();

        // Copy all authentication-related and essential properties from the original
        // client
        copyAuthenticationProperties(originalClientProperties, properties);

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, KafkaProducerInterceptor.SUPERSTREAM_LIBRARY_PREFIX + "metadata-consumer");

        try (Consumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            // Check if the metadata topic exists
            Set<String> topics = consumer.listTopics().keySet();
            if (!topics.contains(METADATA_TOPIC)) {
                logger.warn("The {} topic does not exist on the Kafka cluster at {}", METADATA_TOPIC, bootstrapServers);
                return null;
            }

            // Assign the metadata topic
            TopicPartition partition = new TopicPartition(METADATA_TOPIC, 0);
            consumer.assign(Collections.singletonList(partition));

            // Seek to the end and get the current offset
            consumer.seekToEnd(Collections.singletonList(partition));
            long endOffset = consumer.position(partition);

            if (endOffset == 0) {
                logger.warn("The {} topic is empty", METADATA_TOPIC);
                return null;
            }

            // Seek to the last message
            consumer.seek(partition, endOffset - 1);

            // Poll for the message
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            if (records.isEmpty()) {
                logger.warn("Failed to retrieve a message from the {} topic", METADATA_TOPIC);
                return null;
            }
            logger.info("Successfully retrieved a message from the {} topic", METADATA_TOPIC);

            // Parse the message
            String json = records.iterator().next().value();
            return objectMapper.readValue(json, MetadataMessage.class);
        } catch (IOException e) {
            logger.error("Failed to parse the metadata message", e);
            return null;
        } catch (Exception e) {
            logger.error("Failed to retrieve the metadata message", e);
            return null;
        }
    }

    // Helper method to copy authentication properties
    private void copyAuthenticationProperties(Properties source, Properties destination) {
        if (source == null || destination == null) {
            logger.warn("Cannot copy authentication properties: source or destination is null");
            return;
        }
        // Authentication-related properties
        String[] authProps = {
                // Security protocol
                "security.protocol",

                // SSL properties
                "ssl.truststore.location", "ssl.truststore.password",
                "ssl.keystore.location", "ssl.keystore.password",
                "ssl.key.password", "ssl.endpoint.identification.algorithm",
                "ssl.truststore.type", "ssl.keystore.type", "ssl.secure.random.implementation",
                "ssl.enabled.protocols", "ssl.cipher.suites",

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
                "reconnect.backoff.ms", "reconnect.backoff.max.ms"
        };

        // Copy all authentication properties if they exist in the source
        for (String prop : authProps) {
            if (source.containsKey(prop)) {
                destination.put(prop, source.get(prop));
            }
        }
    }
}