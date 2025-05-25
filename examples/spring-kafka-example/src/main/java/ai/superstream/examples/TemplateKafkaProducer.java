package ai.superstream.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * A thin wrapper around KafkaProducer that accepts a Spring {@link KafkaTemplate}
 * in its constructor. The sole purpose of this class is to reproduce the
 * scenario where the Superstream {@code KafkaProducerInterceptor} receives a
 * single constructor argument of type {@code KafkaTemplate}.
 *
 * <p>Normal applications would not need this class; it exists only for
 * testing/demo of the interceptor.</p>
 */
public class TemplateKafkaProducer extends KafkaProducer<String, String> {

    public TemplateKafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
        // Spring returns an unmodifiable Map; create a mutable copy so the
        // Superstream optimiser (and KafkaProducer itself) can adjust values.
        super(toProperties(kafkaTemplate.getProducerFactory().getConfigurationProperties()));
    }

    private static java.util.Properties toProperties(java.util.Map<String, Object> config) {
        java.util.Map<String,Object> mutable = toMutableMap(config);
        java.util.Properties props = new java.util.Properties();
        props.putAll(mutable);
        return props;
    }

    private static java.util.Map<String, Object> toMutableMap(java.util.Map<String, Object> config) {
        return (java.util.Map<String, Object>) config;
    }
} 