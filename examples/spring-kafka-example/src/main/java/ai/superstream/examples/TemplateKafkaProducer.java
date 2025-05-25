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
        // Delegate to the regular KafkaProducer(Map<String,Object>) constructor
        // by extracting the configuration from the template's producer factory.
        super(kafkaTemplate.getProducerFactory().getConfigurationProperties());
    }
} 