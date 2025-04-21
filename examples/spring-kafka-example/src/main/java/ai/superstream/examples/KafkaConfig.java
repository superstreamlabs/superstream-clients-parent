package ai.superstream.examples;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        // Get bootstrap servers from environment variable or use default
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            bootstrapServers = "localhost:9092";
        }

        // Configure the producer
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.CLIENT_ID_CONFIG, "superstream-spring-producer");
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Set some basic configuration
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 0);

        logger.info("Creating Spring Kafka producer with bootstrap servers: {}", bootstrapServers);
        logger.info("Original producer configuration:");
        configProps.forEach((k, v) -> logger.info("  {} = {}", k, v));

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        KafkaTemplate<String, String> template = new KafkaTemplate<>(producerFactory());

        // Log the configuration after the producer is created
        logger.info("KafkaTemplate created, Superstream optimizations would have been applied");

        return template;
    }
}