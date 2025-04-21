package ai.superstream.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Example Spring Boot application that uses Spring Kafka to produce messages.
 * Run with:
 * java -javaagent:path/to/superstream-clients-1.0.0.jar -jar spring-kafka-example-1.0.0.jar
 *
 * Prerequisites:
 * 1. A Kafka server with the following topics:
 *    - superstream.metadata_v1 - with a configuration message
 *    - superstream.clients - for client reports
 *    - example-topic - for test messages
 *
 * Environment variables:
 * - KAFKA_BOOTSTRAP_SERVERS: The Kafka bootstrap servers (default: localhost:9092)
 * - SUPERSTREAM_TOPICS_LIST: Comma-separated list of topics to optimize for (default: example-topic)
 */
@SpringBootApplication
public class SpringKafkaExampleApplication {
    private static final Logger logger = LoggerFactory.getLogger(SpringKafkaExampleApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaExampleApplication.class, args);
    }

    @Bean
    public CommandLineRunner runner(KafkaTemplate<String, String> kafkaTemplate) {
        return args -> {
            String topic = "example-topic";
            String key = "test-key";
            String value = "Hello, Superstream with Spring Kafka!";

            logger.info("Sending message to topic {}: key={}, value={}", topic, key, value);
            kafkaTemplate.send(topic, key, value).get();
            logger.info("Message sent successfully!");

            // Wait a moment to allow all logs to be printed
            Thread.sleep(1000);
            System.exit(0);
        };
    }
}