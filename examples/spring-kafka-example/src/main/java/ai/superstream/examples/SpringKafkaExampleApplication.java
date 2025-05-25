package ai.superstream.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import ai.superstream.examples.config.KafkaProperties;

@SpringBootApplication
@EnableConfigurationProperties(KafkaProperties.class)
public class SpringKafkaExampleApplication {
    private static final Logger logger = LoggerFactory.getLogger(SpringKafkaExampleApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaExampleApplication.class, args);
    }

    @Bean
    public CommandLineRunner runner(KafkaTemplate<String, String> kafkaTemplate, KafkaProperties kafkaProperties) {
        return args -> {
            try {
                String topic = kafkaProperties.getTopic();
                String key = "test-key";
                String value = "Hello, Superstream with Spring Kafka!";

                logger.info("Sending message to topic {}: key={}, value={}", topic, key, value);
                try (TemplateKafkaProducer producer = new TemplateKafkaProducer(kafkaTemplate)) {
                    producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, key, value)).get();
                }
                logger.info("Message sent successfully!");

                // Wait a moment to allow all logs to be printed
                Thread.sleep(1000);
            } catch (Exception e) {
                logger.error("Error sending message", e);
            }
            System.exit(0);
        };
    }
}