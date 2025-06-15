package ai.superstream.examples;

import akka.actor.ActorSystem;
import akka.kafka.ProducerMessage;
import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.Producer;
import akka.stream.javadsl.Source;
import akka.NotUsed;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.Properties;

/**
 * Example application that uses Akka Kafka to produce messages.
 * Run with:
 * java -javaagent:path/to/superstream-clients-1.0.0.jar -Dlogback.configurationFile=logback.xml -jar akka-kafka-example-1.0.0-jar-with-dependencies.jar
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
public class AkkaKafkaExample {
    private static final Logger logger = LoggerFactory.getLogger(AkkaKafkaExample.class);

    public static void main(String[] args) {
        // Get bootstrap servers from environment variable or use default
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            bootstrapServers = "localhost:9092";
        }

        // Create the actor system
        ActorSystem system = ActorSystem.create("akka-kafka-example");

        try {
            // Configure the producer
            Map<String, Object> configProps = new HashMap<>();
            configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            
            org.apache.kafka.clients.producer.Producer<String, String> kafkaProducer = new KafkaProducer<>(configProps);

            // Create producer settings with the producer
            ProducerSettings<String, String> producerSettings = ProducerSettings
                    .create(system, new StringSerializer(), new StringSerializer())
                    .withProducer(kafkaProducer);

            // Send a test message
            String topic = "example-topic";
            String key = "test-key";
            String value = "Hello, Superstream with Akka Kafka!";

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            logger.info("Sending message to topic {}: key={}, value={}", topic, key, value);

            // Create a source with a single message and send it to Kafka
            CompletableFuture<Void> completionFuture = Source.single(ProducerMessage.single(record))
                    .via(Producer.flexiFlow(producerSettings))
                    .runForeach(result -> {
                        if (result instanceof ProducerMessage.Results) {
                            ProducerMessage.Results<String, String, NotUsed> res = 
                                (ProducerMessage.Results<String, String, NotUsed>) result;
                            logger.info("Message sent successfully");
                        }
                    }, system)
                    .toCompletableFuture()
                    .thenApply(done -> null);

            // Wait for the message to be sent
            completionFuture.get(10, TimeUnit.SECONDS);
            logger.info("Message sent successfully!");

            // Shut down the actor system
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("Error in Akka Kafka example", e);
        } finally {
            // Ensure the actor system is terminated
            if (!system.whenTerminated().isCompleted()) {
                system.terminate();
                try {
                    system.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    logger.error("Error terminating actor system", e);
                }
            }
        }
    }
}