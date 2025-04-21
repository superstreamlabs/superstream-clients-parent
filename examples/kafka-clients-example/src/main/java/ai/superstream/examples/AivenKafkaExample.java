package ai.superstream.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Example application that uses the Kafka Clients API to produce messages.
 * Run with:
 * java -javaagent:path/to/superstream-clients-1.0.0.jar -Dlogback.configurationFile=logback.xml -jar kafka-clients-example-1.0.0-jar-with-dependencies.jar
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
public class AivenKafkaExample {
    private static final Logger logger = LoggerFactory.getLogger(AivenKafkaExample.class);

    public static void main(String[] args) {
        // Get bootstrap servers from environment variable or use default
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            bootstrapServers = "superstream-test-superstream-3591.k.aivencloud.com:18837";
        }

        // Configure the producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put("client.id", "superstream-example-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.type", "PEM");
        props.put("ssl.keystore.type", "PEM");

        Path caPath = Paths.get("/Users/shohamroditi/superstream/superstream-clients-parent/examples/kafka-clients-example/src/main/resources/crets/ca.pem");
        Path clientCertPath = Paths.get("/Users/shohamroditi/superstream/superstream-clients-parent/examples/kafka-clients-example/src/main/resources/crets/client.cert.pem");
        Path clientKeyPath = Paths.get("/Users/shohamroditi/superstream/superstream-clients-parent/examples/kafka-clients-example/src/main/resources/crets/client.pk8.pem");


        props.put("ssl.truststore.certificates", caPath.toAbsolutePath().toString());
        props.put("ssl.keystore.certificate.chain", clientCertPath.toAbsolutePath().toString());
        props.put("ssl.keystore.key", clientKeyPath.toAbsolutePath().toString());


        logger.info("client cert path={}", clientCertPath.toAbsolutePath().toString());
        logger.info("ca path={}", caPath.toAbsolutePath().toString());
        logger.info("client key path={}", clientKeyPath.toAbsolutePath().toString());
        props.put("ssl.endpoint.identification.algorithm", "");



//        props.put("ssl.truststore.certificates", "src/main/resources/crets/ca.pem");
//        props.put("ssl.keystore.certificate.chain", "src/main/resources/crets/client.cert.pem");
//        props.put("ssl.keystore.key", "/Users/shohamroditi/superstream/superstream-clients-parent/examples/kafka-clients-example/src/main/resources/crets/client.key.pem");
//        props.put("ssl.keystore.key", "/Users/shohamroditi/superstream/superstream-clients-parent/examples/kafka-clients-example/src/main/resources/crets/client.pk81.pem");
//        props.put("ssl.keystore.key", "src/main/resources/crets/client.pk81.pem");

        // Set some basic configuration
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        logger.info("Creating producer with bootstrap servers: {}", bootstrapServers);
        logger.info("Original producer configuration:");
        props.forEach((k, v) -> logger.info("  {} = {}", k, v));

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            // The Superstream Agent should have intercepted the producer creation
            // and potentially optimized the configuration

            // Log the actual configuration used by the producer
            logger.info("Actual producer configuration (after potential Superstream optimization):");

            // Get the actual configuration from the producer via reflection
            java.lang.reflect.Field configField = producer.getClass().getDeclaredField("producerConfig");
            configField.setAccessible(true);
            org.apache.kafka.clients.producer.ProducerConfig actualConfig =
                    (org.apache.kafka.clients.producer.ProducerConfig) configField.get(producer);

            // Get the values for key configuration parameters
            logger.info("  compression.type = {}", actualConfig.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG));
            logger.info("  batch.size = {}", actualConfig.getInt(ProducerConfig.BATCH_SIZE_CONFIG));

            // Send a test message
            String topic = "example-topic";
            String key = "test-key";
            String value = "Hello, Superstream!";


            logger.info("Sending message to topic {}: key={}, value={}", topic, key, value);
            producer.send(new ProducerRecord<>(topic, key, value)).get();
            logger.info("Message sent successfully!");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while sending message", e);
        } catch (ExecutionException e) {
            logger.error("Error sending message", e);
        } catch (Exception e) {
            logger.error("Unexpected error", e);
        }
    }

//    private static String writeToTempFile(String prefix, String content) throws Exception {
//        logger.info("prefix: {}", prefix);
//        File tempFile = File.createTempFile(prefix, ".pem");
//        content = content.replace("\\n", "\n");
//        try (FileWriter writer = new FileWriter(tempFile)) {
//            writer.write(content);
//        }
//        logger.info("Wrote temp PEM file: {}", tempFile.getAbsolutePath());
//
//        tempFile.deleteOnExit();
//        return tempFile.getAbsolutePath();
//    }
}
