package ai.superstream.examples;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MskKafkaExample {
    private static final Logger logger = LoggerFactory.getLogger(MskKafkaExample.class);

    // === Configuration Constants ===
    private static final String DEFAULT_BOOTSTRAP_SERVERS =
            "b-23-public.superstreamstgmsk.0y88si.c2.kafka.eu-central-1.amazonaws.com:9198," +
                    "b-24-public.superstreamstgmsk.0y88si.c2.kafka.eu-central-1.amazonaws.com:9198," +
                    "b-2-public.superstreamstgmsk.0y88si.c2.kafka.eu-central-1.amazonaws.com:9198";

    // AWS IAM Credentials
    private static final String AWS_ACCESS_KEY_ID = "<your-access-key>";
    private static final String AWS_SECRET_ACCESS_KEY = "<your-secret-key>";

    private static final String SECURITY_PROTOCOL = "SASL_SSL";
    private static final String SASL_MECHANISM = "AWS_MSK_IAM";
    private static final String SASL_JAAS_CONFIG = "software.amazon.msk.auth.iam.IAMLoginModule required;";
    private static final String SASL_CALLBACK_HANDLER = "software.amazon.msk.auth.iam.IAMClientCallbackHandler";

    private static final String CLIENT_ID = "superstream-example-producer";
    private static final String COMPRESSION_TYPE = "gzip";
    private static final int BATCH_SIZE = 16384;

    private static final String TOPIC_NAME = "example-topic";
    private static final String MESSAGE_KEY = "test-key";
    private static final String MESSAGE_VALUE = "Hello, Superstream!";


    public static void main(String[] args) {
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
        }

        System.setProperty("aws.accessKeyId", AWS_ACCESS_KEY_ID);
        System.setProperty("aws.secretKey", AWS_SECRET_ACCESS_KEY);

        // Configure the producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put("client.id", CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Security
        props.put("security.protocol", SECURITY_PROTOCOL);
        props.put("sasl.mechanism", SASL_MECHANISM);
        props.put("sasl.jaas.config", SASL_JAAS_CONFIG);
        props.put("sasl.client.callback.handler.class", SASL_CALLBACK_HANDLER);

        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE);

        logger.info("Creating producer with bootstrap servers: {}", bootstrapServers);
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

            logger.info("  compression.type = {}", actualConfig.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG));
            logger.info("  batch.size = {}", actualConfig.getInt(ProducerConfig.BATCH_SIZE_CONFIG));

            // Send a test message
            logger.info("Sending message to topic {}: key={}, value={}", TOPIC_NAME, MESSAGE_KEY, MESSAGE_VALUE);
            producer.send(new ProducerRecord<>(TOPIC_NAME, MESSAGE_KEY, MESSAGE_VALUE)).get();
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
}
