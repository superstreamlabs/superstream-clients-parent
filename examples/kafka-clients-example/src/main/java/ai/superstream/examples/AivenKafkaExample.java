package ai.superstream.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Example application that uses the Kafka Clients API to produce messages securely over SSL (TLS).
 *
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
 * - KAFKA_BOOTSTRAP_SERVERS: The Kafka bootstrap servers (default: superstream-test-superstream-3591.k.aivencloud.com:18837)
 * - SUPERSTREAM_TOPICS_LIST: Comma-separated list of topics to optimize for (default: example-topic)
 *
 * SSL Requirements:
 * To establish a secure SSL/TLS connection with Kafka brokers, you must generate and configure:
 *
 *   - CA certificate (`ca.pem`) — the trusted certificate authority (from the Kafka provider)
 *   - Client certificate (`client.cert.pem`) — identifies the client application
 *   - Client private key (`client.pk8.pem`) — the private key paired with the client certificate
 *
 * Steps to prepare truststore and keystore:
 *
 * Step 1: Create a Truststore (truststore.jks) containing the CA certificate:
 *     keytool -importcert \
 *         -trustcacerts \
 *         -alias aiven-ca \
 *         -file /path/to/ca.pem \
 *         -keystore /path/to/truststore.jks \
 *         -storepass changeit
 *
 * Step 2: Create a Keystore (keystore.p12) containing the client certificate and private key:
 *     openssl pkcs12 -export \
 *         -in /path/to/client.cert.pem \
 *         -inkey /path/to/client.pk8.pem \
 *         -out /path/to/keystore.p12 \
 *         -name kafka-client \
 *         -passout pass:changeit
 *
 * Notes:
 * - The Truststore (`truststore.jks`) ensures the client trusts the Kafka broker's SSL certificate.
 * - The Keystore (`keystore.p12`) provides client authentication (mutual TLS) toward the broker.
 * - Both Truststore and Keystore must be correctly configured for SSL handshake to succeed.
 *
 * Security Advice:
 * - The password must be at least 6 characters long and must match the password configured in your Java Kafka client.
 * - You must configure these passwords properly in your Java Kafka client (`ssl.truststore.password`, `ssl.keystore.password`).
 * - Use strong passwords instead of "changeit" in production environments.
 * - Protect your truststore.jks and keystore.p12 files carefully; leaking them would compromise your SSL security.
 */

public class AivenKafkaExample {
    private static final Logger logger = LoggerFactory.getLogger(AivenKafkaExample.class);

    // === Configuration Constants ===
    private static final String DEFAULT_BOOTSTRAP_SERVERS =
            "superstream-test-superstream-3591.k.aivencloud.com:18837";
    // Replace with full absolute path to your generated truststore.jks
    private static final String TRUSTSTORE_LOCATION = "/absolute/path/to/truststore.jks";
    // The password must be at least 6 characters long and must match the password configured in your Java Kafka client.
    private static final String TRUSTSTORE_PASSWORD = "changeit";
    // Replace with full absolute path to your generated keystore.p12
    private static final String KEYSTORE_KEY_PATH = "/absolute/path/to/keystore.p12";
    private static final String SECURITY_PROTOCOL = "SSL";
    private static final String TRUSTSTORE_TYPE = "JKS";
    private static  final String KEYSTORE_TYPE = "PKCS12";

    private static final String CLIENT_ID = "superstream-example-producer";
    private static final String COMPRESSION_TYPE = "none";
    private static final Integer BATCH_SIZE = 15;

    private static final String TOPIC_NAME = "example-topic";
    private static final String MESSAGE_KEY = "test-key";
    private static final String MESSAGE_VALUE = generateLargeCompressibleMessage();

    public static void main(String[] args) {
        // Get bootstrap servers from environment variable or use default
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
        }

        // Configure the producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put("client.id", CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put("security.protocol", SECURITY_PROTOCOL);
        props.put("ssl.truststore.location", TRUSTSTORE_LOCATION);
        props.put("ssl.truststore.password", TRUSTSTORE_PASSWORD);
        props.put("ssl.truststore.type", TRUSTSTORE_TYPE);
        props.put("ssl.keystore.location",KEYSTORE_KEY_PATH );
        props.put("ssl.keystore.password", TRUSTSTORE_PASSWORD);
        props.put("ssl.keystore.type", KEYSTORE_TYPE);
        // Set some basic configuration
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE);

        // Pass the immutable map directly to the KafkaProducer constructor
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        long recordCount = 10; // Number of messages to send
        try {
            while (true) {
                // Send 50 large messages to see compression benefits
                for (int i = 1; i <= recordCount; i++) {
                    String messageKey = MESSAGE_KEY + "-" + i;
                    String messageValue = MESSAGE_VALUE + "-" + i + "-" + System.currentTimeMillis();
                    producer.send(new ProducerRecord<>(TOPIC_NAME, messageKey, messageValue));
                }

                producer.flush();
                Thread.sleep(150000);
            }
        } catch (Exception e) {
            logger.error("Error sending message", e);
        } finally {
            producer.close();
        }
    }

    private static String generateLargeCompressibleMessage() {
        // Return a 1KB JSON string with repeating data that can be compressed well
        StringBuilder json = new StringBuilder();
        json.append("{\n");
        json.append("  \"metadata\": {\n");
        json.append("    \"id\": \"12345\",\n");
        json.append("    \"type\": \"example\",\n");
        json.append("    \"timestamp\": 1635954438000\n");
        json.append("  },\n");
        json.append("  \"data\": {\n");
        json.append("    \"metrics\": [\n");

        // Add repeating metrics data to reach ~1KB
        for (int i = 0; i < 15; i++) {
            if (i > 0)
                json.append(",\n");
            json.append("      {\n");
            json.append("        \"name\": \"metric").append(i).append("\",\n");
            json.append("        \"value\": ").append(i * 10).append(",\n");
            json.append("        \"tags\": [\"tag1\", \"tag2\", \"tag3\"],\n");
            json.append("        \"properties\": {\n");
            json.append("          \"property1\": \"value1\",\n");
            json.append("          \"property2\": \"value2\"\n");
            json.append("        }\n");
            json.append("      }");
        }

        json.append("\n    ]\n");
        json.append("  },\n");
        json.append("  \"config\": {\n");
        json.append("    \"sampling\": \"full\",\n");
        json.append("    \"retention\": \"30d\",\n");
        json.append("    \"compression\": true,\n");
        json.append("    \"encryption\": false\n");
        json.append("  }\n");
        json.append("}");

        String result = json.toString();
        logger.debug("Generated compressible message of {} bytes", result.getBytes(StandardCharsets.UTF_8).length);

        return result;
    }
}
