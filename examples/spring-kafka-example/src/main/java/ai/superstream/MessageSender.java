package ai.superstream;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class MessageSender implements CommandLineRunner {

    private final KafkaProducerService producerService;

    public MessageSender(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    @Override
    public void run(String... args) {
        // Send a simple message
        producerService.sendMessage("Hello from Spring Kafka Example!");
        
        // Send a message with a key
        producerService.sendMessageWithKey("user-1", "Message for user 1");
        producerService.sendMessageWithKey("user-2", "Message for user 2");
        
        System.out.println("Messages sent successfully!");
    }
} 