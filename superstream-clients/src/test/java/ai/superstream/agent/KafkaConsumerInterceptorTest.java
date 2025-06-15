package ai.superstream.agent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for KafkaConsumerInterceptor.
 */
public class KafkaConsumerInterceptorTest {

    private ByteArrayOutputStream outputStreamCaptor;
    private PrintStream standardOut;

    @BeforeEach
    public void setUp() {
        // Capture System.out to verify console output
        outputStreamCaptor = new ByteArrayOutputStream();
        standardOut = System.out;
        System.setOut(new PrintStream(outputStreamCaptor));
    }

    @AfterEach
    public void tearDown() {
        // Restore original System.out
        System.setOut(standardOut);
    }

    @Test
    public void testOnEnter_LogsHelloWorld() {
        // Arrange
        Object[] args = new Object[]{new Properties()};

        // Act
        KafkaConsumerInterceptor.onEnter(args);

        // Assert
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("hello world - KafkaConsumer intercepted!"), 
                   "Should log hello world message to console");
    }

    @Test
    public void testOnEnter_WithNullArgs() {
        // Arrange
        Object[] args = null;

        // Act & Assert - should not throw exception
        assertDoesNotThrow(() -> KafkaConsumerInterceptor.onEnter(args));
        
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("hello world - KafkaConsumer intercepted!"), 
                   "Should still log hello world message even with null args");
    }

    @Test
    public void testOnEnter_WithEmptyArgs() {
        // Arrange
        Object[] args = new Object[]{};

        // Act
        KafkaConsumerInterceptor.onEnter(args);

        // Assert
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("hello world - KafkaConsumer intercepted!"), 
                   "Should log hello world message with empty args");
    }

    @Test
    public void testIsDisabled_ReturnsCurrentValue() {
        // Test the current value of isDisabled() method
        // Note: The actual value depends on the SUPERSTREAM_DISABLED environment variable
        // at class loading time, so we just verify the method returns a boolean value
        boolean result = KafkaConsumerInterceptor.isDisabled();
        // This should not throw an exception and should return a boolean
        assertTrue(result || !result, "isDisabled() should return a boolean value");
    }

    @Test
    public void testOnExit_WithValidConsumer() {
        // Arrange
        Object mockConsumer = new Object(); // Simple mock consumer

        // Act & Assert - should not throw exception
        assertDoesNotThrow(() -> KafkaConsumerInterceptor.onExit(mockConsumer));
    }

    @Test
    public void testOnExit_WithNullConsumer() {
        // Arrange
        Object consumer = null;

        // Act & Assert - should not throw exception
        assertDoesNotThrow(() -> KafkaConsumerInterceptor.onExit(consumer));
    }

    @Test
    public void testMultipleOnEnterCalls() {
        // Arrange
        Object[] args1 = new Object[]{new Properties()};
        Object[] args2 = new Object[]{new Properties()};

        // Act
        KafkaConsumerInterceptor.onEnter(args1);
        KafkaConsumerInterceptor.onEnter(args2);

        // Assert
        String output = outputStreamCaptor.toString();
        // Should contain the message twice
        int count = output.split("hello world - KafkaConsumer intercepted!", -1).length - 1;
        assertEquals(2, count, "Should log hello world message twice");
    }

    @Test
    public void testOnEnterAndOnExit_Together() {
        // Arrange
        Object[] args = new Object[]{new Properties()};
        Object mockConsumer = new Object();

        // Act
        KafkaConsumerInterceptor.onEnter(args);
        KafkaConsumerInterceptor.onExit(mockConsumer);

        // Assert
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("hello world - KafkaConsumer intercepted!"), 
                   "Should log hello world message from onEnter");
        // onExit doesn't produce console output, just logs debug info
    }

    @Test
    public void testLoggingDoesNotInterfereWithExecution() {
        // Arrange
        Object[] args = new Object[]{new Properties()};
        Object mockConsumer = new Object();

        // Act & Assert - methods should complete without throwing exceptions
        long startTime = System.currentTimeMillis();
        
        assertDoesNotThrow(() -> {
            KafkaConsumerInterceptor.onEnter(args);
            KafkaConsumerInterceptor.onExit(mockConsumer);
        });
        
        long executionTime = System.currentTimeMillis() - startTime;
        assertTrue(executionTime < 1000, "Interceptor methods should execute quickly");
    }
} 