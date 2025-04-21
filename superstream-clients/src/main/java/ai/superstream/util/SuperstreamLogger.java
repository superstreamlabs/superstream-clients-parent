package ai.superstream.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom logger for the Superstream library.
 * All logs will have the "superstream" prefix.
 */
public class SuperstreamLogger {
    private static final String PREFIX = "superstream";
    private final Logger logger;

    private SuperstreamLogger(Class<?> clazz) {
        this.logger = LoggerFactory.getLogger(clazz);
    }

    /**
     * Get a logger for the specified class.
     *
     * @param clazz The class to get the logger for
     * @return A new SuperstreamLogger instance
     */
    public static SuperstreamLogger getLogger(Class<?> clazz) {
        return new SuperstreamLogger(clazz);
    }

    /**
     * Add the superstream prefix to a log message.
     *
     * @param message The original message
     * @return The message with prefix
     */
    private String withPrefix(String message) {
        return "[" + PREFIX + "] " + message;
    }

    /**
     * Log an info message with the superstream prefix.
     *
     * @param message The message to log
     */
    public void info(String message) {
        logger.info(withPrefix(message));
    }

    /**
     * Log an info message with parameters and the superstream prefix.
     *
     * @param message The message to log
     * @param args The parameters for the message
     */
    public void info(String message, Object... args) {
        logger.info(withPrefix(message), args);
    }

    /**
     * Log a warning message with the superstream prefix.
     *
     * @param message The message to log
     */
    public void warn(String message) {
        logger.warn(withPrefix(message));
    }

    /**
     * Log a warning message with parameters and the superstream prefix.
     *
     * @param message The message to log
     * @param args The parameters for the message
     */
    public void warn(String message, Object... args) {
        logger.warn(withPrefix(message), args);
    }

    /**
     * Log an error message with the superstream prefix.
     *
     * @param message The message to log
     */
    public void error(String message) {
        logger.error(withPrefix(message));
    }

    /**
     * Log an error message with parameters and the superstream prefix.
     *
     * @param message The message to log
     * @param args The parameters for the message
     */
    public void error(String message, Object... args) {
        logger.error(withPrefix(message), args);
    }

    /**
     * Log an error message with an exception and the superstream prefix.
     *
     * @param message The message to log
     * @param throwable The exception to log
     */
    public void error(String message, Throwable throwable) {
        logger.error(withPrefix(message), throwable);
    }

    /**
     * Log a debug message with the superstream prefix.
     *
     * @param message The message to log
     */
    public void debug(String message) {
        logger.debug(withPrefix(message));
    }

    /**
     * Log a debug message with parameters and the superstream prefix.
     *
     * @param message The message to log
     * @param args The parameters for the message
     */
    public void debug(String message, Object... args) {
        logger.debug(withPrefix(message), args);
    }
}