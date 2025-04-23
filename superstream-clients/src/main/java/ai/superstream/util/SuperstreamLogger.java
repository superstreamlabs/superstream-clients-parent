package ai.superstream.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom logger for the Superstream library that falls back to System.out/System.err
 * if no SLF4J implementation is available.
 */
public class SuperstreamLogger {
    private static final String PREFIX = "superstream";
    private final Logger logger;
    private final String loggerName;
    private static boolean slf4jAvailable = false;

    static {
        try {
            // Try to detect if SLF4J implementation is available
            Class.forName("org.slf4j.impl.StaticLoggerBinder");
            slf4jAvailable = true;
        } catch (ClassNotFoundException e) {
            System.out.println("[superstream] No SLF4J implementation found. Falling back to System.out logging.");
            slf4jAvailable = false;
        }
    }

    private SuperstreamLogger(Class<?> clazz) {
        this.logger = LoggerFactory.getLogger(clazz);
        this.loggerName = clazz.getSimpleName();
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
        if (slf4jAvailable) {
            logger.info(withPrefix(message));
        } else {
            System.out.println(loggerName + " INFO: " + withPrefix(message));
        }
    }

    /**
     * Log an info message with parameters and the superstream prefix.
     *
     * @param message The message to log
     * @param args The parameters for the message
     */
    public void info(String message, Object... args) {
        if (slf4jAvailable) {
            logger.info(withPrefix(message), args);
        } else {
            System.out.println(loggerName + " INFO: " + withPrefix(formatMessage(message, args)));
        }
    }

    /**
     * Log a warning message with the superstream prefix.
     *
     * @param message The message to log
     */
    public void warn(String message) {
        if (slf4jAvailable) {
            logger.warn(withPrefix(message));
        } else {
            System.out.println(loggerName + " WARN: " + withPrefix(message));
        }
    }

    /**
     * Log a warning message with parameters and the superstream prefix.
     *
     * @param message The message to log
     * @param args The parameters for the message
     */
    public void warn(String message, Object... args) {
        if (slf4jAvailable) {
            logger.warn(withPrefix(message), args);
        } else {
            System.out.println(loggerName + " WARN: " + withPrefix(formatMessage(message, args)));
        }
    }

    /**
     * Log an error message with the superstream prefix.
     *
     * @param message The message to log
     */
    public void error(String message) {
        if (slf4jAvailable) {
            logger.error(withPrefix(message));
        } else {
            System.err.println(loggerName + " ERROR: " + withPrefix(message));
        }
    }

    /**
     * Log an error message with parameters and the superstream prefix.
     *
     * @param message The message to log
     * @param args The parameters for the message
     */
    public void error(String message, Object... args) {
        if (slf4jAvailable) {
            logger.error(withPrefix(message), args);
        } else {
            System.err.println(loggerName + " ERROR: " + withPrefix(formatMessage(message, args)));
        }
    }

    /**
     * Log an error message with an exception and the superstream prefix.
     *
     * @param message The message to log
     * @param throwable The exception to log
     */
    public void error(String message, Throwable throwable) {
        if (slf4jAvailable) {
            logger.error(withPrefix(message), throwable);
        } else {
            System.err.println(loggerName + " ERROR: " + withPrefix(message));
            throwable.printStackTrace();
        }
    }

    /**
     * Log a debug message with the superstream prefix.
     *
     * @param message The message to log
     */
    public void debug(String message) {
        if (slf4jAvailable) {
            logger.debug(withPrefix(message));
        } else {
            System.out.println(loggerName + " DEBUG: " + withPrefix(message));
        }
    }

    /**
     * Log a debug message with parameters and the superstream prefix.
     *
     * @param message The message to log
     * @param args The parameters for the message
     */
    public void debug(String message, Object... args) {
        if (slf4jAvailable) {
            logger.debug(withPrefix(message), args);
        } else {
            System.out.println(loggerName + " DEBUG: " + withPrefix(formatMessage(message, args)));
        }
    }

    private String formatMessage(String message, Object... args) {
        // Simple implementation of string formatting for fallback mode
        if (args == null || args.length == 0) {
            return message;
        }

        String result = message;
        for (Object arg : args) {
            int idx = result.indexOf("{}");
            if (idx >= 0) {
                result = result.substring(0, idx) +
                        (arg == null ? "null" : arg.toString()) +
                        result.substring(idx + 2);
            } else {
                break;
            }
        }
        return result;
    }
}