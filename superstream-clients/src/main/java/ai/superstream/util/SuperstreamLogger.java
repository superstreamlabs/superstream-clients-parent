package ai.superstream.util;

/**
 * Custom logger for the Superstream library that falls back to System.out/System.err
 */
public class SuperstreamLogger {
    private static final String PREFIX = "superstream";
    private final String className;
    // Flag to control debug logging - default to false to hide debug logs
    private static boolean debugEnabled = false;

    static {
        // Check if debug logging is enabled via system property or environment variable
        String debugFlag = System.getProperty("superstream.debug");
        if (debugFlag == null) {
            debugFlag = System.getenv("SUPERSTREAM_DEBUG");
        }
        debugEnabled = "true".equalsIgnoreCase(debugFlag);
    }

    // Enable or disable debug logging programmatically
    public static void setDebugEnabled(boolean enabled) {
        debugEnabled = enabled;
    }

    private SuperstreamLogger(Class<?> clazz) {
        this.className = clazz.getSimpleName();
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
     * Log an info message.
     */
    public void info(String message) {
        System.out.println(formatLogMessage("INFO", message));
    }

    /**
     * Log an info message with parameters.
     */
    public void info(String message, Object... args) {
        System.out.println(formatLogMessage("INFO", formatArgs(message, args)));
    }

    /**
     * Log a warning message.
     */
    public void warn(String message) {
        System.out.println(formatLogMessage("WARN", message));
    }

    /**
     * Log a warning message with parameters.
     */
    public void warn(String message, Object... args) {
        System.out.println(formatLogMessage("WARN", formatArgs(message, args)));
    }

    /**
     * Log an error message.
     */
    public void error(String message) {
        System.err.println(formatLogMessage("ERROR", message));
    }

    /**
     * Log an error message with parameters.
     */
    public void error(String message, Object... args) {
        System.err.println(formatLogMessage("ERROR", formatArgs(message, args)));
    }

    /**
     * Log an error message with an exception.
     */
    public void error(String message, Throwable throwable) {
        String formattedMessage = formatExceptionMessage(message, throwable);
        System.err.println(formatLogMessage("ERROR", formattedMessage));
    }

    /**
     * Log a debug message.
     */
    public void debug(String message) {
        if (debugEnabled) {
            System.out.println(formatLogMessage("DEBUG", message));
        }
    }

    /**
     * Log a debug message with parameters.
     */
    public void debug(String message, Object... args) {
        if (debugEnabled) {
            System.out.println(formatLogMessage("DEBUG", formatArgs(message, args)));
        }
    }

    public static boolean isDebugEnabled() {
        return debugEnabled;
    }

    /**
     * Format a log message with the Superstream prefix and class name.
     */
    private String formatLogMessage(String level, String message) {
        return String.format("[%s] %s %s: %s", PREFIX, level, className, message);
    }

    /**
     * Replace placeholder {} with actual values.
     */
    private String formatArgs(String message, Object... args) {
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

    /**
     * Format an exception message with class name, message and stack trace.
     * This is a standardized way to format exception messages across the codebase.
     * We are doing that because we saw that in some logging systems, the stack trace is not included in the error message unless it appears without a new line.
     */
    private String formatExceptionMessage(String message, Throwable throwable) {
        // Convert stack trace to string
        java.io.StringWriter sw = new java.io.StringWriter();
        java.io.PrintWriter pw = new java.io.PrintWriter(sw);
        throwable.printStackTrace(pw);
        String stackTrace = sw.toString().replaceAll("\\r?\\n", " ");
        
        return String.format("%s. Error: %s - %s. Stack trace: %s",
            message,
            throwable.getClass().getName(),
            throwable.getMessage(),
            stackTrace);
    }
}