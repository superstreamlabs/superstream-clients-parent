package ai.superstream.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for collecting environment variables.
 */
public class EnvironmentVariables {
    private static final String SUPERSTREAM_PREFIX = "SUPERSTREAM_";

    /**
     * Collects all environment variables that start with SUPERSTREAM_
     * @return A map of environment variable names to their values
     */
    public static Map<String, String> getSuperstreamEnvironmentVariables() {
        Map<String, String> envVars = new HashMap<>();
        Map<String, String> systemEnv = System.getenv();
        
        for (Map.Entry<String, String> entry : systemEnv.entrySet()) {
            if (entry.getKey().startsWith(SUPERSTREAM_PREFIX)) {
                envVars.put(entry.getKey(), entry.getValue());
            }
        }
        
        return envVars;
    }
} 