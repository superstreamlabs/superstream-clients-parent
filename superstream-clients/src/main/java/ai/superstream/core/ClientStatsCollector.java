package ai.superstream.core;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects and tracks client statistics for Kafka producers.
 * This class provides methods to record data about producer operations
 * and calculates compression ratio metrics.
 */
public class ClientStatsCollector {
    // Tracks the total bytes written before compression since last report
    private final AtomicLong totalBytesBeforeCompression = new AtomicLong(0);
    
    // Tracks the total bytes written after compression since last report
    private final AtomicLong totalBytesAfterCompression = new AtomicLong(0);
    
    /**
     * Records data about a batch of messages sent by a producer.
     * 
     * @param uncompressedSize Size of the batch before compression (in bytes)
     * @param compressedSize Size of the batch after compression (in bytes)
     */
    public void recordBatch(long uncompressedSize, long compressedSize) {
        totalBytesBeforeCompression.addAndGet(uncompressedSize);
        totalBytesAfterCompression.addAndGet(compressedSize);
    }
    
    /**
     * Captures current statistics and resets counters atomically.
     * This prevents race conditions between reading values and resetting them.
     * 
     * @return Object containing both before and after compression sizes
     */
    public synchronized Stats captureAndReset() {
        // Get current values
        long beforeCompression = totalBytesBeforeCompression.get();
        long afterCompression = totalBytesAfterCompression.get();
        
        // Reset counters
        totalBytesBeforeCompression.set(0);
        totalBytesAfterCompression.set(0);
        
        // Return captured values
        return new Stats(beforeCompression, afterCompression);
    }
    
    /**
     * Container for statistics captured at a point in time.
     */
    public static class Stats {
        private final long bytesBeforeCompression;
        private final long bytesAfterCompression;
        
        public Stats(long bytesBeforeCompression, long bytesAfterCompression) {
            this.bytesBeforeCompression = bytesBeforeCompression;
            this.bytesAfterCompression = bytesAfterCompression;
        }
        
        public long getBytesBeforeCompression() {
            return bytesBeforeCompression;
        }
        
        public long getBytesAfterCompression() {
            return bytesAfterCompression;
        }
        
        public double getCompressionRatio() {
            if (bytesBeforeCompression == 0) {
                return 1.0;
            }
            return (double) bytesAfterCompression / bytesBeforeCompression;
        }
    }
}