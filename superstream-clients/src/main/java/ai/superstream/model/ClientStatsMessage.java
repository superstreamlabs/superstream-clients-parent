package ai.superstream.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Represents a message containing client statistics information
 * to be sent to the superstream.clients topic.
 */
public class ClientStatsMessage {
    private String clientId;
    private String ipAddress;
    private String type;
    private String messageType;
    private long totalWriteBefore;
    private long totalWriteAfter;
    private double compressionRatio;
    private int superstreamClusterId;
    private String clientVersion;

    public ClientStatsMessage() {
        // Default constructor for Jackson
    }

    public ClientStatsMessage(String clientId, String ipAddress, 
                             long totalWriteBefore, long totalWriteAfter, double compressionRatio,
                             String clientVersion) {
        this.clientId = clientId;
        this.ipAddress = ipAddress;
        this.type = "producer";  // Client type is producer
        this.messageType = "client_stats"; // Message type is client_stats
        this.totalWriteBefore = totalWriteBefore;
        this.totalWriteAfter = totalWriteAfter;
        this.compressionRatio = compressionRatio;
        this.clientVersion = clientVersion;
    }
    
    public ClientStatsMessage(String clientId, String ipAddress, int superstreamClusterId, 
                            long totalWriteBefore, long totalWriteAfter, double compressionRatio,
                            String clientVersion) {
        this.clientId = clientId;
        this.ipAddress = ipAddress;
        this.type = "producer";  // Client type is producer
        this.messageType = "client_stats"; // Message type is client_stats
        this.totalWriteBefore = totalWriteBefore;
        this.totalWriteAfter = totalWriteAfter;
        this.compressionRatio = compressionRatio;
        this.superstreamClusterId = superstreamClusterId;
        this.clientVersion = clientVersion;
    }

    @JsonProperty("client_id")
    public String getClientId() {
        return clientId;
    }

    @JsonProperty("client_id")
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    @JsonProperty("ip_address")
    public String getIpAddress() {
        return ipAddress;
    }

    @JsonProperty("ip_address")
    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    @JsonProperty("type")
    public String getType() {
        return type;
    }

    @JsonProperty("type")
    public void setType(String type) {
        this.type = type;
    }

    @JsonProperty("message_type")
    public String getMessageType() {
        return messageType;
    }

    @JsonProperty("message_type")
    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    @JsonProperty("total_write_before")
    public long getTotalWriteBefore() {
        return totalWriteBefore;
    }

    @JsonProperty("total_write_before")
    public void setTotalWriteBefore(long totalWriteBefore) {
        this.totalWriteBefore = totalWriteBefore;
    }

    @JsonProperty("total_write_after")
    public long getTotalWriteAfter() {
        return totalWriteAfter;
    }

    @JsonProperty("total_write_after")
    public void setTotalWriteAfter(long totalWriteAfter) {
        this.totalWriteAfter = totalWriteAfter;
    }

    @JsonProperty("compression_ratio")
    public double getCompressionRatio() {
        return compressionRatio;
    }

    @JsonProperty("compression_ratio")
    public void setCompressionRatio(double compressionRatio) {
        this.compressionRatio = compressionRatio;
    }
    
    @JsonProperty("superstream_cluster_id")
    public int getSuperstreamClusterId() {
        return superstreamClusterId;
    }

    @JsonProperty("superstream_cluster_id")
    public void setSuperstreamClusterId(int superstreamClusterId) {
        this.superstreamClusterId = superstreamClusterId;
    }

    @JsonProperty("version")
    public String getClientVersion() {
        return clientVersion;
    }

    @JsonProperty("version")
    public void setClientVersion(String clientVersion) {
        this.clientVersion = clientVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientStatsMessage that = (ClientStatsMessage) o;
        return totalWriteBefore == that.totalWriteBefore &&
                totalWriteAfter == that.totalWriteAfter &&
                Double.compare(that.compressionRatio, compressionRatio) == 0 &&
                superstreamClusterId == that.superstreamClusterId &&
                Objects.equals(clientId, that.clientId) &&
                Objects.equals(ipAddress, that.ipAddress) &&
                Objects.equals(type, that.type) &&
                Objects.equals(messageType, that.messageType) &&
                Objects.equals(clientVersion, that.clientVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId, ipAddress, type, messageType, totalWriteBefore, 
                          totalWriteAfter, compressionRatio, superstreamClusterId, clientVersion);
    }

    @Override
    public String toString() {
        return "ClientStatsMessage{" +
                "client_id='" + clientId + '\'' +
                ", ip_address='" + ipAddress + '\'' +
                ", type='" + type + '\'' +
                ", message_type='" + messageType + '\'' +
                ", total_write_before=" + totalWriteBefore +
                ", total_write_after=" + totalWriteAfter +
                ", compression_ratio=" + compressionRatio +
                ", superstream_cluster_id=" + superstreamClusterId +
                ", version='" + clientVersion + '\'' +
                '}';
    }
} 