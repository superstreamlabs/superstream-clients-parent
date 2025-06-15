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
    private String clientVersion;
    private java.util.Map<String, Double> producerMetrics; // Producer metrics map
    private java.util.List<String> topics; // Topics written by producer
    private java.util.Map<String, java.util.Map<String, Double>> topicMetrics; // Topic-level metrics map
    private java.util.Map<String, java.util.Map<String, Double>> nodeMetrics; // Node-level metrics map
    private java.util.Map<String, String> appInfoMetrics; // App-info metrics map with string values
    private java.util.Map<String,Object> originalConfiguration;
    private java.util.Map<String,Object> optimizedConfiguration;
    private java.util.Map<String, String> environmentVariables;
    private String hostname;
    private String producerUuid;
    private String mostImpactfulTopic;
    private String language = "Java";
    private String error;

    public ClientStatsMessage() {
        // Default constructor for Jackson
    }

    public ClientStatsMessage(String clientId, String ipAddress,
                              long totalWriteBefore, long totalWriteAfter,
                              String clientVersion, String hostname, String producerUuid) {
        this.clientId = clientId;
        this.ipAddress = ipAddress;
        this.type = "producer";
        this.messageType = "client_stats";
        this.totalWriteBefore = totalWriteBefore;
        this.totalWriteAfter = totalWriteAfter;
        this.clientVersion = clientVersion;
        this.environmentVariables = ai.superstream.util.EnvironmentVariables.getSuperstreamEnvironmentVariables();
        this.hostname = hostname;
        this.producerUuid = producerUuid;
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

    @JsonProperty("write_before_compression_delta")
    public long getTotalWriteBefore() {
        return totalWriteBefore;
    }

    @JsonProperty("write_before_compression_delta")
    public void setTotalWriteBefore(long totalWriteBefore) {
        this.totalWriteBefore = totalWriteBefore;
    }

    @JsonProperty("write_after_compression_delta")
    public long getTotalWriteAfter() {
        return totalWriteAfter;
    }

    @JsonProperty("write_after_compression_delta")
    public void setTotalWriteAfter(long totalWriteAfter) {
        this.totalWriteAfter = totalWriteAfter;
    }

    @JsonProperty("version")
    public String getClientVersion() {
        return clientVersion;
    }

    @JsonProperty("version")
    public void setClientVersion(String clientVersion) {
        this.clientVersion = clientVersion;
    }

    @JsonProperty("producer_metrics")
    public java.util.Map<String, Double> getProducerMetrics() {
        return producerMetrics;
    }

    @JsonProperty("producer_metrics")
    public void setProducerMetrics(java.util.Map<String, Double> producerMetrics) {
        this.producerMetrics = producerMetrics;
    }

    @JsonProperty("topic_metrics")
    public java.util.Map<String, java.util.Map<String, Double>> getTopicMetrics() {
        return topicMetrics;
    }

    @JsonProperty("topic_metrics")
    public void setTopicMetrics(java.util.Map<String, java.util.Map<String, Double>> topicMetrics) {
        this.topicMetrics = topicMetrics;
    }

    @JsonProperty("topics")
    public java.util.List<String> getTopics() {
        return topics;
    }

    @JsonProperty("topics")
    public void setTopics(java.util.List<String> topics) {
        this.topics = topics;
    }

    @JsonProperty("node_metrics")
    public java.util.Map<String, java.util.Map<String, Double>> getNodeMetrics() {
        return nodeMetrics;
    }

    @JsonProperty("node_metrics")
    public void setNodeMetrics(java.util.Map<String, java.util.Map<String, Double>> nodeMetrics) {
        this.nodeMetrics = nodeMetrics;
    }

    @JsonProperty("app_info_metrics")
    public java.util.Map<String, String> getAppInfoMetrics() {
        return appInfoMetrics;
    }

    @JsonProperty("app_info_metrics")
    public void setAppInfoMetrics(java.util.Map<String, String> appInfoMetrics) {
        this.appInfoMetrics = appInfoMetrics;
    }

    @JsonProperty("original_configuration")
    public java.util.Map<String,Object> getOriginalConfiguration() { return originalConfiguration; }
    @JsonProperty("original_configuration")
    public void setOriginalConfiguration(java.util.Map<String,Object> cfg) { this.originalConfiguration = cfg; }

    @JsonProperty("optimized_configuration")
    public java.util.Map<String,Object> getOptimizedConfiguration() { return optimizedConfiguration; }
    @JsonProperty("optimized_configuration")
    public void setOptimizedConfiguration(java.util.Map<String,Object> cfg) { this.optimizedConfiguration = cfg; }

    @JsonProperty("environment_variables")
    public java.util.Map<String, String> getEnvironmentVariables() {
        return environmentVariables;
    }

    @JsonProperty("environment_variables")
    public void setEnvironmentVariables(java.util.Map<String, String> environmentVariables) {
        this.environmentVariables = environmentVariables;
    }

    @JsonProperty("hostname")
    public String getHostname() {
        return hostname;
    }

    @JsonProperty("hostname")
    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    @JsonProperty("superstream_client_uid")
    public String getProducerUuid() {
        return producerUuid;
    }

    @JsonProperty("superstream_client_uid")
    public void setProducerUuid(String producerUuid) {
        this.producerUuid = producerUuid;
    }

    @JsonProperty("most_impactful_topic")
    public String getMostImpactfulTopic() {
        return mostImpactfulTopic == null ? "" : mostImpactfulTopic;
    }

    @JsonProperty("most_impactful_topic")
    public void setMostImpactfulTopic(String mostImpactfulTopic) {
        this.mostImpactfulTopic = mostImpactfulTopic;
    }

    @JsonProperty("language")
    public String getLanguage() { return language; }
    @JsonProperty("language")
    public void setLanguage(String language) { this.language = language; }

    @JsonProperty("error")
    public String getError() { return error; }
    @JsonProperty("error")
    public void setError(String error) { this.error = error; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientStatsMessage that = (ClientStatsMessage) o;
        return totalWriteBefore == that.totalWriteBefore &&
                totalWriteAfter == that.totalWriteAfter &&
                Objects.equals(clientId, that.clientId) &&
                Objects.equals(ipAddress, that.ipAddress) &&
                Objects.equals(type, that.type) &&
                Objects.equals(messageType, that.messageType) &&
                Objects.equals(clientVersion, that.clientVersion) &&
                Objects.equals(producerMetrics, that.producerMetrics) &&
                Objects.equals(topics, that.topics) &&
                Objects.equals(originalConfiguration, that.originalConfiguration) &&
                Objects.equals(optimizedConfiguration, that.optimizedConfiguration) &&
                Objects.equals(environmentVariables, that.environmentVariables) &&
                Objects.equals(hostname, that.hostname) &&
                Objects.equals(producerUuid, that.producerUuid) &&
                Objects.equals(mostImpactfulTopic, that.mostImpactfulTopic) &&
                Objects.equals(language, that.language) &&
                Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId, ipAddress, type, messageType, totalWriteBefore, 
                          totalWriteAfter, clientVersion, producerMetrics, topics, 
                          originalConfiguration, optimizedConfiguration, environmentVariables,
                          hostname, producerUuid, mostImpactfulTopic, language, error);
    }

    @Override
    public String toString() {
        return "ClientStatsMessage{" +
                "client_id='" + clientId + '\'' +
                ", ip_address='" + ipAddress + '\'' +
                ", type='" + type + '\'' +
                ", message_type='" + messageType + '\'' +
                ", write_before_compression_delta=" + totalWriteBefore +
                ", write_after_compression_delta=" + totalWriteAfter +
                ", version='" + clientVersion + '\'' +
                ", producer_metrics=" + producerMetrics +
                ", topics=" + topics +
                ", original_configuration=" + originalConfiguration +
                ", optimized_configuration=" + optimizedConfiguration +
                ", environment_variables=" + environmentVariables +
                ", hostname='" + hostname + '\'' +
                ", superstream_client_uid='" + producerUuid + '\'' +
                ", most_impactful_topic='" + mostImpactfulTopic + '\'' +
                ", language='" + language + '\'' +
                ", error='" + error + '\'' +
                '}';
    }
} 