package ai.superstream.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

/**
 * Represents a message to be sent to the superstream.clients topic.
 */
public class ClientMessage {
    private int superstreamClusterId;
    private boolean active;
    private String clientId;
    private String ipAddress;
    private String clientVersion;
    private String language;
    private String clientType;
    private Map<String, Object> originalConfiguration;
    private Map<String, Object> optimizedConfiguration;
    private String mostImpactfulTopic;
    private Map<String, String> environmentVariables;
    private String hostname;
    private String producerUuid;
    private String error;

    public ClientMessage() {
        // Default constructor for Jackson
    }

    public ClientMessage(int superstreamClusterId, boolean active, String clientId, String ipAddress, String clientVersion, String language, String clientType,
                         Map<String, Object> originalConfiguration, Map<String, Object> optimizedConfiguration,
                         String mostImpactfulTopic, String hostname, String producerUuid, String error) {
        this.superstreamClusterId = superstreamClusterId;
        this.active = active;
        this.clientId = clientId;
        this.ipAddress = ipAddress;
        this.clientVersion = clientVersion;
        this.language = language;
        this.clientType = clientType;
        this.originalConfiguration = originalConfiguration;
        this.optimizedConfiguration = optimizedConfiguration;
        this.mostImpactfulTopic = mostImpactfulTopic;
        this.environmentVariables = ai.superstream.util.EnvironmentVariables.getSuperstreamEnvironmentVariables();
        this.hostname = hostname;
        this.producerUuid = producerUuid;
        this.error = error;
    }

    @JsonProperty("superstream_cluster_id")
    public int getSuperstreamClusterId() {
        return superstreamClusterId;
    }

    @JsonProperty("superstream_cluster_id")
    public void setSuperstreamClusterId(int superstreamClusterId) {
        this.superstreamClusterId = superstreamClusterId;
    }

    @JsonProperty("active")
    public boolean isActive() {
        return active;
    }

    @JsonProperty("active")
    public void setActive(boolean active) {
        this.active = active;
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

    @JsonProperty("version")
    public String getClientVersion() {
        return clientVersion;
    }

    @JsonProperty("version")
    public void setClientVersion(String clientVersion) {
        this.clientVersion = clientVersion;
    }

    @JsonProperty("language")
    public String getLanguage() {
        return language;
    }

    @JsonProperty("language")
    public void setLanguage(String language) {
        this.language = language;
    }

    @JsonProperty("client_type")
    public String getClientType() {
        return clientType;
    }

    @JsonProperty("client_type")
    public void setClientType(String clientType) {
        this.clientType = clientType;
    }

    @JsonProperty("original_configuration")
    public Map<String, Object> getOriginalConfiguration() {
        return originalConfiguration;
    }

    @JsonProperty("original_configuration")
    public void setOriginalConfiguration(Map<String, Object> originalConfiguration) {
        this.originalConfiguration = originalConfiguration;
    }

    @JsonProperty("optimized_configuration")
    public Map<String, Object> getOptimizedConfiguration() {
        return optimizedConfiguration;
    }

    @JsonProperty("optimized_configuration")
    public void setOptimizedConfiguration(Map<String, Object> optimizedConfiguration) {
        this.optimizedConfiguration = optimizedConfiguration;
    }

    @JsonProperty("most_impactful_topic")
    public String getMostImpactfulTopic() {
        return mostImpactfulTopic;
    }

    @JsonProperty("most_impactful_topic")
    public void setMostImpactfulTopic(String mostImpactfulTopic) {
        this.mostImpactfulTopic = mostImpactfulTopic;
    }

    @JsonProperty("environment_variables")
    public Map<String, String> getEnvironmentVariables() {
        return environmentVariables;
    }

    @JsonProperty("environment_variables")
    public void setEnvironmentVariables(Map<String, String> environmentVariables) {
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

    @JsonProperty("error")
    public String getError() {
        return error;
    }

    @JsonProperty("error")
    public void setError(String error) {
        this.error = error;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientMessage that = (ClientMessage) o;
        return superstreamClusterId == that.superstreamClusterId &&
                active == that.active &&
                Objects.equals(clientId, that.clientId) &&
                Objects.equals(ipAddress, that.ipAddress) &&
                Objects.equals(clientVersion, that.clientVersion) &&
                Objects.equals(language, that.language) &&
                Objects.equals(clientType, that.clientType) &&
                Objects.equals(originalConfiguration, that.originalConfiguration) &&
                Objects.equals(optimizedConfiguration, that.optimizedConfiguration) &&
                Objects.equals(mostImpactfulTopic, that.mostImpactfulTopic) &&
                Objects.equals(environmentVariables, that.environmentVariables) &&
                Objects.equals(hostname, that.hostname) &&
                Objects.equals(producerUuid, that.producerUuid) &&
                Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(superstreamClusterId, active, clientId, ipAddress, clientVersion, language, clientType, 
                          originalConfiguration, optimizedConfiguration, mostImpactfulTopic, 
                          environmentVariables, hostname, producerUuid, error);
    }

    @Override
    public String toString() {
        return "ClientMessage{" +
                "superstream_cluster_id=" + superstreamClusterId +
                ", active=" + active +
                ", client_id='" + clientId + '\'' +
                ", ip_address='" + ipAddress + '\'' +
                ", version='" + clientVersion + '\'' +
                ", language='" + language + '\'' +
                ", client_type='" + clientType + '\'' +
                ", original_configuration=" + originalConfiguration +
                ", optimized_configuration=" + optimizedConfiguration +
                ", most_impactful_topic='" + mostImpactfulTopic + '\'' +
                ", environment_variables=" + environmentVariables +
                ", hostname='" + hostname + '\'' +
                ", superstream_client_uid='" + producerUuid + '\'' +
                ", error='" + error + '\'' +
                '}';
    }
}