# Kafka Multi-Cluster Setup with Docker Guide

This guide explains how to set up multiple Kafka clusters using Docker Compose and connect to them from both Docker containers (like Conduktor) and host applications.

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
- [Connection Guide](#connection-guide)
- [Troubleshooting](#troubleshooting)
- [Common Issues and Solutions](#common-issues-and-solutions)

## Overview

This setup creates two independent Kafka clusters:
- **Cluster 1**: Kafka2 (port 9095) with Zookeeper2 (port 2182)
- **Cluster 2**: Kafka3 (port 9096) with Zookeeper3 (port 2183)

Each Kafka broker is configured with dual listeners to support:
- **Internal connections**: From other Docker containers
- **External connections**: From the host machine

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Host Machine                           │
│                                                             │
│  ┌─────────────────┐          ┌─────────────────┐         │
│  │ External App    │          │ External App    │         │
│  │ (Superstream)   │          │ (Your App)      │         │
│  └────────┬────────┘          └────────┬────────┘         │
│           │                             │                   │
│     localhost:9095               localhost:9096            │
│           │                             │                   │
├───────────┴─────────────────────────────┴──────────────────┤
│                    Docker Network: kafka-network            │
│                                                             │
│  ┌─────────────────┐          ┌─────────────────┐         │
│  │   Conduktor     │          │  Other Container │         │
│  └────────┬────────┘          └────────┬────────┘         │
│           │                             │                   │
│     kafka2:29095                  kafka3:29096             │
│           │                             │                   │
│  ┌────────┴────────┐          ┌────────┴────────┐         │
│  │     Kafka2      │          │     Kafka3      │         │
│  │   INTERNAL:     │          │   INTERNAL:     │         │
│  │   :29095        │          │   :29096        │         │
│  │   EXTERNAL:     │          │   EXTERNAL:     │         │
│  │   :9095         │          │   :9096         │         │
│  └────────┬────────┘          └────────┬────────┘         │
│           │                             │                   │
│  ┌────────┴────────┐          ┌────────┴────────┐         │
│  │   Zookeeper2    │          │   Zookeeper3    │         │
│  │    :2182        │          │    :2183        │         │
│  └─────────────────┘          └─────────────────┘         │
└─────────────────────────────────────────────────────────────┘
```

## Prerequisites

- Docker Desktop installed and running
- Docker Compose installed
- Basic understanding of Kafka concepts
- Port availability: 9095, 9096, 29095, 29096, 2182, 2183

## Setup Instructions

### Step 1: Create the Docker Compose File

Create a file named `docker-compose.yml` with the following content:

```yaml
version: '3.8'

services:
  zookeeper2:
    image: confluentinc/cp-zookeeper:7.4.0
    container_name: zookeeper2
    environment:
      ZOOKEEPER_CLIENT_PORT: 2182
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2182:2182"
    networks:
      - kafka-network

  kafka2:
    image: confluentinc/cp-kafka:7.4.0
    container_name: kafka2
    depends_on:
      - zookeeper2
    ports:
      - "9095:9095"      # External connections
      - "29095:29095"    # Internal connections
    environment:
      KAFKA_BROKER_ID: 2
      KAFKA_ZOOKEEPER_CONNECT: zookeeper2:2182
      # Dual listeners configuration
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka2:29095,EXTERNAL://localhost:9095
      KAFKA_LISTENERS: INTERNAL://0.0.0.0:29095,EXTERNAL://0.0.0.0:9095
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
    networks:
      - kafka-network

  zookeeper3:
    image: confluentinc/cp-zookeeper:7.4.0
    container_name: zookeeper3
    environment:
      ZOOKEEPER_CLIENT_PORT: 2183
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2183:2183"
    networks:
      - kafka-network

  kafka3:
    image: confluentinc/cp-kafka:7.4.0
    container_name: kafka3
    depends_on:
      - zookeeper3
    ports:
      - "9096:9096"      # External connections
      - "29096:29096"    # Internal connections
    environment:
      KAFKA_BROKER_ID: 3
      KAFKA_ZOOKEEPER_CONNECT: zookeeper3:2183
      # Dual listeners configuration
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka3:29096,EXTERNAL://localhost:9096
      KAFKA_LISTENERS: INTERNAL://0.0.0.0:29096,EXTERNAL://0.0.0.0:9096
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
    networks:
      - kafka-network

networks:
  kafka-network:
    external: true
```

### Step 2: Create the Docker Network

```bash
docker network create kafka-network
```

### Step 3: Start the Clusters

```bash
# Start all services
docker-compose up -d

# Verify all containers are running
docker ps

# Check logs if needed
docker logs kafka2
docker logs kafka3
```

### Step 4: Connect Conduktor (if using)

If you're using Conduktor or another containerized Kafka client:

```bash
# Connect Conduktor to the kafka-network
docker network connect kafka-network <conduktor-container-name>

# Example:
docker network connect kafka-network superstream-dev-conduktor-console-1
```

## Connection Guide

### From Docker Containers (e.g., Conduktor)

When connecting from applications running inside Docker containers:

**Cluster 1 (Kafka2):**
- Bootstrap Server: `kafka2:29095`
- Authentication: None

**Cluster 2 (Kafka3):**
- Bootstrap Server: `kafka3:29096`
- Authentication: None

**Both Clusters:**
- Bootstrap Servers: `kafka2:29095,kafka3:29096`

### From Host Machine (e.g., Superstream Agent, IDE)

When connecting from applications running on your host machine:

**Cluster 1 (Kafka2):**
- Bootstrap Server: `localhost:9095`
- Authentication: None

**Cluster 2 (Kafka3):**
- Bootstrap Server: `localhost:9096`
- Authentication: None

**Both Clusters:**
- Bootstrap Servers: `localhost:9095,localhost:9096`

### Testing Connections

Test from inside container:
```bash
# List topics on Cluster 1
docker exec -it kafka2 kafka-topics --bootstrap-server kafka2:29095 --list

# Create a test topic on Cluster 2
docker exec -it kafka3 kafka-topics --create --topic test-topic --bootstrap-server kafka3:29096 --partitions 1 --replication-factor 1
```

Test from host machine:
```bash
# Using kafkacat (install with: brew install kcat)
kcat -b localhost:9095 -L
kcat -b localhost:9096 -L

# Using nc to test port connectivity
nc -zv localhost 9095
nc -zv localhost 9096
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Connection Refused Error
```
org.apache.kafka.common.KafkaException: Failed to create new KafkaAdminClient
Caused by: java.net.ConnectException: Connection refused
```

**Solution:**
- Ensure containers are running: `docker ps`
- Wait 10-20 seconds after starting containers for Kafka to initialize
- Check if ports are available: `lsof -i :9095` (Mac/Linux) or `netstat -an | findstr 9095` (Windows)

#### 2. Timeout Error from Conduktor
```
org.apache.kafka.common.errors.TimeoutException: Timed out waiting for a node assignment
```

**Solution:**
- Make sure Conduktor is connected to kafka-network
- Use internal addresses (kafka2:29095) not localhost
- Verify network connectivity: `docker network inspect kafka-network`

#### 3. External Application Can't Connect After Adding Conduktor

**Problem:** After configuring for Conduktor, external apps can't connect to `localhost:9095`

**Solution:** This guide uses dual listeners specifically to avoid this issue. Make sure you're using the configuration with both INTERNAL and EXTERNAL listeners.

### Verification Commands

Check container status:
```bash
# List all containers
docker ps

# Check specific container logs
docker logs kafka2 --tail 50
docker logs kafka3 --tail 50

# Inspect network
docker network inspect kafka-network

# Check which containers are on the network
docker network inspect kafka-network | grep -A 4 "Containers"
```

Check Kafka broker registration:
```bash
# Connect to Zookeeper and list brokers
docker exec -it zookeeper2 zkCli.sh -server localhost:2182
# In ZK shell: ls /brokers/ids
```

### Clean Up

To stop and remove all containers:
```bash
# Stop containers
docker-compose down

# Remove network (only if not used by other containers)
docker network rm kafka-network

# Clean up volumes (WARNING: removes data)
docker-compose down -v
```

## Key Configuration Explained

### Dual Listener Configuration

The magic happens in these environment variables:

```yaml
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka2:29095,EXTERNAL://localhost:9095
KAFKA_LISTENERS: INTERNAL://0.0.0.0:29095,EXTERNAL://0.0.0.0:9095
```

- **INTERNAL listener**: Used for container-to-container communication
- **EXTERNAL listener**: Used for host-to-container communication
- Kafka automatically provides the correct address based on where the client is connecting from

### Why Two Separate Clusters?

This setup creates two independent Kafka clusters (not a single cluster with two brokers) because:
- Each Kafka broker has its own Zookeeper
- Brokers don't share state or replicate data between them
- Useful for testing multi-cluster scenarios or isolation requirements

## Best Practices

1. **Always wait for initialization**: Kafka takes 10-20 seconds to fully start
2. **Use meaningful container names**: Makes debugging easier
3. **Check logs first**: Most issues are visible in container logs
4. **Network isolation**: Use external networks for better control
5. **Port management**: Keep track of port mappings to avoid conflicts

## Conclusion

This setup provides a flexible Kafka environment that supports both containerized and host-based clients. The dual listener configuration ensures compatibility with various connection scenarios without requiring reconfiguration when switching between different client types.