# Superstream SDK Testing Guide

## Test Classes Overview

### 1. KafkaProducerExample (baseline example)

**What it does:** Simplest case - one producer, one cluster, one topic.

**What we test:**
- Basic Superstream functionality
- Optimization application (batch.size, linger.ms, compression)
- Metrics collection and reporting

**How to run:**
```bash
# Basic test
export SUPERSTREAM_TOPICS_LIST=example-topic
java -javaagent:superstream-clients-1.0.0.jar -jar kafka-clients-example.jar

# Test with disabled optimization
export SUPERSTREAM_DISABLED=true
java -javaagent:superstream-clients-1.0.0.jar -jar kafka-clients-example.jar

# Test latency-sensitive mode (change linger.ms to 0 in code)
export SUPERSTREAM_LATENCY_SENSITIVE=true
java -javaagent:superstream-clients-1.0.0.jar -jar kafka-clients-example.jar
```

---

### 2. MultiProducerMultiClusterExample

**What it does:** Creates 4 producers for 3 different Kafka clusters running in parallel.

**What we test:**
- Multiple producers working simultaneously
- Multiple Kafka clusters support
- Topic filtering (SUPERSTREAM_TOPICS_LIST)
- Concurrent producer operation

**Configuration via environment variables:**
```bash
# Configure clusters
export CLUSTER1_BOOTSTRAP_SERVERS=localhost:9092
export CLUSTER2_BOOTSTRAP_SERVERS=localhost:9093  
export CLUSTER3_BOOTSTRAP_SERVERS=localhost:9094

# Optimize only specific topics
export SUPERSTREAM_TOPICS_LIST=cluster1-topic-a,cluster2-topic

# Run
java -javaagent:superstream-clients-1.0.0.jar -jar multi-producer-multi-cluster-example.jar
```

**What to change for different tests:**
- Add/remove topics from `SUPERSTREAM_TOPICS_LIST` - test filtering
- Change `CLUSTER*_BOOTSTRAP_SERVERS` - test with real clusters
- Code creates producers with small batch.size (32KB) - test optimization

---

### 3. CustomProducerMultiClusterExample

**What it does:** Uses custom classes that extend KafkaProducer with additional logic.

**What we test:**
- Interception works with KafkaProducer subclasses
- Optimizations apply to custom implementations
- Metrics collected for all producer types

**Producer classes:**
- `MetricsAwareProducer` - adds message/byte/error counters
- `RetryableProducer` - adds custom retry logic

**Configuration:**
```bash
# Enable custom metrics logging
export ENABLE_METRICS_LOGGING=true

# Configure clusters
export CLUSTER1_BOOTSTRAP_SERVERS=localhost:9092
export CLUSTER2_BOOTSTRAP_SERVERS=localhost:9093

# Run (executes 10 iterations of 30 seconds each)
java -javaagent:superstream-clients-1.0.0.jar -jar custom-producer-multi-cluster-example.jar
```

**Key features:**
- Uses compression=snappy (test change to zstd)
- Very small batch.size=16KB (test optimization)
- Different linger.ms for different producers

---

### 4. SpringMultiProducerExample

**What it does:** Spring Boot application creating KafkaProducer as Spring beans.

**What we test:**
- Works with Spring-managed beans
- Intercepts producers created via @Bean
- Spring context compatibility
- @Scheduled tasks aren't blocked

**Configuration:**
```bash
# Environment variables have priority over Spring properties
export SPRING_PRODUCER_COMPRESSION=lz4
export SPRING_PRODUCER_BATCH_SIZE=32768
export SPRING_PRODUCER_LINGER_MS=100

# Configure clusters
export CLUSTER1_BOOTSTRAP_SERVERS=localhost:9092
export CLUSTER2_BOOTSTRAP_SERVERS=localhost:9093
export CLUSTER3_BOOTSTRAP_SERVERS=localhost:9094

# Run Spring Boot application
java -javaagent:superstream-clients-1.0.0.jar -jar spring-multi-producer-example.jar
```

**Key features:**
- Automatic sending every 30 seconds via @Scheduled
- 4 producers: 2 for cluster1, one each for cluster2 and cluster3
- cluster3 producer overrides compression to lz4

---

### 5. SpringKafkaMultiProducerExample

**What it does:** Spring Boot with Spring Kafka - uses ProducerFactory and KafkaTemplate.

**What we test:**
- Intercepts producers created via ProducerFactory
- Works with KafkaTemplate
- Transactional producer support
- Asynchronous callbacks

**Configuration:**
```bash
# Spring Kafka specific settings
export SPRING_KAFKA_COMPRESSION=snappy
export SPRING_KAFKA_BATCH_SIZE=16384
export SPRING_KAFKA_LINGER_MS=50

# Enable transactions for cluster3
export ENABLE_TRANSACTIONAL=true

# Run
java -javaagent:superstream-clients-1.0.0.jar -jar spring-kafka-multi-producer-example.jar
```

**Key features:**
- ListenableFuture for async processing
- Transactional sending (if ENABLE_TRANSACTIONAL=true)
- Success/failure counters

---

## Common Tests for All Classes

### Test 1: Basic Functionality
```bash
export SUPERSTREAM_TOPICS_LIST=topic1,topic2,topic3
export SUPERSTREAM_DEBUG=false
# Run the class
# Check logs for optimization application
```

### Test 2: Disable Superstream
```bash
export SUPERSTREAM_DISABLED=true
# Run the class
# Verify no [superstream] logs and no optimizations
```

### Test 3: Latency-sensitive Mode
```bash
export SUPERSTREAM_LATENCY_SENSITIVE=true
# Run the class
# Verify linger.ms is not modified
```

### Test 4: Debug Mode
```bash
export SUPERSTREAM_DEBUG=true
# Run the class
# See detailed DEBUG logs
```

### Test 5: Selective Optimization
```bash
# Specify only some topics
export SUPERSTREAM_TOPICS_LIST=topic1
# Run the class
# Verify only specified topics are optimized
```

---

## What to Look for in Logs

### Successful optimization:
```
[superstream] INFO ConfigurationOptimizer: Overriding configuration: batch.size=65536 (was: 32768)
[superstream] INFO ConfigurationOptimizer: Overriding configuration: linger.ms=5000 (was: 500)
[superstream] INFO ConfigurationOptimizer: Overriding configuration: compression.type=zstd (was: lz4)
```

### Intelligent behavior:
```
[superstream] INFO ConfigurationOptimizer: Keeping existing batch.size value 1048576 as it's greater than recommended value 65536
```

### Reporting and metrics:
```
[superstream] INFO ClientReporter: Successfully reported client information to superstream.clients
[superstream] INFO ClientStatsReporter: Producer stats sent: before=260339 bytes, after=1170 bytes
```

---

## Recommended Testing Order

1. **Start with KafkaProducerExample** - ensure basic functionality works
2. **MultiProducerMultiClusterExample** - test scalability
3. **CustomProducerMultiClusterExample** - test inheritance support
4. **Spring examples** - test framework integration

Each class is self-contained and demonstrates a specific aspect of Superstream SDK functionality.

---

## Key Points to Remember

- **Environment variables** control behavior without code changes
- **Metadata topic** (`superstream.metadata_v1`) must contain optimization recommendations
- **Client reports** are sent to `superstream.clients` topic
- **Debug mode** helps troubleshoot issues
- **Each test** validates different architectural patterns

The goal is to ensure Superstream SDK works correctly across all common Kafka usage patterns in Java applications.

---

## Quick Reference Table

| Class | Producers | Clusters | Framework | Special Features |
|-------|-----------|----------|-----------|------------------|
| KafkaProducerExample | 1 | 1 | Pure Java | Baseline test |
| MultiProducerMultiClusterExample | 4 | 3 | Pure Java | Concurrent producers |
| CustomProducerMultiClusterExample | 3 | 2 | Pure Java | Custom KafkaProducer subclasses |
| SpringMultiProducerExample | 4 | 3 | Spring Boot | Direct KafkaProducer beans |
| SpringKafkaMultiProducerExample | 4 | 3 | Spring Kafka | ProducerFactory & KafkaTemplate |