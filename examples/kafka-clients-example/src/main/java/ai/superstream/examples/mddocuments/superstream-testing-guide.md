# Superstream SDK Complete Testing Guide

This guide covers all 10 test classes (5 new + 5 modified) for comprehensive testing of Superstream SDK functionality.

## Table of Contents
1. [Overview](#overview)
2. [Modified Classes](#modified-classes)
3. [New Classes](#new-classes)
4. [Testing Scenarios](#testing-scenarios)
5. [Expected Results](#expected-results)
6. [Quick Reference](#quick-reference)

---

## Overview

### Purpose
These test classes validate that Superstream SDK correctly:
- Intercepts KafkaProducer construction
- Handles various bootstrap server formats
- Works with Properties and Map configurations
- Handles custom producer classes properly (PricelineKafkaProducer fix)
- Integrates with Spring and Spring Kafka
- Optimizes producer configurations

### Environment Variables
Common environment variables used across all tests:
```bash
# Superstream control
SUPERSTREAM_DISABLED=true/false          # Disable optimization
SUPERSTREAM_DEBUG=true/false             # Enable debug logging
SUPERSTREAM_LATENCY_SENSITIVE=true/false # Preserve linger.ms
SUPERSTREAM_TOPICS_LIST=topic1,topic2    # Topics to optimize

# Cluster configuration
CLUSTER1_BOOTSTRAP_SERVERS=localhost:9092
CLUSTER2_BOOTSTRAP_SERVERS=localhost:9095
CLUSTER3_BOOTSTRAP_SERVERS=localhost:9096
```

---

## Modified Classes

### 1. KafkaProducerExample.java

**Purpose:** Basic single-producer test with configurable bootstrap server formats.

**What it tests:**
- Basic Superstream functionality
- Different bootstrap server formats via `BOOTSTRAP_FORMAT` environment variable
- Configuration optimization (batch.size, linger.ms, compression)

**Key features:**
- Single producer, single cluster
- Configurable bootstrap format: string, list, arrays_list, list_of
- Large compressible messages for compression testing

**How to run:**
```bash
# Test different bootstrap formats
export BOOTSTRAP_FORMAT=string  # Default
export BOOTSTRAP_FORMAT=list
export BOOTSTRAP_FORMAT=arrays_list
export BOOTSTRAP_FORMAT=list_of

java -javaagent:superstream-clients-1.0.0.jar -jar kafka-clients-example.jar
```

**Expected results:**
```
[INFO] Using String format for bootstrap servers: localhost:9092
[superstream] INFO ConfigurationOptimizer: Overriding configuration: batch.size=65536 (was: 1048576)
[superstream] INFO ConfigurationOptimizer: Overriding configuration: compression.type=zstd (was: zstd)
```

---

### 2. MultiProducerMultiClusterExample.java

**Purpose:** Tests multiple producers across multiple clusters with different bootstrap formats.

**What it tests:**
- Multiple producers working simultaneously
- Each producer uses a different bootstrap format
- Topic filtering with SUPERSTREAM_TOPICS_LIST
- Concurrent message sending

**Key features:**
- 4 producers across 3 clusters
- Each producer uses different bootstrap format:
  - producer1A: String
  - producer1B: List
  - producer2: Arrays.asList()
  - producer3: List.of()

**How to run:**
```bash
export CLUSTER1_BOOTSTRAP_SERVERS=localhost:9092
export CLUSTER2_BOOTSTRAP_SERVERS=localhost:9095
export CLUSTER3_BOOTSTRAP_SERVERS=localhost:9096
export SUPERSTREAM_TOPICS_LIST=cluster1-topic-a,cluster2-topic

java -javaagent:superstream-clients-1.0.0.jar -jar multi-producer-multi-cluster-example.jar
```

**Expected results:**
- All 4 producers created successfully
- Each logs its bootstrap format
- Optimization applied based on SUPERSTREAM_TOPICS_LIST

---

### 3. CustomProducerMultiClusterExample.java

**Purpose:** Tests custom classes extending KafkaProducer, including the problematic PricelineKafkaProducer.

**What it tests:**
- Superstream works with KafkaProducer subclasses
- PricelineKafkaProducer fix (only base constructor intercepted)
- Both Properties and Map constructors
- Custom producer logic preserved

**Key features:**
- 3 custom producer types:
  - MetricsAwareProducer: adds metrics tracking
  - PricelineKafkaProducer: simulates customer's problematic class
  - RetryableProducer: adds retry logic
- Small batch.size (16KB) to verify optimization

**How to run:**
```bash
export ENABLE_METRICS_LOGGING=true
export SUPERSTREAM_DEBUG=true
java -javaagent:superstream-clients-1.0.0.jar -jar custom-producer-multi-cluster-example.jar
```

**Expected results:**
```
[INFO] Created PricelineKafkaProducer for client: priceline-producer-cluster1
[INFO] NOTE: PricelineKafkaProducer should NOT be directly intercepted by Superstream
[superstream] Only intercepts underlying KafkaProducer constructor
```

---

### 4. SpringMultiProducerExample.java

**Purpose:** Tests Spring Boot integration with direct KafkaProducer beans using different bootstrap formats.

**What it tests:**
- Spring-managed KafkaProducer beans
- @Bean method interception
- Different bootstrap formats in Spring context
- @Scheduled task compatibility

**Key features:**
- 4 Spring-managed producers
- Each uses different bootstrap format
- Automatic message sending every 30 seconds
- Environment variable override support

**How to run:**
```bash
export SPRING_PRODUCER_COMPRESSION=lz4
export SPRING_PRODUCER_BATCH_SIZE=32768
export SPRING_PRODUCER_LINGER_MS=100

java -javaagent:superstream-clients-1.0.0.jar -jar spring-multi-producer-example.jar
```

**Expected results:**
- Spring context loads successfully
- Each producer logs its bootstrap format
- Scheduled tasks run without issues

---

### 5. SpringKafkaMultiProducerExample.java

**Purpose:** Tests Spring Kafka integration with ProducerFactory and KafkaTemplate, loading config from YAML.

**What it tests:**
- ProducerFactory interception
- KafkaTemplate functionality
- Configuration from application.yml
- Transactional producer support
- Different bootstrap formats

**Key features:**
- Uses Spring Kafka abstractions
- Loads configuration from application.yml
- Each ProducerFactory uses different bootstrap format
- Optional transactional support

**Required file: application.yml**
```yaml
cluster1:
  bootstrap:
    servers: ${CLUSTER1_BOOTSTRAP_SERVERS:localhost:9092}
    
spring:
  kafka:
    compression: ${SPRING_KAFKA_COMPRESSION:snappy}
    batch:
      size: ${SPRING_KAFKA_BATCH_SIZE:16384}
```

**How to run:**
```bash
export ENABLE_TRANSACTIONAL=true
java -javaagent:superstream-clients-1.0.0.jar -jar spring-kafka-multi-producer-example.jar
```

**Expected results:**
- Configuration loaded from application.yml
- Each ProducerFactory logs its bootstrap format
- Transactional producers work if enabled

---

## New Classes

### 6. MultiProducerWithMapExample.java

**Purpose:** Tests multiple producers using Map/HashMap configuration instead of Properties.

**What it tests:**
- Map-based configuration (vs Properties)
- All bootstrap server formats with Maps
- Multiple clusters and producers
- Concurrent operation

**Key features:**
- Uses HashMap/Map exclusively
- 4 different bootstrap formats
- Parallel message sending
- Small batch size for optimization testing

**How to run:**
```bash
export CLUSTER1_BOOTSTRAP_SERVERS=localhost:9092
export CLUSTER2_BOOTSTRAP_SERVERS=localhost:9095
export CLUSTER3_BOOTSTRAP_SERVERS=localhost:9096

java -javaagent:superstream-clients-1.0.0.jar -jar multi-producer-with-map-example.jar
```

**Expected results:**
```
[INFO] Creating producer with clientId: producer-cluster1-a using Map with String bootstrap format
[INFO] Creating producer with clientId: producer-cluster2 using Map with Arrays.asList() bootstrap format
```

---

### 7. ProducerPropsPassingExample.java

**Purpose:** Demonstrates the limitation where Superstream cannot optimize when properties are passed by value.

**What it tests:**
- Properties passed by reference (works)
- Properties copied before passing (doesn't work)
- Unmodifiable Map (doesn't work)
- Properties reuse (works but dangerous)
- Map passed by reference (works)

**Key features:**
- 5 different test cases
- Clear logging of before/after values
- Demonstrates when optimization fails

**How to run:**
```bash
export SUPERSTREAM_DEBUG=true
java -javaagent:superstream-clients-1.0.0.jar -jar producer-props-passing-example.jar
```

**Expected results:**
```
TEST CASE 1: Properties passed by reference
✅ SUCCESS: Superstream optimized batch.size from 16384 to 65536

TEST CASE 2: Properties copied before passing
❌ EXPECTED: Original properties were NOT modified (we passed a copy)

TEST CASE 3: Unmodifiable Map
❌ EXPECTED: Unmodifiable map prevents Superstream optimization
```

---

### 8. CustomKafkaProducerExample.java

**Purpose:** Specifically tests the PricelineKafkaProducer fix and other custom producer implementations.

**What it tests:**
- PricelineKafkaProducer not directly intercepted
- Custom producer functionality preserved
- Both Properties and Map constructors
- Multiple custom producer types

**Key features:**
- Exact reproduction of customer's PricelineKafkaProducer
- MetricsKafkaProducer with callbacks
- RetryKafkaProducer with custom retry logic
- Mix of Properties and Map configurations

**How to run:**
```bash
export SUPERSTREAM_DEBUG=true
java -javaagent:superstream-clients-1.0.0.jar -jar custom-kafka-producer-example.jar
```

**Expected results:**
```
[INFO] Created PricelineKafkaProducer for client: priceline-producer
[INFO] PricelineKafkaProducer flushing for client: priceline-producer
[superstream] Intercepted KafkaProducer constructor (not PricelineKafkaProducer)
```

---

### 9. AllBootstrapFormatsExample.java

**Purpose:** Comprehensive test of ALL possible bootstrap server formats.

**What it tests:**
- Every possible format for bootstrap.servers
- Properties-based formats
- Map-based formats
- Edge cases (empty list, spaces, immutable lists)

**Key features:**
- Tests 13+ different formats
- Functional interface for clean testing
- Success/failure logging for each format
- Edge case handling

**How to run:**
```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_BOOTSTRAP_SERVERS_2=localhost:9095
java -javaagent:superstream-clients-1.0.0.jar -jar all-bootstrap-formats-example.jar
```

**Expected results:**
```
Testing format: Properties + String (single server)
✅ SUCCESS: Producer created and message sent

Testing format: Map + List.of()
✅ SUCCESS: Producer created and message sent

Testing format: Empty list (should fail)
❌ FAILED: Format Empty list - Error: Bootstrap servers cannot be empty
```

---

### 10. MultiProducerWithPropertiesExample.java

**Purpose:** Multiple producers using Properties with focus on different bootstrap formats.

**What it tests:**
- Properties-based configuration (complement to Map example)
- Different bootstrap formats per producer
- Multiple clusters
- Concurrent operation

**Key features:**
- Properties exclusively (vs MultiProducerWithMapExample)
- 4 producers with different bootstrap formats
- Comprehensive format coverage
- Small batch size for optimization testing

**How to run:**
```bash
export CLUSTER1_BOOTSTRAP_SERVERS=localhost:9092
export CLUSTER2_BOOTSTRAP_SERVERS=localhost:9095
export CLUSTER3_BOOTSTRAP_SERVERS=localhost:9096

java -javaagent:superstream-clients-1.0.0.jar -jar multi-producer-with-properties-example.jar
```

**Expected results:**
```
[INFO] Creating producer with clientId: producer-cluster1-a using String bootstrap format
[INFO] Creating producer with clientId: producer-cluster3 using List.of() bootstrap format
```

---

## Testing Scenarios

### Scenario 1: Basic Functionality Test
```bash
# Run each example with Superstream enabled
export SUPERSTREAM_DEBUG=true
export SUPERSTREAM_TOPICS_LIST=example-topic,cluster1-topic-a,spring-topic-1a

# Run basic example
java -javaagent:superstream-clients-1.0.0.jar -jar kafka-clients-example.jar

# Check logs for optimization
grep "Overriding configuration" output.log
```

### Scenario 2: Bootstrap Format Compatibility
```bash
# Test all formats with AllBootstrapFormatsExample
java -javaagent:superstream-clients-1.0.0.jar -jar all-bootstrap-formats-example.jar

# Verify all formats work (except empty list)
grep "SUCCESS" output.log | wc -l  # Should be 12+
```

### Scenario 3: PricelineKafkaProducer Fix Validation
```bash
# Run custom producer example
export SUPERSTREAM_DEBUG=true
java -javaagent:superstream-clients-1.0.0.jar -jar custom-kafka-producer-example.jar

# Verify PricelineKafkaProducer constructor NOT intercepted
grep "Intercepting constructor of PricelineKafkaProducer" output.log  # Should NOT appear
```

### Scenario 4: Properties Passing Problem
```bash
# Run props passing example
java -javaagent:superstream-clients-1.0.0.jar -jar producer-props-passing-example.jar

# Verify which cases work and which don't
grep "SUCCESS\|EXPECTED" output.log
```

### Scenario 5: Spring Integration
```bash
# Test Spring Boot
java -javaagent:superstream-clients-1.0.0.jar -jar spring-multi-producer-example.jar

# Test Spring Kafka with YAML
java -javaagent:superstream-clients-1.0.0.jar -jar spring-kafka-multi-producer-example.jar
```

---

## Expected Results

### When Superstream is Working Correctly:

1. **Configuration Optimization:**
```
[superstream] INFO ConfigurationOptimizer: Overriding configuration: batch.size=65536 (was: 32768)
[superstream] INFO ConfigurationOptimizer: Overriding configuration: linger.ms=5000 (was: 100)
[superstream] INFO ConfigurationOptimizer: Overriding configuration: compression.type=zstd (was: lz4)
```

2. **Client Reporting:**
```
[superstream] INFO ClientReporter: Successfully reported client information to superstream.clients
```

3. **Metrics Collection:**
```
[superstream] INFO ClientStatsReporter: Producer stats sent: before=260339 bytes, after=1170 bytes
```

### When Testing Edge Cases:

1. **Properties by Value (Won't Optimize):**
```
Original properties were NOT modified (we passed a copy)
```

2. **Custom Producers (Should Work):**
```
Created MetricsAwareProducer: metrics-producer with client.id: metrics-producer
Metrics for metrics-producer: Messages sent: 150, Bytes sent: 614400, Errors: 0
```

3. **Different Bootstrap Formats (All Should Work):**
```
Using List format for bootstrap servers: [localhost:9092, localhost:9095]
Using Arrays.asList() format for bootstrap servers: [localhost:9092]
Using List.of() format for bootstrap servers: [localhost:9092]
```

---

## Quick Reference

### Class Comparison Table

| Class | Type | Config | Bootstrap Formats | Special Features |
|-------|------|--------|------------------|------------------|
| KafkaProducerExample | Modified | Properties | Configurable (1) | Single producer baseline |
| MultiProducerMultiClusterExample | Modified | Properties | All 4 formats | Multiple clusters |
| CustomProducerMultiClusterExample | Modified | Mixed | String | PricelineKafkaProducer |
| SpringMultiProducerExample | Modified | Properties | All 4 formats | Spring beans |
| SpringKafkaMultiProducerExample | Modified | Map | All 4 formats | Spring Kafka + YAML |
| MultiProducerWithMapExample | New | Map | All 4 formats | Map-based config |
| ProducerPropsPassingExample | New | Mixed | String | Props passing demo |
| CustomKafkaProducerExample | New | Mixed | String | PricelineKafkaProducer focus |
| AllBootstrapFormatsExample | New | Mixed | ALL (13+) | Format testing |
| MultiProducerWithPropertiesExample | New | Properties | All 4 formats | Properties focus |

### Bootstrap Format Support

- **String**: `"localhost:9092"` or `"localhost:9092,localhost:9095"`
- **List**: `new ArrayList<>(Arrays.asList("localhost:9092"))`
- **Arrays.asList()**: `Arrays.asList("localhost:9092", "localhost:9095")`
- **List.of()**: `List.of("localhost:9092")` (Java 9+)

### Common Test Commands

```bash
# Enable all Superstream features
export SUPERSTREAM_DEBUG=true
export SUPERSTREAM_TOPICS_LIST=topic1,topic2,topic3

# Test with optimization disabled
export SUPERSTREAM_DISABLED=true

# Test latency-sensitive mode
export SUPERSTREAM_LATENCY_SENSITIVE=true

# Run any example
java -javaagent:superstream-clients-1.0.0.jar -jar [example-name].jar
```

---

## Troubleshooting

### Issue: Optimization Not Applied
- Check SUPERSTREAM_DISABLED is not true
- Verify topic is in SUPERSTREAM_TOPICS_LIST
- Ensure metadata topic has optimization config

### Issue: Bootstrap Format Error
- Verify format is supported
- Check for typos in BOOTSTRAP_FORMAT
- Ensure servers are reachable

### Issue: Spring Context Fails
- Check application.yml is in classpath
- Verify Spring dependencies
- Look for bean creation errors

### Issue: PricelineKafkaProducer Intercepted
- Ensure using latest Superstream agent with fix
- Check agent uses `.KafkaProducer` pattern
- Verify with SUPERSTREAM_DEBUG=true