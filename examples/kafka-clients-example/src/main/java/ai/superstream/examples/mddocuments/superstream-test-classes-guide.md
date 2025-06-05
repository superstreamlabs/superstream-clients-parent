# Superstream SDK Test Classes Complete Guide

## Overview
This guide provides comprehensive information about all test classes in the Superstream SDK test suite, including configuration, producer counts, topics, and operational details.

## Test Classes Summary

### 1. KafkaProducerExample
**Purpose:** Basic single-producer test with configurable bootstrap server formats

**Configuration:**
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SUPERSTREAM_TOPICS_LIST=example-topic
SUPERSTREAM_DEBUG=true
SUPERSTREAM_LATENCY_SENSITIVE=false
SUPERSTREAM_DISABLED=false
BOOTSTRAP_FORMAT=string
```

**Producer Details:**
- **Number of Producers:** 1
- **Topics:** `example-topic`
- **Data Type:** JSON (~1KB) with metadata and repeating metrics patterns
- **Frequency:** 50 messages every 60 seconds
- **Compression:** `zstd`
- **Batch Size:** `1048576` (1MB)
- **Linger.ms:** `500`
- **Special Features:** Tests different bootstrap server formats (string/list/arrays_list/list_of)

---

### 2. MultiProducerMultiClusterExample
**Purpose:** Tests multiple producers across multiple clusters with different bootstrap formats

**Configuration:**
```bash
CLUSTER1_BOOTSTRAP_SERVERS=localhost:9092
CLUSTER2_BOOTSTRAP_SERVERS=localhost:9095
CLUSTER3_BOOTSTRAP_SERVERS=localhost:9096
SUPERSTREAM_TOPICS_LIST=cluster1-topic-a,cluster1-topic-b,cluster2-topic,cluster3-topic
SUPERSTREAM_DEBUG=true
SUPERSTREAM_LATENCY_SENSITIVE=false
SUPERSTREAM_DISABLED=false
```

**Producer Details:**
- **Number of Producers:** 4
- **Topics:** `cluster1-topic-a`, `cluster1-topic-b`, `cluster2-topic`, `cluster3-topic`
- **Data Type:** JSON (~3KB) with metrics and configuration
- **Frequency:** 50 messages every 30 seconds per producer
- **Compression:** `lz4`
- **Batch Size:** `32768` (32KB)
- **Linger.ms:** `100`
- **Special Features:** Each producer uses different bootstrap format (String, List, Arrays.asList, List.of)

---

### 3. CustomProducerMultiClusterExample
**Purpose:** Tests custom classes extending KafkaProducer, including PricelineKafkaProducer fix

**Configuration:**
```bash
CLUSTER1_BOOTSTRAP_SERVERS=localhost:9092
CLUSTER2_BOOTSTRAP_SERVERS=localhost:9095
SUPERSTREAM_TOPICS_LIST=custom-producer-topic-1,custom-producer-topic-2
SUPERSTREAM_DEBUG=true
SUPERSTREAM_LATENCY_SENSITIVE=false
SUPERSTREAM_DISABLED=false
ENABLE_METRICS_LOGGING=true
```

**Producer Details:**
- **Number of Producers:** 3
  - MetricsAwareProducer (tracks metrics)
  - PricelineKafkaProducer (simulates customer's problematic class)
  - RetryableProducer (adds retry logic)
- **Topics:** 
  - `custom-producer-topic-1` (2 producers)
  - `custom-producer-topic-2` (1 producer)
- **Data Type:** JSON with repeating pattern for compression
- **Frequency:** 50, 30, 30 messages every 30 seconds respectively
- **Compression:** `snappy`
- **Batch Size:** `16384` (16KB)
- **Linger.ms:** `200` (base), `300` (PricelineKafkaProducer)

---

### 4. SpringMultiProducerExample
**Purpose:** Spring Boot application with direct KafkaProducer beans

**Configuration:**
```bash
CLUSTER1_BOOTSTRAP_SERVERS=localhost:9092
CLUSTER2_BOOTSTRAP_SERVERS=localhost:9095
CLUSTER3_BOOTSTRAP_SERVERS=localhost:9096
SUPERSTREAM_TOPICS_LIST=spring-topic-1a,spring-topic-1b,spring-topic-2,spring-topic-3
SUPERSTREAM_DEBUG=true
SUPERSTREAM_LATENCY_SENSITIVE=false
SUPERSTREAM_DISABLED=false
SPRING_PRODUCER_COMPRESSION=gzip
SPRING_PRODUCER_BATCH_SIZE=32768
SPRING_PRODUCER_LINGER_MS=100
```

**Producer Details:**
- **Number of Producers:** 4
- **Topics:** `spring-topic-1a`, `spring-topic-1b`, `spring-topic-2`, `spring-topic-3`
- **Data Type:** JSON with Spring Boot metadata and compressible content
- **Frequency:** 20, 25, 30, 35 messages every 30 seconds
- **Compression:** `gzip` (default), `lz4` (cluster3)
- **Batch Size:** `32768` (default), `65536` (cluster3)
- **Linger.ms:** `100`
- **Special Features:** Spring-managed beans with @Scheduled tasks

---

### 5. SpringKafkaMultiProducerExample
**Purpose:** Spring Boot with Spring Kafka (ProducerFactory, KafkaTemplate) and YAML configuration

**Configuration:**
```bash
CLUSTER1_BOOTSTRAP_SERVERS=localhost:9092
CLUSTER2_BOOTSTRAP_SERVERS=localhost:9095
CLUSTER3_BOOTSTRAP_SERVERS=localhost:9096
SUPERSTREAM_TOPICS_LIST=spring-kafka-topic-1a,spring-kafka-topic-1b,spring-kafka-topic-2,spring-kafka-topic-3
SUPERSTREAM_DEBUG=true
SUPERSTREAM_LATENCY_SENSITIVE=false
SUPERSTREAM_DISABLED=false
ENABLE_TRANSACTIONAL=false
SPRING_KAFKA_COMPRESSION=snappy
SPRING_KAFKA_BATCH_SIZE=16384
SPRING_KAFKA_LINGER_MS=50
```

**Producer Details:**
- **Number of Producers:** 4 (via ProducerFactory/KafkaTemplate)
- **Topics:** `spring-kafka-topic-1a`, `spring-kafka-topic-1b`, `spring-kafka-topic-2`, `spring-kafka-topic-3`
- **Data Type:** JSON with framework metadata, Spring version, config source
- **Frequency:** 25, 30, 35, 40 messages every 30 seconds
- **Compression:** `snappy`
- **Batch Size:** `16384` (16KB)
- **Linger.ms:** `50` (default), `200` (cluster1B)
- **Special Features:** Transactional support for cluster3 (if enabled), loads from application.yml

---

### 6. MultiProducerWithMapExample
**Purpose:** Multiple producers using Map/HashMap configuration instead of Properties

**Configuration:**
```bash
CLUSTER1_BOOTSTRAP_SERVERS=localhost:9092
CLUSTER2_BOOTSTRAP_SERVERS=localhost:9095
CLUSTER3_BOOTSTRAP_SERVERS=localhost:9096
SUPERSTREAM_TOPICS_LIST=cluster1-topic-a,cluster1-topic-b,cluster2-topic,cluster3-topic
SUPERSTREAM_DEBUG=true
SUPERSTREAM_LATENCY_SENSITIVE=false
SUPERSTREAM_DISABLED=false
```

**Producer Details:**
- **Number of Producers:** 4
- **Topics:** `cluster1-topic-a`, `cluster1-topic-b`, `cluster2-topic`, `cluster3-topic`
- **Data Type:** JSON (~3KB) identical to MultiProducerMultiClusterExample
- **Frequency:** 50 messages every 30 seconds per producer
- **Compression:** `lz4`
- **Batch Size:** `32768` (32KB)
- **Linger.ms:** `100`
- **Special Features:** Uses HashMap instead of Properties

---

### 7. ProducerPropsPassingExample
**Purpose:** Demonstrates the limitation where properties passed by value cannot be optimized

**Configuration:**
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SUPERSTREAM_TOPICS_LIST=props-passing-test-topic
SUPERSTREAM_DEBUG=true
SUPERSTREAM_LATENCY_SENSITIVE=false
SUPERSTREAM_DISABLED=false
```

**Producer Details:**
- **Number of Producers:** 5 (test cases)
- **Topics:** `props-passing-test-topic`
- **Data Type:** Simple JSON with testCase and timestamp
- **Frequency:** 1 test message per test case
- **Compression:** `gzip`
- **Batch Size:** `16384` (16KB) - to demonstrate optimization
- **Linger.ms:** `100`
- **Special Features:** 5 test cases showing when optimization works/fails

---

### 8. CustomKafkaProducerExample
**Purpose:** Focused testing of PricelineKafkaProducer fix and custom producer implementations

**Configuration:**
```bash
CLUSTER1_BOOTSTRAP_SERVERS=localhost:9092
CLUSTER2_BOOTSTRAP_SERVERS=localhost:9095
SUPERSTREAM_TOPICS_LIST=custom-producer-topic-1,custom-producer-topic-2
SUPERSTREAM_DEBUG=true
SUPERSTREAM_LATENCY_SENSITIVE=false
SUPERSTREAM_DISABLED=false
```

**Producer Details:**
- **Number of Producers:** 3
  - MetricsKafkaProducer: Tracks sent messages/bytes/errors
  - PricelineKafkaProducer: Logs flush/close operations
  - RetryKafkaProducer: Auto-retry with backoff (3 attempts, 1000ms)
- **Topics:** 
  - `custom-producer-topic-1` (2 producers)
  - `custom-producer-topic-2` (1 producer)
- **Data Type:** JSON with large repeating payload (50x pattern repetitions)
- **Frequency:** Continuous sending of 50 messages every 30 seconds
- **Compression:** `snappy`
- **Batch Size:** `16384` (16KB)
- **Linger.ms:** `200`/`300`

---

### 9. AllBootstrapFormatsExample
**Purpose:** Comprehensive test of ALL possible bootstrap.servers formats

**Configuration:**
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_BOOTSTRAP_SERVERS_2=localhost:9095
SUPERSTREAM_TOPICS_LIST=bootstrap-formats-test
SUPERSTREAM_DEBUG=true
SUPERSTREAM_LATENCY_SENSITIVE=false
SUPERSTREAM_DISABLED=false
TEST_TOPIC=bootstrap-formats-test
```

**Producer Details:**
- **Number of Producers:** 13+ (one per format test)
- **Topics:** `bootstrap-formats-test`
- **Data Type:** JSON with format name and timestamp
- **Frequency:** 1 message per format (one-time test)
- **Compression:** `gzip`
- **Batch Size:** `8192` (8KB) - intentionally small for testing
- **Linger.ms:** `50`
- **Special Features:** Tests 13+ formats including edge cases

---

### 10. MultiProducerWithPropertiesExample
**Purpose:** Multiple producers with focus on Properties (complement to Map example)

**Configuration:**
```bash
CLUSTER1_BOOTSTRAP_SERVERS=localhost:9092
CLUSTER2_BOOTSTRAP_SERVERS=localhost:9095
CLUSTER3_BOOTSTRAP_SERVERS=localhost:9096
SUPERSTREAM_TOPICS_LIST=cluster1-topic-a,cluster1-topic-b,cluster2-topic,cluster3-topic
SUPERSTREAM_DEBUG=true
SUPERSTREAM_LATENCY_SENSITIVE=false
SUPERSTREAM_DISABLED=false
```

**Producer Details:**
- **Number of Producers:** 4
- **Topics:** `cluster1-topic-a`, `cluster1-topic-b`, `cluster2-topic`, `cluster3-topic`
- **Data Type:** JSON (~3KB) with metrics and configuration
- **Frequency:** 50 messages every 30 seconds per producer
- **Compression:** `lz4`
- **Batch Size:** `32768` (32KB)
- **Linger.ms:** `100`
- **Special Features:** Properties-based configuration with different bootstrap formats

## Common Patterns Across All Tests

1. **Data Generation:** Most classes generate repeating data patterns for effective compression testing
2. **Batch Frequency:** Standard pattern is batching every 30 seconds
3. **Small Batch Sizes:** Intentionally small (16-32KB) to demonstrate Superstream optimization
4. **Reliability Settings:** All producers use `acks=all` and `enable.idempotence=true`
5. **Bootstrap Formats Tested:**
   - String: `"localhost:9092"`
   - List: `new ArrayList<>(Arrays.asList("localhost:9092"))`
   - Arrays.asList(): `Arrays.asList("localhost:9092")`
   - List.of(): `List.of("localhost:9092")` (Java 9+)

## Key Testing Scenarios

1. **Basic Functionality:** Single producer optimization (KafkaProducerExample)
2. **Scalability:** Multiple producers and clusters (MultiProducer* examples)
3. **Custom Classes:** Extension and inheritance support (Custom* examples)
4. **Spring Integration:** Both direct and Spring Kafka approaches
5. **Configuration Formats:** Properties vs Map, mutable vs immutable
6. **Edge Cases:** Empty lists, spaces in config, unmodifiable collections

## Expected Optimization Results

When Superstream is enabled and working correctly:
- `batch.size`: Typically optimized from 16-32KB to 64KB+
- `linger.ms`: May be increased from 50-100ms to 5000ms
- `compression.type`: May be changed to more efficient algorithms like `zstd`

## Running the Tests

All tests follow the same pattern:
```bash
# Set environment variables
export [CONFIGURATION_VARIABLES]

# Run with Superstream agent
java -javaagent:superstream-clients-1.0.0.jar -jar [test-class-name].jar
```

Monitor logs for:
- `[superstream] INFO ConfigurationOptimizer: Overriding configuration`
- `[INFO] Created [ProducerType] for client: [client-id]`
- Success/failure messages for each test case