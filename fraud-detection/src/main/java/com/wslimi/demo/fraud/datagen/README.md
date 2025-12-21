# Transaction Data Generator

## Overview

The **Transaction Data Generator** is a Flink-based application that generates realistic transaction data and streams it to Kafka. It uses Flink's DataStream API and leverages Flink's parallelism, fault tolerance, and checkpointing capabilities.

## Architecture

```
┌─────────────────────────────────────┐
│  TransactionSourceFunction          │
│  (Custom Flink SourceFunction)      │
│  - Generates transactions            │
│  - Configurable rate                 │
│  - Optional fraud patterns           │
└──────────────┬──────────────────────┘
               │
               │ DataStream<Transaction>
               │
               ▼
┌─────────────────────────────────────┐
│  Watermark Strategy                  │
│  (Event Time Processing)             │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  KafkaSink                          │
│  - Writes to Kafka topic            │
│  - AT_LEAST_ONCE delivery           │
└─────────────────────────────────────┘
```

## Components

### 1. GeneratorConfig
Configuration class for fine-grained control:
- **Kafka settings**: broker, topic
- **Generation rate**: messages per second, total messages
- **Transaction amounts**: min/max, currencies
- **Accounts**: number of accounts, prefix
- **Fraud simulation**: enable/disable, probability, amounts

### 2. TransactionGenerator
Core generation logic:
- Generates random transactions
- Injects fraud patterns (small transaction followed by large one)
- Realistic account distribution

### 3. TransactionSourceFunction
Flink SourceFunction implementation:
- Integrates generator with Flink DataStream API
- Respects Flink's checkpointing mechanism
- Supports parallel execution

### 4. TransactionDataGeneratorApp
Main application:
- Configures Flink execution environment
- Sets up DataStream pipeline
- Provides multiple configuration profiles

## Configuration Profiles

The generator comes with 5 pre-configured profiles:

### 1. Default (default)
```bash
# Moderate rate, no fraud patterns
Messages/sec: 10
Total: ∞ (infinite)
Amount: 10.0 - 5000.0
Accounts: 100
Fraud patterns: Disabled
```

### 2. Fraud Test (fraud)
```bash
# Fraud patterns enabled for testing
Messages/sec: 5
Total: ∞ (infinite)
Amount: 10.0 - 5000.0
Accounts: 50
Fraud patterns: Enabled (10% probability)
  - Small amount: 50.0
  - Large amount: 75000.0
Verbose: true
```

### 3. High Volume (high)
```bash
# Performance testing
Messages/sec: 100
Total: ∞ (infinite)
Amount: 10.0 - 10000.0
Accounts: 1000
Fraud patterns: Enabled (2% probability)
```

### 4. Low Volume (low)
```bash
# Easy observation
Messages/sec: 1
Total: 100 messages
Amount: 10.0 - 1000.0
Accounts: 10
Verbose: true
```

### 5. Demo (demo)
```bash
# Optimized for presentations
Messages/sec: 5
Total: ∞ (infinite)
Amount: 10.0 - 5000.0
Accounts: 20
Fraud patterns: Enabled (15% probability)
  - Small amount: 50.0
  - Large amount: 75000.0
Verbose: true
```

## Usage

### Running from IntelliJ

1. Open `TransactionDataGeneratorApp.java`
2. Run the main method
3. To use a specific profile, add program arguments:
   - `fraud` - Fraud test profile
   - `high` - High volume profile
   - `low` - Low volume profile
   - `demo` - Demo profile

### Running from Command Line

```bash
# Build the project
mvn clean package

# Run with default profile
java -cp target/fraud-detection-1.0-SNAPSHOT.jar \
  com.wslimi.demo.fraud.TransactionDataGeneratorApp

# Run with fraud test profile
java -cp target/fraud-detection-1.0-SNAPSHOT.jar \
  com.wslimi.demo.fraud.TransactionDataGeneratorApp fraud

# Run with demo profile
java -cp target/fraud-detection-1.0-SNAPSHOT.jar \
  com.wslimi.demo.fraud.TransactionDataGeneratorApp demo
```

### Monitoring

Once started, you can monitor the generator:

1. **Flink Web UI**: http://localhost:8081
   - View job status
   - Monitor throughput
   - Check operator metrics

2. **Conduktor UI**: http://localhost:8080
   - View Kafka topics
   - Inspect messages
   - Monitor consumer lag

3. **Console Logs**: Watch for fraud pattern indicators
   - `🚨 FRAUD PATTERN STARTED` - Small transaction detected
   - `🚨 FRAUD PATTERN COMPLETED` - Large transaction from same account

## Fraud Pattern Details

When fraud patterns are enabled, the generator simulates a common fraud detection scenario:

1. **Small Test Transaction**: A fraudster makes a small transaction (<100) to verify the account is active
2. **Large Fraudulent Transaction**: Shortly after, a large transaction (>50000) is made from the same account

This pattern is designed to be detected by the `StatefulFraudDetectionDemo` application.

## Stopping the Generator

### Graceful Shutdown
Press `Ctrl+C` in the terminal. The generator will:
1. Stop generating new transactions
2. Flush remaining messages to Kafka
3. Complete checkpointing
4. Shut down cleanly

### Force Stop
If needed, kill the process:
```bash
# Find the process
ps aux | grep TransactionDataGeneratorApp

# Kill it
kill -9 <PID>
```

## Integration with Demo Applications

The generated data feeds these demo applications:

1. **KafkaSourceDemo**: Basic Kafka source reading
2. **NaiveFraudDetectionDemo**: Simple threshold-based fraud detection
3. **StatefulFraudDetectionDemo**: Stateful fraud pattern detection
4. **TimerReportingDemo**: Time-based aggregation and reporting
5. **FlinkFraudDetectionFullApplication**: Complete fraud detection pipeline

## Customization

To create a custom configuration:

```java
GeneratorConfig customConfig = GeneratorConfig.builder()
    .kafkaBootstrapServers("localhost:9092")
    .topic("my-custom-topic")
    .messagesPerSecond(20)
    .totalMessages(1000) // 0 = infinite
    .minAmount(100.0)
    .maxAmount(10000.0)
    .numberOfAccounts(200)
    .enableFraudPatterns(true)
    .fraudPatternProbability(0.05) // 5%
    .fraudSmallAmount(75.0)
    .fraudLargeAmount(100000.0)
    .verbose(true)
    .build();

TransactionDataGeneratorApp app = new TransactionDataGeneratorApp(customConfig);
app.start();
```

## Troubleshooting

### Cannot connect to Kafka
- Ensure Kafka is running: `docker ps | grep broker`
- Check bootstrap servers configuration
- Verify network connectivity

### Low throughput
- Increase `messagesPerSecond` in config
- Increase Flink parallelism
- Check Kafka broker performance

### Flink Web UI not accessible
- Check port 8081 is not in use
- Look for "rest.port" configuration
- Check firewall settings

## Performance Considerations

- **Single parallelism**: Ensures ordered generation, but limits throughput
- **Multiple parallelism**: Increases throughput but loses strict ordering
- **Checkpointing**: Enabled every 10 seconds for fault tolerance
- **Backpressure**: Flink handles backpressure automatically

## Example Output

```
================================================================================
FLINK TRANSACTION DATA GENERATOR
================================================================================
Configuration:
  Kafka Broker: localhost:9092
  Topic: transactions
  Messages/sec: 5
  Total messages: ∞ (infinite)
  Amount range: 10.0 - 5000.0
  Number of accounts: 20
  Currencies: [USD, EUR, GBP]
  Fraud patterns enabled: true
    - Fraud probability: 15.0%
    - Small amount: 50.0
    - Large amount: 75000.0
================================================================================
Starting Flink Data Generator Job...
Flink Web UI: http://localhost:8081
Press Ctrl+C to stop...
================================================================================
Transaction Generator started - subtask 1/1
Starting transaction generation...
🚨 FRAUD PATTERN STARTED: Small transaction (50.0) from account acc-0012
Generated normal transaction: Transaction[transactionId=tx-a3b4c5d6, ...]
🚨 FRAUD PATTERN COMPLETED: Large transaction (75000.0) from account acc-0012
...
```

