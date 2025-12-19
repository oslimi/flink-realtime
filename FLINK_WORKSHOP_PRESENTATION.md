# Apache Flink Workshop: Real-Time Fraud Detection

## 🎯 Workshop Overview

This workshop demonstrates Apache Flink's real-time stream processing capabilities through a practical **Fraud Detection** use case. Participants will learn core Flink concepts including stateful processing, timers, and Kafka integration.

---

## 📋 Table of Contents

1. [Introduction](#introduction)
2. [Architecture](#architecture)
3. [Infrastructure Setup](#infrastructure-setup)
4. [Data Model](#data-model)
5. [Flink Concepts Demonstrated](#flink-concepts-demonstrated)
6. [Code Walkthrough](#code-walkthrough)
7. [Running the Demo](#running-the-demo)
8. [Testing Scenarios](#testing-scenarios)
9. [Future Topics](#future-topics-for-next-editions)

---

## 🚀 Introduction

### What is Apache Flink?

Apache Flink is a **distributed stream processing framework** designed for:
- **Stateful computations** over unbounded and bounded data streams
- **Event-time processing** with support for out-of-order events
- **Exactly-once semantics** for fault-tolerant processing
- **Low latency** and **high throughput** processing

### Use Case: Real-Time Fraud Detection

Financial institutions need to detect fraudulent transactions in real-time. This demo implements a pattern-based fraud detection system that:

1. **Detects suspicious patterns**: A small transaction (< $100) followed by a large transaction (> $50,000) from the same account
2. **Generates alerts**: Immediate notification when fraud is detected
3. **Produces periodic reports**: Aggregated fraud reports every minute

---

## 🏗️ Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Data Pipeline                                   │
│                                                                              │
│  ┌──────────┐    ┌─────────────────┐    ┌──────────────────┐    ┌────────┐ │
│  │  Kafka   │    │ Fraud Detection │    │ Report Aggregator│    │ Kafka  │ │
│  │(input)   │───▶│   Processor     │───▶│    Processor     │───▶│(output)│ │
│  │          │    │   (Stateful)    │    │    (Timers)      │    │        │ │
│  └──────────┘    └─────────────────┘    └──────────────────┘    └────────┘ │
│       │                  │                       │                    │     │
│       │                  ▼                       ▼                    │     │
│       │          ┌─────────────┐         ┌─────────────┐              │     │
│       │          │   Alerts    │         │   Reports   │              │     │
│       │          │   Topic     │         │   Topic     │              │     │
│       │          └─────────────┘         └─────────────┘              │     │
│  transactions         fraud-alerts           fraud-reports                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Processing Pipeline

```
Kafka Source (transactions)
        │
        ▼
┌───────────────────────────────────┐
│   Parse JSON to Transaction       │
│   (MapFunction)                   │
└───────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│   KeyBy: srcAccountId             │
│   (Partitioning)                  │
└───────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│   AdvancedStatefulFraudDetection  │
│   - ValueState<Transaction>       │
│   - Pattern Detection             │
│   - Emits FraudAdvancedAlert      │
└───────────────────────────────────┘
        │
        ├──────────────────────────────▶ Kafka Sink (fraud-alerts)
        │
        ▼
┌───────────────────────────────────┐
│   KeyBy: accountId                │
│   (Partitioning)                  │
└───────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│   FraudReportAggregator           │
│   - ListState<FraudAdvancedAlert> │
│   - Processing Time Timer (60s)   │
│   - Emits FraudReport             │
└───────────────────────────────────┘
        │
        ▼
Kafka Sink (fraud-reports)
```

---

## 🐳 Infrastructure Setup

### Docker Services

| Service | Image | Port | Description |
|---------|-------|------|-------------|
| **broker** | `apache/kafka:latest` | 9092 | Kafka broker (KRaft mode) |
| **postgresql** | `postgres:14` | 5432 | Database for Conduktor |
| **conduktor-console** | `conduktor/conduktor-console:1.41.0` | 8080 | Kafka management UI |
| **flink-jobmanager** | `flink:1.20` | 8081 | Flink cluster coordinator |
| **flink-taskmanager** | `flink:1.20` | - | Flink worker (4 task slots) |

### Starting the Infrastructure

```bash
cd infrastructure
docker compose up -d
```

### Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| Conduktor UI | http://localhost:8080 | admin@conduktor.io / adminP4ss! |
| Flink Dashboard | http://localhost:8081 | - |
| Kafka Broker | localhost:9092 | - |

---

## 📊 Data Model

### Transaction (Input)

```java
public record Transaction(
    String transactionId,    // Unique transaction ID
    String srcAccountId,     // Source account (key for partitioning)
    String destAccountId,    // Destination account
    Double amount,           // Transaction amount
    String currency,         // Currency code (EUR, USD, etc.)
    Long eventTime           // Event timestamp
) implements Serializable {}
```

**Example JSON:**
```json
{
  "transactionId": "txn-001",
  "srcAccountId": "acc-12345",
  "destAccountId": "acc-67890",
  "amount": 50.00,
  "currency": "EUR",
  "eventTime": 1702900000000
}
```

### FraudAdvancedAlert (Output)

```java
@Builder
public record FraudAdvancedAlert(
    String AlertId,                    // Unique alert ID
    Long timestamp,                    // Alert generation time
    Transaction previousTransaction,   // The small transaction
    Transaction currentTransaction,    // The large transaction
    String comment                     // Description of the fraud pattern
) implements Serializable {}
```

### FraudReport (Aggregated Output)

```java
@Builder
public record FraudReport(
    String reportId,          // Unique report ID
    Long reportTimestamp,     // Report generation time
    long windowStart,         // Window start timestamp
    long windowEnd,           // Window end timestamp
    String accountId,         // Account ID
    int totalAlerts,          // Number of alerts in window
    double totalFraudAmount,  // Total fraud amount
    List<String> alertIds,    // List of alert IDs
    String summary            // Human-readable summary
) implements Serializable {}
```

---

## 🎓 Flink Concepts Demonstrated

### 1. KeyedProcessFunction

The `KeyedProcessFunction` is a low-level API that provides access to:
- **State**: Managed state scoped to the current key
- **Timers**: Ability to register and react to timers
- **Context**: Access to timestamp, key, and side outputs

```java
public class AdvancedStatefulFraudDetectionProcessor 
    extends KeyedProcessFunction<String, Transaction, FraudAdvancedAlert> {
    
    @Override
    public void processElement(Transaction transaction, Context ctx, Collector<FraudAdvancedAlert> out) {
        // Process each element
    }
    
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<FraudAdvancedAlert> out) {
        // React to timer firing
    }
}
```

### 2. ValueState (Keyed State)

`ValueState<T>` stores a single value per key. Used to remember the previous transaction.

```java
// Declaration
private transient ValueState<Transaction> previousTransactionState;

// Initialization in open()
previousTransactionState = getRuntimeContext().getState(
    new ValueStateDescriptor<>("previous-transaction", Transaction.class));

// Usage
Transaction previous = previousTransactionState.value();  // Read
previousTransactionState.update(transaction);              // Write
previousTransactionState.clear();                          // Clear
```

### 3. ListState (Keyed State)

`ListState<T>` stores a list of values per key. Used to collect alerts for aggregation.

```java
// Declaration
private transient ListState<FraudAdvancedAlert> alertsState;

// Initialization in open()
alertsState = getRuntimeContext().getListState(
    new ListStateDescriptor<>("alerts-list", FraudAdvancedAlert.class));

// Usage
alertsState.add(alert);           // Add element
for (FraudAdvancedAlert a : alertsState.get()) { }  // Iterate
alertsState.clear();              // Clear all
```

### 4. Processing Time Timers

Timers allow scheduling callbacks at specific points in time.

```java
// Register a timer
long nextTimer = context.timerService().currentProcessingTime() + 60_000L;
context.timerService().registerProcessingTimeTimer(nextTimer);

// Timer callback
@Override
public void onTimer(long timestamp, OnTimerContext ctx, Collector<FraudReport> out) {
    // This is called when the timer fires
    // Generate report, schedule next timer
}
```

### 5. Kafka Integration

**Kafka Source:**
```java
KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
    .setBootstrapServers("localhost:9092")
    .setTopics("transactions")
    .setGroupId("fraud-detection-group")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();
```

**Kafka Sink:**
```java
KafkaSink<FraudAdvancedAlert> alertsSink = KafkaSink.<FraudAdvancedAlert>builder()
    .setBootstrapServers("localhost:9092")
    .setRecordSerializer(KafkaRecordSerializationSchema.<FraudAdvancedAlert>builder()
        .setTopic("fraud-alerts")
        .setValueSerializationSchema(new FraudAdvancedAlertSerializationSchema(objectMapper))
        .build())
    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    .build();
```

---

## 💻 Code Walkthrough

### Main Application Flow

```java
// 1. Create execution environment
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// 2. Read from Kafka
DataStream<Transaction> transactions = env
    .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
    .map(json -> objectMapper.readValue(json, Transaction.class));

// 3. Fraud Detection (Stateful)
DataStream<FraudAdvancedAlert> fraudAlerts = transactions
    .keyBy(Transaction::srcAccountId)
    .process(new AdvancedStatefulFraudDetectionProcessor());

// 4. Report Aggregation (Timers)
DataStream<FraudReport> fraudReports = fraudAlerts
    .keyBy(alert -> alert.currentTransaction().srcAccountId())
    .process(new FraudReportAggregatorProcessor());

// 5. Sink to Kafka
fraudAlerts.sinkTo(alertsSink);
fraudReports.sinkTo(reportsSink);

// 6. Execute
env.execute("Fraud Detection Job");
```

### Fraud Detection Logic

```java
@Override
public void processElement(Transaction transaction, Context ctx, Collector<FraudAdvancedAlert> out) {
    Transaction previous = previousTransactionState.value();
    
    // Pattern: small tx (< 100) followed by large tx (> 50000)
    if (previous != null 
        && previous.amount() < 100.0 
        && transaction.amount() > 50000.0) {
        
        // FRAUD DETECTED!
        FraudAdvancedAlert alert = FraudAdvancedAlert.builder()
            .AlertId(UUID.randomUUID().toString())
            .timestamp(System.currentTimeMillis())
            .previousTransaction(previous)
            .currentTransaction(transaction)
            .comment("Fraud pattern detected")
            .build();
        
        out.collect(alert);
        previousTransactionState.clear();
    } else {
        previousTransactionState.update(transaction);
    }
}
```

### Report Aggregation with Timers

```java
@Override
public void processElement(FraudAdvancedAlert alert, Context ctx, Collector<FraudReport> out) {
    // Initialize timer on first element
    if (nextTimerState.value() == null) {
        long nextTimer = ctx.timerService().currentProcessingTime() + 60_000L;
        ctx.timerService().registerProcessingTimeTimer(nextTimer);
        nextTimerState.update(nextTimer);
    }
    
    // Collect alerts
    alertsState.add(alert);
}

@Override
public void onTimer(long timestamp, OnTimerContext ctx, Collector<FraudReport> out) {
    // Aggregate alerts
    List<FraudAdvancedAlert> alerts = new ArrayList<>();
    for (FraudAdvancedAlert a : alertsState.get()) {
        alerts.add(a);
    }
    
    // Emit report
    if (!alerts.isEmpty()) {
        FraudReport report = FraudReport.builder()
            .reportId(UUID.randomUUID().toString())
            .totalAlerts(alerts.size())
            .build();
        out.collect(report);
    }
    
    // Clear and reschedule
    alertsState.clear();
    ctx.timerService().registerProcessingTimeTimer(timestamp + 60_000L);
}
```

---

## ▶️ Running the Demo

### Step 1: Start Infrastructure

```bash
cd infrastructure
docker compose up -d
```

### Step 2: Create Kafka Topics

```bash
docker exec -it broker /opt/kafka/bin/kafka-topics.sh \
  --create --topic transactions --bootstrap-server localhost:9092 --partitions 3

docker exec -it broker /opt/kafka/bin/kafka-topics.sh \
  --create --topic fraud-alerts --bootstrap-server localhost:9092 --partitions 3

docker exec -it broker /opt/kafka/bin/kafka-topics.sh \
  --create --topic fraud-reports --bootstrap-server localhost:9092 --partitions 3
```

### Step 3: Build the Application

```bash
mvn clean package -pl fraud-detection -am
```

### Step 4: Run from IDE

Run `FlinkFraudDetectionApplication.java` from IntelliJ IDEA.

### Step 5: Send Test Transactions

**Normal transaction:**
```bash
docker exec -it broker /opt/kafka/bin/kafka-console-producer.sh \
  --topic transactions --bootstrap-server localhost:9092

{"transactionId":"txn-001","srcAccountId":"acc-123","destAccountId":"acc-456","amount":500.0,"currency":"EUR","eventTime":1702900000000}
```

**Fraud pattern (small then large):**
```bash
{"transactionId":"txn-002","srcAccountId":"acc-123","destAccountId":"acc-789","amount":50.0,"currency":"EUR","eventTime":1702900001000}
{"transactionId":"txn-003","srcAccountId":"acc-123","destAccountId":"acc-999","amount":75000.0,"currency":"EUR","eventTime":1702900002000}
```

### Step 6: Verify Output

**Check alerts:**
```bash
docker exec -it broker /opt/kafka/bin/kafka-console-consumer.sh \
  --topic fraud-alerts --bootstrap-server localhost:9092 --from-beginning
```

**Check reports (wait 1 minute):**
```bash
docker exec -it broker /opt/kafka/bin/kafka-console-consumer.sh \
  --topic fraud-reports --bootstrap-server localhost:9092 --from-beginning
```

---

## 🧪 Testing Scenarios

### Scenario 1: Normal Transactions
Send multiple transactions with amounts between $100 and $50,000.
**Expected:** No alerts generated.

### Scenario 2: Single Fraud Pattern
1. Send transaction with amount < $100
2. Send transaction with amount > $50,000 (same account)
**Expected:** One alert generated immediately.

### Scenario 3: Multiple Fraud Patterns
Send multiple fraud patterns within 1 minute.
**Expected:** Multiple alerts, one aggregated report after 60 seconds.

### Scenario 4: Different Accounts
Send fraud patterns from different accounts.
**Expected:** Separate state maintained per account, separate reports.

---

## 🔮 Future Topics for Next Editions

### 1. **Event Time Processing & Watermarks**
- Handle out-of-order events
- Configure watermark strategies
- Late data handling with side outputs

```java
WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withTimestampAssigner((event, timestamp) -> event.eventTime());
```

### 2. **Windowing**
- Tumbling Windows
- Sliding Windows
- Session Windows
- Global Windows with custom triggers

```java
transactions
    .keyBy(Transaction::srcAccountId)
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .aggregate(new TransactionAggregator());
```

### 3. **CEP (Complex Event Processing)**
- Pattern detection with Flink CEP
- Sequence patterns
- Time constraints

```java
Pattern<Transaction, ?> fraudPattern = Pattern.<Transaction>begin("small")
    .where(tx -> tx.amount() < 100)
    .next("large")
    .where(tx -> tx.amount() > 50000)
    .within(Time.minutes(10));
```

### 4. **Checkpointing & Fault Tolerance**
- Enable checkpointing
- Configure state backends (RocksDB)
- Exactly-once semantics

```java
env.enableCheckpointing(60000);
env.setStateBackend(new EmbeddedRocksDBStateBackend());
```

### 5. **Async I/O**
- External service calls without blocking
- Database lookups
- API enrichment

```java
AsyncDataStream.unorderedWait(
    transactions,
    new AsyncDatabaseRequest(),
    1000, TimeUnit.MILLISECONDS,
    100  // capacity
);
```

### 6. **Table API & SQL**
- Declarative stream processing
- SQL queries on streams
- Temporal joins

```sql
SELECT 
    srcAccountId,
    COUNT(*) as tx_count,
    SUM(amount) as total_amount
FROM transactions
GROUP BY 
    srcAccountId,
    TUMBLE(eventTime, INTERVAL '5' MINUTE);
```

### 7. **Machine Learning Integration**
- ONNX model inference
- Real-time scoring
- Feature engineering

### 8. **Monitoring & Metrics**
- Custom metrics
- Prometheus integration
- Grafana dashboards

### 9. **Savepoints & State Migration**
- Application upgrades
- State schema evolution
- Job migration

### 10. **Side Outputs**
- Multiple output streams
- Late data handling
- Error routing

```java
OutputTag<Transaction> lateDataTag = new OutputTag<>("late-data") {};

SingleOutputStreamOperator<Result> result = stream
    .process(new ProcessFunction<>() {
        @Override
        public void processElement(Transaction tx, Context ctx, Collector<Result> out) {
            if (isLate(tx)) {
                ctx.output(lateDataTag, tx);
            } else {
                out.collect(process(tx));
            }
        }
    });

DataStream<Transaction> lateData = result.getSideOutput(lateDataTag);
```

---

## 📚 Resources

- [Apache Flink Documentation](https://flink.apache.org/docs/)
- [Flink Training](https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/overview/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Conduktor Documentation](https://docs.conduktor.io/)

---

## 👥 Workshop Exercises

### Exercise 1: Add a New Fraud Pattern
Implement detection for: 3 or more transactions within 1 minute from the same account.

### Exercise 2: Add Event Time Processing
Modify the pipeline to use event time instead of processing time.

### Exercise 3: Add a Database Sink
Write fraud alerts to PostgreSQL using JDBC connector.

### Exercise 4: Add Metrics
Add custom metrics to track:
- Number of transactions processed
- Number of fraud alerts generated
- Processing latency

---

## 🎉 Summary

In this workshop, we covered:

✅ **Stateful Stream Processing** with ValueState and ListState  
✅ **Processing Time Timers** for periodic aggregation  
✅ **Kafka Integration** for source and sink  
✅ **KeyedProcessFunction** for low-level control  
✅ **Real-world Use Case**: Fraud Detection  

**Next Steps:**
- Explore event time processing
- Implement windowing
- Add checkpointing for fault tolerance
- Integrate with external systems

---

*Happy Streaming! 🚀*

