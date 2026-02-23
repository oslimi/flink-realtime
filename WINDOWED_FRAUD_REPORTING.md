# Windowed Fraud Reporting Demo

## Overview

The `WindowedFraudReportingDemoApp` demonstrates an alternative approach to fraud report aggregation using **Flink's windowing abstraction** instead of timers. This approach provides better semantics for handling time-based aggregations.

## Key Differences: Windows vs Timers

### Timer-based Approach (FraudReportAggregatorProcessor)
```
✗ Complex state management (separate ListState + ValueState)
✗ Manual timer scheduling and tracking
✗ Difficult to reason about semantics (when does window close?)
✗ No built-in watermark integration
✓ More flexible for custom logic
```

### Window-based Approach (WindowedFraudReportProcessor)
```
✓ Automatic window lifecycle management
✓ Built-in watermark handling
✓ Clear semantics (window boundaries are explicit)
✓ Easier late data handling
✓ Simpler code and less state complexity
✗ Less flexible for custom timer logic
```

## Implementation Details

### 1. Application Structure: WindowedFraudReportingDemoApp

```
KafkaSource (transactions)
    ↓
[Parse JSON] → Transaction objects
    ↓
[keyBy(srcAccountId)] → Group by account
    ↓
[AdvancedStatefulFraudDetectionProcessor] → Detect fraud patterns
    ↓
[FraudAdvancedAlert] → Fraud alerts
    ↓
[keyBy(accountId)] → Group by account
    ↓
[5-Second Tumbling Window] → Group in windows
    ↓
[WindowedFraudReportProcessor] → Aggregate per window
    ↓
[FraudReport] → Periodic reports
    ↓
├→ KafkaSink (fraud-reports-windowed)
├→ JDBC Sink (PostgreSQL)
└→ Console print
```

### 2. Window Configuration

```java
.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
```

**Characteristics:**
- **Type:** Tumbling (non-overlapping)
- **Time Domain:** Processing time (local clock)
- **Duration:** 5 seconds
- **Trigger:** Window close (when processing time passes window end)

### 3. ProcessWindowFunction

```java
public class WindowedFraudReportProcessor 
    extends ProcessWindowFunction<FraudAdvancedAlert, FraudReport, String, TimeWindow>
```

**Parameters:**
- `FraudAdvancedAlert` - Input type (fraud alerts)
- `FraudReport` - Output type (aggregated reports)
- `String` - Key type (account ID)
- `TimeWindow` - Window type (time-based window)

**Key Method:**
```java
void process(String accountId,
             Context ctx,
             Iterable<FraudAdvancedAlert> alerts,  // All alerts in window
             Collector<FraudReport> out)
```

## Advantages of Windows Over Timers

### 1. **Clearer Semantics**
```
Timer: "Register a timer, wait for it to fire"
Window: "Group data into 5-second buckets, emit report per bucket"
```

### 2. **Automatic Watermark Integration**
```
// Window closes when:
// - Processing time advances past window end (processing time window)
// - Watermark advances past window end (event time window)
```

### 3. **Built-in Late Data Handling**
```java
.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
.allowedLateness(Time.seconds(10))  // Accept 10s late data
```

### 4. **Simpler State Management**
```
Timer approach:  3 separate state objects
                 (ListState + 2 × ValueState)
                 
Window approach: No state needed - 
                 window framework handles grouping
```

## Code Comparison

### Timer-Based (Complex)
```java
@Override
public void processElement(FraudAdvancedAlert alert, Context context, Collector<FraudReport> collector) {
    if (nextTimerState.value() == null) {
        long nextTimer = context.timerService().currentProcessingTime() + REPORT_INTERVAL_MS;
        context.timerService().registerProcessingTimeTimer(nextTimer);
        // Manual timer scheduling
    }
    alertsState.add(alert);  // Store in ListState
}

@Override
public void onTimer(long timestamp, OnTimerContext ctx, Collector<FraudReport> out) {
    // Manually aggregate stored alerts
    // Manually re-register next timer
}
```

### Window-Based (Simple)
```java
@Override
public void process(String accountId,
                    Context ctx,
                    Iterable<FraudAdvancedAlert> alerts,  // Framework provides grouped data
                    Collector<FraudReport> out) {
    // Just aggregate and emit
    int alertCount = 0;
    for (FraudAdvancedAlert alert : alerts) {
        alertCount++;
        // ... process
    }
    out.collect(report);
    // Window framework handles next firing
}
```

## When to Use Each Approach

### Use **Timers** When:
- ✓ You need irregular/custom firing patterns
- ✓ You need to trigger on external events
- ✓ You need to delay or cancel firings dynamically
- ✓ You need complex state transitions

### Use **Windows** When:
- ✓ You have regular time-based aggregations
- ✓ You want clean semantics
- ✓ You want watermark integration
- ✓ You want to handle late data easily
- ✓ You're building standard analytics pipelines

## Configuration Options

### Window Types

```java
// Tumbling (non-overlapping)
TumblingProcessingTimeWindows.of(Time.seconds(5))

// Sliding (overlapping)
SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))

// Session (gap-based)
EventTimeSessionWindows.withGap(Time.seconds(30))

// Global + custom trigger
GlobalWindows()
    .triggering(PurgingTrigger.purging(CountTrigger.of(100)))
```

### Allowed Lateness

```java
.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
.allowedLateness(Time.seconds(10))
.sideOutputLateData(outputTag)  // Capture late arrivals
```

## Performance Characteristics

| Aspect | Timer-Based | Window-Based |
|--------|-------------|--------------|
| State Size | Variable (accumulated) | None (stateless) |
| Memory Footprint | Higher | Lower |
| Complexity | Higher | Lower |
| Watermark Integration | Manual | Automatic |
| Late Data Handling | Manual | Built-in |
| Debuggability | Harder | Easier |

## Testing the Windowed Demo

### 1. Send transactions to Kafka
```bash
# Transaction 1
echo '{"transactionId":"tx-001","srcAccountId":"acc-123","destAccountId":"acc-456","amount":50.0,"currency":"EUR","eventTime":1702900000000}' | \
  docker exec -i broker kafka-console-producer --broker-list localhost:9092 --topic transactions

# Transaction 2 (trigger fraud)
echo '{"transactionId":"tx-002","srcAccountId":"acc-123","destAccountId":"acc-789","amount":75000.0,"currency":"EUR","eventTime":1702900001000}' | \
  docker exec -i broker kafka-console-producer --broker-list localhost:9092 --topic transactions
```

### 2. Monitor Flink UI
```
http://localhost:8084
```

### 3. Check PostgreSQL
```bash
docker exec -i postgresql psql -U app_user -d streaming_demo -c "SELECT * FROM fraud_reports;"
```

### 4. Monitor Kafka Topics
```bash
# Check fraud reports
docker exec -i broker kafka-console-consumer --bootstrap-server localhost:9092 --topic fraud-reports-windowed --from-beginning

# Check fraud alerts
docker exec -i broker kafka-console-consumer --bootstrap-server localhost:9092 --topic fraud-alerts-windowed --from-beginning
```

## Key Takeaways

1. **Windows are the declarative approach** - You describe WHAT windows you want, Flink handles WHEN they fire
2. **Timers are the imperative approach** - You control exactly WHEN firing happens
3. **Windows integrate naturally with watermarks** - Better for distributed time-based processing
4. **Start with windows, use timers only when needed** - Simpler is usually better in streaming

## Related Flink Concepts to Explore

- **Event Time vs Processing Time vs Ingestion Time** - Window semantics differ
- **Watermarks** - How to handle late data in distributed systems
- **Allowed Lateness** - Graceful late data handling
- **Side Outputs** - Capture late/dropped data separately
- **Custom Triggers** - Extend window firing logic
- **Evictor Functions** - Remove elements from windows


