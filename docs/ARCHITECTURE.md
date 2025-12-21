# Flink Fraud Detection Demo

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Data Generator │────▶│  Kafka (broker)  │────▶│  Fraud Detector │
│  (Flink Job)    │     │  transactions    │     │  (Flink Job)    │
└─────────────────┘     └──────────────────┘     └────────┬────────┘
                                                          │
                        ┌──────────────────┐              │
                        │  Kafka           │◀─────────────┘
                        │  fraud-alerts    │
                        └──────────────────┘
```

## Infrastructure

| Service | Port | Purpose |
|---------|------|---------|
| Kafka | 9092 | Message broker |
| Flink JobManager | 8081 | Job management & Web UI |
| Flink TaskManager | - | Task execution |
| Conduktor UI | 8080 | Kafka visualization |
| PostgreSQL | 5432 | Persistent storage |

## Project Structure

```
fraud-detection/
├── src/main/java/com/wslimi/demo/fraud/
│   ├── TransactionDataGeneratorApp.java    # Generates transactions
│   ├── NaiveFraudDetectionDemo.java        # Demo 1: Simple threshold
│   ├── StatefulFraudDetectionDemoApp.java  # Demo 2: Stateful detection
│   ├── TimerReportingDemoApp.java          # Demo 3: Timers & aggregation
│   ├── config/
│   ├── datagen/
│   ├── model/
│   ├── processor/
│   ├── serde/
│   ├── source/
│   └── util/
└── src/main/python/                        # PyFlink equivalent
```

## Demo Scenarios

### Demo 1: Naive Fraud Detection
Simple threshold-based detection: `amount > 10,000` triggers alert.

**Concepts**: Kafka Source/Sink, Map transformation, Filtering

### Demo 2: Stateful Fraud Detection
Pattern detection using ValueState: small transaction (`<100`) followed by large transaction (`>50,000`) from same account.

**Concepts**: KeyedProcessFunction, ValueState, State management

### Demo 3: Timer-based Reporting
Periodic aggregation using processing time timers.

**Concepts**: Timers, State accumulation, onTimer callback

## Fraud Detection Rule

```
IF previous_tx.amount < 100 AND current_tx.amount > 50,000
   AND previous_tx.account == current_tx.account
THEN raise_fraud_alert()
```

This pattern detects "card testing" attacks where fraudsters verify a card with small transactions before draining the account.
