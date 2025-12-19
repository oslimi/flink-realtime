# Fraud Detection Module - Directory Structure

This document shows the complete structure of the fraud-detection module with both Java and Python implementations.

## Structure Overview

```
fraud-detection/
├── pom.xml                                    # Maven build configuration
├── src/
│   ├── main/
│   │   ├── java/                             # Java implementation
│   │   │   └── com/
│   │   │       └── wslimi/
│   │   │           └── demo/
│   │   │               └── fraud/
│   │   │                   ├── KafkaSourceDemo.java
│   │   │                   ├── NaiveFraudDetectionDemo.java
│   │   │                   ├── StatefulFraudDetectionDemo.java
│   │   │                   ├── TimerReportingDemo.java
│   │   │                   ├── JdbcSinkDemo.java
│   │   │                   ├── FlinkFraudDetectionFullApplication.java
│   │   │                   ├── model/
│   │   │                   │   ├── Transaction.java
│   │   │                   │   ├── FraudNaiveAlert.java
│   │   │                   │   ├── FraudAdvancedAlert.java
│   │   │                   │   └── FraudReport.java
│   │   │                   ├── processor/
│   │   │                   │   ├── NaiveFraudDetectionProcessor.java
│   │   │                   │   ├── AdvancedStatefulFraudDetectionProcessor.java
│   │   │                   │   └── FraudReportAggregatorProcessor.java
│   │   │                   └── serde/
│   │   │                       ├── FraudNaiveAlertSerializationSchema.java
│   │   │                       ├── FraudAdvancedAlertSerializationSchema.java
│   │   │                       └── FraudReportSerializationSchema.java
│   │   │
│   │   ├── python/                           # Python implementation (PyFlink)
│   │   │   ├── README.md
│   │   │   ├── requirements.txt
│   │   │   ├── model/
│   │   │   │   ├── __init__.py
│   │   │   │   ├── transaction.py
│   │   │   │   ├── fraud_naive_alert.py
│   │   │   │   ├── fraud_advanced_alert.py
│   │   │   │   └── fraud_report.py
│   │   │   ├── processor/
│   │   │   │   ├── __init__.py
│   │   │   │   ├── naive_fraud_detection_processor.py
│   │   │   │   ├── advanced_stateful_fraud_detection_processor.py
│   │   │   │   └── fraud_report_aggregator_processor.py
│   │   │   ├── serde/
│   │   │   │   ├── __init__.py
│   │   │   │   └── serialization_schemas.py
│   │   │   ├── scripts/
│   │   │   │   ├── kafka_source_demo.py
│   │   │   │   ├── naive_fraud_detection_demo.py
│   │   │   │   ├── stateful_fraud_detection_demo.py
│   │   │   │   ├── timer_reporting_demo.py
│   │   │   │   └── jdbc_sink_demo.py
│   │   │   └── config/
│   │   │       └── flink_config.py
│   │   │
│   │   └── resources/
│   │       └── log4j2.xml
│   │
│   └── test/
│       └── java/
│           └── com/
│               └── wslimi/
│                   └── demo/
│                       └── fraud/
└── target/                                    # Maven build output
    └── fraud-detection-1.0-SNAPSHOT.jar
```

## Key Design Principles

### 1. **Maven Standard Layout**
- Follows Maven's standard directory structure
- `src/main/java` for Java sources
- `src/main/python` for Python sources (polyglot project)
- `src/main/resources` for configuration files
- `src/test/java` for Java tests

### 2. **Parallel Implementations**
- Java and Python implementations are **functionally equivalent**
- Same package/module structure
- Same class/file names (adapted to language conventions)
- Share the same infrastructure (Kafka, Flink, PostgreSQL)

### 3. **Language Conventions**
- **Java**: CamelCase classes, package structure
- **Python**: snake_case files, flat module structure

### 4. **Demo Progression**
Both implementations follow the same learning path:
1. **KafkaSourceDemo** - Reading from Kafka
2. **NaiveFraudDetectionDemo** - Stateless processing
3. **StatefulFraudDetectionDemo** - State management
4. **TimerReportingDemo** - Processing time timers
5. **JdbcSinkDemo** - Database integration

## Benefits of This Structure

✅ **Clear separation** of Java and Python code  
✅ **Maven compatibility** - standard layout for Java builds  
✅ **Independent Python environment** - separate requirements.txt  
✅ **Easy comparison** - equivalent implementations side by side  
✅ **Shared infrastructure** - both use same Docker services  
✅ **Educational value** - learn Flink in both languages  

## Usage

### Java (Maven)
```bash
cd fraud-detection
mvn clean package
java -cp target/fraud-detection-1.0-SNAPSHOT.jar com.wslimi.demo.fraud.NaiveFraudDetectionDemo
```

### Python (PyFlink)
```bash
cd fraud-detection/src/main/python
pip install -r requirements.txt
python scripts/naive_fraud_detection_demo.py
```

## Notes

- Both implementations can run **simultaneously** (different Kafka consumer groups)
- Share same Kafka topics and database tables
- Useful for **performance comparison** and **API comparison**
- Great for teams transitioning between Java and Python

