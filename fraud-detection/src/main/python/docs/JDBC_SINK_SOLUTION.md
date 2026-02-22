# PyFlink JDBC Sink - Complete Solution

## The Challenge

Writing to PostgreSQL from PyFlink is **fundamentally different** from Java Flink due to serialization limitations in the Python-Java bridge (Py4J).

## All Errors Encountered (and why)

### 1. ❌ `NoSuchMethodException: createRowJdbcStatementBuilder`
**Attempted:** Using DataStream `JdbcSink.sink()` like Java
```python
postgres_sink = JdbcSink.sink(sql, types, connection_opts, exec_opts)
fraud_report_rows.add_sink(postgres_sink)
```
**Why it failed:** PyFlink's `JdbcSink` API is incomplete - it cannot serialize Python lambdas to Java statement builders.

### 2. ❌ `ValidationException: Too many fields referenced from an atomic type`
**Attempted:** Converting DataStream to Table with explicit schema
```python
report_table = table_env.from_data_stream(
    fraud_report_rows,
    Schema.new_builder().column('report_id', DataTypes.STRING())...
)
```
**Why it failed:** PyFlink cannot properly infer field types when converting Python objects through `map()`.

### 3. ❌ `ValidationException: Different number of columns. Query schema: [f0: RAW('[B', '...')]`
**Attempted:** Auto-inferring schema from named Row fields
```python
fraud_report_rows = fraud_reports.map(lambda r: Row(report_id=r.report_id, ...))
report_table = table_env.from_data_stream(fraud_report_rows)
```
**Why it failed:** PyFlink serializes Python Row objects as raw bytes (`[B`) when passed through map - the field names are lost in translation.

### 4. ❌ `ClassNotFoundException: org.apache.flink.connector.kafka.source.KafkaSource`
**Attempted:** Running Table API SQL without proper JAR configuration
```python
table_env.execute_sql("INSERT INTO fraud_reports_sink SELECT * FROM fraud_reports_kafka")
```
**Why it failed:** The Table API SQL job runs in a separate context and doesn't have access to the Kafka connector JAR.

## ✅ The Working Solution

### Architecture: DataStream → Kafka → Table API → PostgreSQL

```
┌─────────────┐    ┌───────────────┐    ┌─────────────┐    ┌────────────┐
│ Transactions│ -> │Fraud Detection│ -> │   Reports   │ -> │   Kafka    │
│  (Kafka)    │    │  (DataStream) │    │ (DataStream)│    │  (topic)   │
└─────────────┘    └───────────────┘    └─────────────┘    └────────────┘
                                                                    │
                                                                    v
                                                            ┌────────────┐
                                                            │ Table API  │
                                                            │ SQL SELECT │
                                                            └────────────┘
                                                                    │
                                                                    v
                                                            ┌────────────┐
                                                            │ PostgreSQL │
                                                            └────────────┘
```

### Implementation

#### Step 1: Configure JARs for both DataStream and Table API
```python
jdbc_jar = os.path.join(os.path.dirname(__file__), '..', 'lib', 'flink-connector-jdbc-3.3.0-1.20.jar')
postgres_jar = os.path.join(os.path.dirname(__file__), '..', 'lib', 'postgresql-42.7.3.jar')
kafka_jar = os.path.join(os.path.dirname(__file__), '..', 'lib', 'flink-sql-connector-kafka-3.3.0-1.20.jar')

# For Table API (SQL jobs)
jar_urls = f"file://{os.path.abspath(jdbc_jar)};file://{os.path.abspath(postgres_jar)};file://{os.path.abspath(kafka_jar)}"
table_env.get_config().get_configuration().set_string("pipeline.jars", jar_urls)

# For DataStream API
env.get_config().set_files([os.path.abspath(jdbc_jar), os.path.abspath(postgres_jar), os.path.abspath(kafka_jar)])
```

#### Step 2: Create JDBC Sink Table (PostgreSQL)
```python
table_env.execute_sql(f"""
    CREATE TABLE fraud_reports_sink (
        report_id STRING,
        report_timestamp BIGINT,
        window_start BIGINT,
        window_end BIGINT,
        account_id STRING,
        total_alerts INT,
        total_fraud_amount DOUBLE,
        summary STRING,
        PRIMARY KEY (report_id) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = '{POSTGRES_URL}',
        'table-name' = 'fraud_reports',
        'username' = '{POSTGRES_USER}',
        'password' = '{POSTGRES_PASSWORD}',
        'driver' = 'org.postgresql.Driver',
        'sink.buffer-flush.max-rows' = '1000',
        'sink.buffer-flush.interval' = '200ms',
        'sink.max-retries' = '5'
    )
""")
```

#### Step 3: Create Kafka Source Table (intermediate storage)
```python
table_env.execute_sql(f"""
    CREATE TABLE fraud_reports_kafka (
        report_id STRING,
        report_timestamp BIGINT,
        ...
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'fraud-reports',
        'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
    )
""")
```

#### Step 4: Write DataStream reports to Kafka as JSON
```python
# DataStream can write to Kafka easily
reports_kafka_sink = KafkaSink.builder() \
    .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic("fraud-reports")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
    ) \
    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
    .build()

fraud_reports_json = fraud_reports.map(lambda r: r.to_json())
fraud_reports_json.sink_to(reports_kafka_sink)
```

#### Step 5: Use Table API SQL to copy from Kafka to PostgreSQL
```python
# This works because Table API handles the deserialization from JSON to Row
table_env.execute_sql("""
    INSERT INTO fraud_reports_sink
    SELECT 
        report_id,
        report_timestamp,
        window_start,
        window_end,
        account_id,
        total_alerts,
        total_fraud_amount,
        summary
    FROM fraud_reports_kafka
""")
```

#### Step 6: Execute DataStream job
```python
env.execute("JDBC Sink Demo (PyFlink)")
```

## Why This Works

1. **DataStream → Kafka**: Python objects serialize to JSON easily ✅
2. **Kafka → Table API**: Flink's JSON deserializer creates proper Row objects ✅
3. **Table API → JDBC**: Pure Java execution, no Python serialization issues ✅

## Performance Implications

| Step | Overhead | Reason |
|------|----------|--------|
| Python → JSON | Low | Simple string serialization |
| Write to Kafka | Medium | Network I/O + Kafka latency |
| Kafka → Row | Low | Native Flink JSON format |
| Row → JDBC | Low | Native Java JDBC |
| **Total** | **~5-10ms** | Extra hop through Kafka |

### vs Java Direct JDBC

| Approach | Latency | Complexity |
|----------|---------|------------|
| **Java**: DataStream → JDBC | ~1-2ms | Simple ✅ |
| **PyFlink**: DataStream → Kafka → Table API → JDBC | ~5-10ms | Complex ⚠️ |

## Alternative Solutions

### 1. Use Java Flink (Recommended for Production)
- Direct JDBC sink with `JdbcSink.sink()`
- No serialization overhead
- Better performance
- See: `JdbcSinkDemoApp.java`

### 2. Use Kafka Connect JDBC Sink
- Kafka Connect reads from `fraud-reports` topic
- Writes directly to PostgreSQL
- No Flink Table API needed
- Requires separate Kafka Connect deployment

### 3. Use a separate consumer
- Python/Java application reads from Kafka
- Uses native PostgreSQL client (psycopg2, JDBC)
- Simple and reliable
- Not part of Flink pipeline

## Conclusion

**PyFlink JDBC sink requires workarounds** due to Py4J serialization limitations. The working solution uses an intermediate Kafka topic as a "bridge" between DataStream and Table API.

For production fraud detection systems requiring **low latency and high throughput**, this demonstrates why **Java Flink is preferred over PyFlink** for database operations.

### When to use PyFlink JDBC approach:
- ✅ Prototyping and demos
- ✅ When extra 5-10ms latency is acceptable
- ✅ When you need Python-only pipeline

### When to use Java Flink:
- ✅ Production systems
- ✅ Low-latency requirements (<10ms)
- ✅ Direct database operations
- ✅ Complex stateful processing

