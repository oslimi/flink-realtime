# Java vs PyFlink

## Performance

See [python-java-comparison.html](../fraud-detection/src/main/python/out/python-java-comparison.html) for benchmarks.

Java outperforms PyFlink by ~3-5x due to Py4J bridge overhead.

## Architecture

```
Java:  JVM (native execution)
PyFlink:  JVM ◄──Py4J──► Python Process (serialization overhead)
```

## Serialization Issues

| Issue | Cause | Fix |
|-------|-------|-----|
| Float precision loss | Python float → Java Double | Use Decimal or int cents |
| Large number overflow | Python int unlimited, Java Long limited | Validate ranges |
| DateTime mismatch | datetime vs Instant | Use epoch millis |

### Example

```python
amount = 99999999999.99  # Python: OK
# After Py4J: 1.0E11 (precision lost)

# Fix: use integer cents
amount_cents = 9999999999999
```

## Comparison

| Aspect | Java | PyFlink |
|--------|------|---------|
| Packaging | JAR | ZIP + deps |
| Cluster | Native | Needs Python installed |
| State backends | All | Limited |
| Performance | Optimal | 3-5x slower UDFs |

## When to Use

**Java**: Production, high throughput, complex state

**PyFlink**: Prototyping, ML integration, SQL pipelines

## Code

```java
// Java - native
transactions.keyBy(Transaction::srcAccountId).process(new FraudDetector());
```

```python
# PyFlink - crosses Py4J on every record
transactions.key_by(lambda t: t.src_account_id).process(FraudDetector())
```
