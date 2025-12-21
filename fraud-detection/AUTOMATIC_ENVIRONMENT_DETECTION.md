# Automatic Environment Detection

## How It Works

The `TransactionDataGeneratorApp` now automatically detects whether it's running in:
- **IDE/Local environment** → uses `localhost:9092`
- **Docker/Flink cluster** → uses `broker:29092`

## Detection Logic

The `getKafkaBootstrapServers()` method checks in this order:

1. **Explicit Environment Variable** (highest priority)
   - If `KAFKA_BOOTSTRAP_SERVERS` is set → uses that value
   - Useful for custom configurations

2. **Docker Detection** (automatic)
   - Checks if `HOSTNAME` starts with `flink-`
   - Checks if `HOSTNAME` contains `taskmanager`
   - Checks if `IN_DOCKER=true`
   - If any of these → uses `broker:29092`

3. **Default** (local development)
   - If none of the above → uses `localhost:9092`

## Usage

### Running from IDE
```java
// No configuration needed!
// Just run the main() method
// Automatically uses: localhost:9092
```

### Running in Flink Cluster
```bash
./scripts/deploy-data-generator.sh -p demo
# Automatically uses: broker:29092
```

### Override with Environment Variable
```bash
# If you need a custom Kafka address
export KAFKA_BOOTSTRAP_SERVERS=my-kafka-server:9092
```

## Benefits

✅ **No code changes** between environments  
✅ **No configuration files** to manage  
✅ **Automatic detection** based on runtime environment  
✅ **Override capability** when needed  
✅ **Single codebase** for all environments  

## How Detection Works in Docker

When deployed to Flink cluster:
- Container hostname = `flink-taskmanager-<uuid>`
- Detection: `hostname.startsWith("flink-")` → TRUE
- Result: Uses `broker:29092`

When running from IDE:
- Hostname = your machine name (e.g., `wessim-laptop`)
- Detection: `hostname.startsWith("flink-")` → FALSE
- Result: Uses `localhost:9092`

## Verification

Check the logs to see which address was selected:

```
INFO  - Detected Docker environment, using internal broker address: broker:29092
```
or
```
INFO  - Detected local environment, using localhost: localhost:9092
```

## Apply This Pattern to Other Applications

Use the same pattern in your fraud detection apps:

```java
private static String getKafkaBootstrapServers() {
    String hostname = System.getenv("HOSTNAME");
    String explicitKafka = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
    
    if (explicitKafka != null && !explicitKafka.isEmpty()) {
        return explicitKafka;
    }
    
    if (hostname != null && hostname.startsWith("flink-")) {
        return "broker:29092";
    }
    
    return "localhost:9092";
}
```

This ensures all your Flink applications work seamlessly in both IDE and Docker deployment!

