# Flink Streaming Demo

Real-time fraud detection demo using Apache Flink.

## Quick Start

```bash
# Start infrastructure
cd infrastructure && sudo docker compose up -d

# Build
cd fraud-detection && mvn clean package -DskipTests

# Deploy data generator
./fraud-detection/app/java/deploy-data-generator.sh -p fraud -d

# Deploy fraud detection
./fraud-detection/app/java/deploy-stateful-fraud-detection.sh -d
```

## URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Flink UI | http://localhost:8081 | - |
| Conduktor | http://localhost:8080 | admin@conduktor.io / adminP4ss! |

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Execution Guide](docs/EXECUTION.md)
- [Java vs PyFlink](docs/JAVA_VS_PYFLINK.md)

## Project Structure

```
├── fraud-detection/     # Flink jobs (Java + Python)
│   └── app/
│       ├── java/        # Java deployment scripts
│       └── python/      # Python deployment scripts
├── infrastructure/      # Docker Compose
├── scripts/            # Infrastructure scripts (start/stop)
└── docs/               # Documentation
```

## Demos

1. **Naive Detection**: Threshold-based fraud detection
2. **Stateful Detection**: Pattern detection with ValueState
3. **Timer Reporting**: Periodic aggregation with timers

## Requirements

- Docker & Docker Compose
- Java 17
- Maven 3.8+
