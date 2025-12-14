# Streaming Demo Architecture

A local development environment for streaming demos with Kafka, Flink, and PostgreSQL.

## Architecture

- **Kafka Broker**: Message streaming platform
- **Conduktor UI**: Kafka management and monitoring UI
- **Apache Flink**: Stream processing framework
- **PostgreSQL**: Database for data persistence

## Project Structure

```
streaming/
├── docker-compose.yml          # Main Docker Compose file
├── .env                        # Environment variables
├── infrastructure/             # Infrastructure configurations
│   ├── kafka/                  # Kafka broker configs
│   ├── flink/                  # Flink configs
│   ├── postgres/               # PostgreSQL init scripts
│   └── conduktor/              # Conduktor UI configs
├── applications/               # Application code
│   ├── flink-jobs/             # Flink streaming jobs
│   ├── kafka-producers/        # Kafka producer applications
│   └── kafka-consumers/        # Kafka consumer applications
├── scripts/                    # Utility scripts
├── data/                       # Sample data
│   ├── input/                  # Input data files
│   └── output/                 # Output data files
└── docs/                       # Documentation
```

## Getting Started

1. Start the infrastructure:
   ```bash
   docker-compose up -d
   ```

2. Access the services:
   - Conduktor UI: http://localhost:8080
   - Flink Dashboard: http://localhost:8081
   - PostgreSQL: localhost:5432

## Development

TODO: Add development instructions

## License

MIT

