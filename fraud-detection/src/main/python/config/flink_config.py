"""
Flink configuration utilities.
"""
import logging
import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Configuration


def get_kafka_bootstrap_servers():
    """
    Automatically detect Kafka bootstrap servers based on environment.
    - Docker deployment: uses broker:29092
    - IDE/local execution: uses localhost:9092

    This mirrors the Java EnvironmentDetector class.
    """
    logger = logging.getLogger(__name__)

    # Check for explicit environment variable first
    explicit_kafka = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if explicit_kafka:
        logger.info(f"Using explicit KAFKA_BOOTSTRAP_SERVERS: {explicit_kafka}")
        return explicit_kafka

    # Auto-detect: check if running in Docker
    hostname = os.environ.get("HOSTNAME", "")
    in_docker = os.environ.get("IN_DOCKER", "")

    if hostname.startswith("flink-") or "taskmanager" in hostname or in_docker == "true":
        logger.info("Detected Docker environment, using internal broker address: broker:29092")
        return "broker:29092"

    # Default to localhost for IDE/local execution
    logger.info("Detected local environment, using localhost: localhost:9092")
    return "localhost:9092"


def is_docker_environment():
    """Check if running in Docker environment."""
    hostname = os.environ.get("HOSTNAME", "")
    in_docker = os.environ.get("IN_DOCKER", "")
    return hostname.startswith("flink-") or "taskmanager" in hostname or in_docker == "true"


def is_local_environment():
    """Check if running in local/IDE environment."""
    return not is_docker_environment()


# Kafka Configuration - Auto-detected
KAFKA_BOOTSTRAP = get_kafka_bootstrap_servers()
TRANSACTIONS_TOPIC = "transactions"
FRAUD_ALERTS_NAIVE_TOPIC = "fraud-alerts-naive"
FRAUD_ALERTS_STATEFUL_TOPIC = "fraud-alerts-stateful"
FRAUD_ALERTS_TOPIC = "fraud-alerts"
FRAUD_REPORTS_TOPIC = "fraud-reports"

# PostgreSQL Configuration - Auto-detected
def get_postgres_host():
    """Get PostgreSQL host based on environment."""
    if is_docker_environment():
        return "postgresql"  # Docker container name
    return "localhost"

POSTGRES_HOST = get_postgres_host()
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:5432/streaming_demo"
POSTGRES_USER = "app_user"
POSTGRES_PASSWORD = "app_password"
POSTGRES_DRIVER = "org.postgresql.Driver"

# Flink Configuration
FLINK_WEB_UI_PORT = 8082
DEFAULT_PARALLELISM = 2


def create_stream_env(enable_web_ui=True, parallelism=DEFAULT_PARALLELISM):
    """
    Create and configure Flink StreamExecutionEnvironment.

    Args:
        enable_web_ui: Whether to enable web UI
        parallelism: Default parallelism for the job

    Returns:
        Configured StreamExecutionEnvironment
    """
    import os
    import glob

    # Create configuration
    config = Configuration()

    if enable_web_ui:
        # Use port range to allow fallback if 8082 is busy
        config.set_string("rest.port", "8082-8092")
        config.set_string("rest.bind-address", "localhost")
        config.set_string("rest.bind-port", "8082-8092")

    # Create environment
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(parallelism)

    # Add JAR dependencies from lib directory
    lib_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'lib')
    if os.path.exists(lib_dir):
        jar_files = glob.glob(os.path.join(lib_dir, '*.jar'))
        if jar_files:
            jar_urls = [f"file://{os.path.abspath(jar)}" for jar in jar_files]
            env.add_jars(*jar_urls)
            logging.info(f"Added {len(jar_files)} JAR dependencies from {lib_dir}")

    return env


def setup_logging(level=logging.INFO):
    """
    Setup logging configuration.

    Args:
        level: Logging level (default: INFO)
    """
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )


def get_kafka_properties(consumer_group):
    """
    Get Kafka consumer properties.

    Args:
        consumer_group: Kafka consumer group ID

    Returns:
        Dictionary of Kafka properties
    """
    return {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': consumer_group
    }

