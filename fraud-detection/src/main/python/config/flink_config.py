"""
Flink configuration utilities.
"""
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Configuration


# Kafka Configuration
KAFKA_BOOTSTRAP = "localhost:9092"
TRANSACTIONS_TOPIC = "transactions"
FRAUD_ALERTS_NAIVE_TOPIC = "fraud-alerts-naive"
FRAUD_ALERTS_STATEFUL_TOPIC = "fraud-alerts-stateful"
FRAUD_ALERTS_TOPIC = "fraud-alerts"
FRAUD_REPORTS_TOPIC = "fraud-reports"

# PostgreSQL Configuration
POSTGRES_URL = "jdbc:postgresql://localhost:5432/streaming_demo"
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

