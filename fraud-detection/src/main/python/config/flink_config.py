import logging
import os
import glob
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Configuration

logger = logging.getLogger(__name__)

def get_kafka_bootstrap_servers():
    explicit = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if explicit:
        return explicit
    if is_docker_environment():
        return "broker:29092"
    return "localhost:9092"

def is_docker_environment():
    hostname = os.environ.get("HOSTNAME", "")
    in_docker = os.environ.get("IN_DOCKER", "")
    return hostname.startswith("flink-") or "taskmanager" in hostname or in_docker == "true"

def is_local_environment():
    return not is_docker_environment()

def get_postgres_host():
    return "postgresql" if is_docker_environment() else "localhost"

# Configuration
KAFKA_BOOTSTRAP = get_kafka_bootstrap_servers()
TRANSACTIONS_TOPIC = "transactions"
FRAUD_ALERTS_NAIVE_TOPIC = "fraud-alerts-naive"
FRAUD_ALERTS_STATEFUL_TOPIC = "fraud-alerts-stateful"
FRAUD_ALERTS_TOPIC = "fraud-alerts"
FRAUD_REPORTS_TOPIC = "fraud-reports"

POSTGRES_HOST = get_postgres_host()
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:5432/streaming_demo"
POSTGRES_USER = "app_user"
POSTGRES_PASSWORD = "app_password"

DEFAULT_PARALLELISM = 2

def create_stream_env(enable_web_ui=True, parallelism=DEFAULT_PARALLELISM):
    config = Configuration()
    # Don't set rest.port for cluster deployment
    # The Flink cluster already has UI on port 8081

    # Add Python files directory to system path for workers
    python_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config.set_string("python.files", python_dir)

    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(parallelism)

    # Add JAR files from lib directory
    lib_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'lib')
    if os.path.exists(lib_dir):
        jars = glob.glob(os.path.join(lib_dir, '*.jar'))
        if jars:
            env.add_jars(*[f"file://{os.path.abspath(j)}" for j in jars])

    return env

def setup_logging(level=logging.INFO):
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )

def get_kafka_properties(consumer_group):
    return {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': consumer_group
    }

