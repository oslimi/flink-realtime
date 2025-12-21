#!/usr/bin/env python3
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
)

from config.flink_config import (
    KAFKA_BOOTSTRAP, TRANSACTIONS_TOPIC, FRAUD_ALERTS_STATEFUL_TOPIC,
    setup_logging, is_local_environment
)
from model.transaction import Transaction
from processor.advanced_stateful_fraud_detection_processor import AdvancedStatefulFraudDetectionProcessor

logger = logging.getLogger(__name__)


def main():
    setup_logging()
    logger.info(f"Starting Stateful Fraud Detection | Kafka: {KAFKA_BOOTSTRAP}")

    env = StreamExecutionEnvironment.get_execution_environment()

    if is_local_environment():
        import glob
        lib_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'lib')
        if os.path.exists(lib_dir):
            jars = glob.glob(os.path.join(lib_dir, '*.jar'))
            if jars:
                env.add_jars(*[f"file://{os.path.abspath(j)}" for j in jars])

    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
        .set_topics(TRANSACTIONS_TOPIC) \
        .set_group_id("python-stateful-fraud-detection") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(FRAUD_ALERTS_STATEFUL_TOPIC)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ).build()

    transactions = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source") \
        .map(lambda json_str: Transaction.from_json(json_str))

    alerts = transactions \
        .key_by(lambda t: t.src_account_id) \
        .process(AdvancedStatefulFraudDetectionProcessor())

    alerts.map(lambda a: a.to_json(), output_type=Types.STRING()).sink_to(sink)
    alerts.print()

    env.execute("[PYTHON] Stateful Fraud Detection")


if __name__ == "__main__":
    main()
