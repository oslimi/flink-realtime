"""
Processor classes for fraud detection.
"""

from processor.naive_fraud_detection_processor import NaiveFraudDetectionProcessor
from processor.advanced_stateful_fraud_detection_processor import AdvancedStatefulFraudDetectionProcessor
from processor.fraud_report_aggregator_processor import FraudReportAggregatorProcessor

__all__ = [
    'NaiveFraudDetectionProcessor',
    'AdvancedStatefulFraudDetectionProcessor',
    'FraudReportAggregatorProcessor'
]

