"""
Naive Fraud Detection Processor - Simple threshold-based fraud detection.
"""
import logging
import uuid

from pyflink.datastream import KeyedProcessFunction

from model.transaction import Transaction
from model.fraud_naive_alert import FraudNaiveAlert


logger = logging.getLogger(__name__)


class NaiveFraudDetectionProcessor(KeyedProcessFunction):
    """
    Simple fraud detection processor.
    Flags a transaction as fraudulent if the amount exceeds a threshold.
    This is a STATELESS approach - no pattern detection, just a simple threshold check.
    """

    FRAUD_THRESHOLD = 10000.0

    def process_element(self, transaction: Transaction, context: 'KeyedProcessFunction.Context'):
        """
        Process each transaction and check if it exceeds the fraud threshold.

        Args:
            transaction: The transaction to process
            context: Flink process context
        """
        if transaction.amount > self.FRAUD_THRESHOLD:
            logger.warning(
                "🚨 FRAUD DETECTED! Transaction %s from account %s with amount %s exceeds threshold %s",
                transaction.transaction_id,
                transaction.src_account_id,
                transaction.amount,
                self.FRAUD_THRESHOLD
            )

            # Emit fraudulent transaction
            alert = FraudNaiveAlert(
                alert_id=str(uuid.uuid4()),
                timestamp=context.timer_service().current_processing_time(),
                transaction=transaction,
                comment=f"Amount exceeds threshold of {self.FRAUD_THRESHOLD}"
            )
            yield alert
        else:
            logger.info(
                "✅ Transaction %s is valid. Amount: %s",
                transaction.transaction_id,
                transaction.amount
            )

