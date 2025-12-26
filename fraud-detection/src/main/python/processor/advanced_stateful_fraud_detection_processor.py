"""
Advanced Stateful Fraud Detection Processor - Pattern-based fraud detection with state.
"""
import logging
import uuid

from pyflink.datastream import KeyedProcessFunction, RuntimeContext
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueStateDescriptor

from model.transaction import Transaction
from model.fraud_advanced_alert import FraudAdvancedAlert


logger = logging.getLogger(__name__)


class AdvancedStatefulFraudDetectionProcessor(KeyedProcessFunction):
    """
    Stateful fraud detection processor.
    Detects fraud pattern: small transaction (< 100) followed by large transaction (> 50000).
    """

    SMALL_AMOUNT_THRESHOLD = 100.0
    LARGE_AMOUNT_THRESHOLD = 50000.0

    def __init__(self):
        # State to track the previous transaction
        self.previous_transaction_state = None

    def open(self, runtime_context: RuntimeContext):
        """
        Initialize state when the processor starts.

        Args:
            runtime_context: Flink runtime context
        """
        self.previous_transaction_state = runtime_context.get_state(
            ValueStateDescriptor("previous-transaction", Types.PICKLED_BYTE_ARRAY())
        )
    def process_element(self, transaction: Transaction, context: 'KeyedProcessFunction.Context'):
        """
        Process each transaction and detect fraud patterns using state.

        Args:
            transaction: The transaction to process
            context: Flink process context
        """
        previous_transaction = self.previous_transaction_state.value()

        # Check for fraud pattern: small transaction followed by large transaction
        if (previous_transaction is not None
            and previous_transaction.amount < self.SMALL_AMOUNT_THRESHOLD
            and transaction.amount > self.LARGE_AMOUNT_THRESHOLD):

            logger.info(
                "🚨 FRAUD PATTERN DETECTED! Account %s had a small transaction (%s) followed by large transaction (%s)",
                transaction.src_account_id,
                previous_transaction.amount,
                transaction.amount
            )

            alert = FraudAdvancedAlert(
                alert_id=str(uuid.uuid4()),
                timestamp=context.timer_service().current_processing_time(),
                previous_transaction=previous_transaction,
                current_transaction=transaction,
                comment=f"[PYTHON] Fraud pattern: small transaction (<{self.SMALL_AMOUNT_THRESHOLD:.2f}) "
                        f"followed by large transaction (>{self.LARGE_AMOUNT_THRESHOLD:.2f})",
                processing_duration=context.timer_service().current_processing_time() - transaction.event_time
            )

            # Emit alert
            yield alert

            # Reset state after detecting fraud
            self.previous_transaction_state.clear()
        else:
            # Update state with current transaction
            if transaction.amount < self.SMALL_AMOUNT_THRESHOLD:
                logger.debug(
                    "📝 Small transaction detected for account %s: amount %s",
                    transaction.src_account_id,
                    transaction.amount
                )
            elif transaction.amount <= self.LARGE_AMOUNT_THRESHOLD:
                logger.info(
                    "✅ Normal transaction for account %s: amount %s",
                    transaction.src_account_id,
                    transaction.amount
                )

            self.previous_transaction_state.update(transaction)

