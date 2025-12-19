"""
Fraud Report Aggregator Processor - Timer-based periodic reporting.
"""
import logging
import uuid
from typing import List
from pyflink.datastream import KeyedProcessFunction, RuntimeContext
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor

from model.fraud_advanced_alert import FraudAdvancedAlert
from model.fraud_report import FraudReport


logger = logging.getLogger(__name__)


class FraudReportAggregatorProcessor(KeyedProcessFunction):
    """
    Fraud Report Aggregator Processor.
    Receives fraud alerts as input and generates periodic reports every minute.
    Only aggregates alerts - does not detect fraud.
    """

    REPORT_INTERVAL_MS = 60_000  # 1 minute

    def __init__(self):
        # State to collect alerts during the period
        self.alerts_state = None
        # State for timer tracking
        self.window_start_state = None
        self.next_timer_state = None

    def open(self, runtime_context: RuntimeContext):
        """
        Initialize state when the processor starts.

        Args:
            runtime_context: Flink runtime context
        """
        self.alerts_state = runtime_context.get_list_state(
            ListStateDescriptor("alerts-list", Types.PICKLED_BYTE_ARRAY())
        )

        self.window_start_state = runtime_context.get_state(
            ValueStateDescriptor("window-start", Types.LONG())
        )

        self.next_timer_state = runtime_context.get_state(
            ValueStateDescriptor("next-timer", Types.LONG())
        )

    def process_element(self, alert: FraudAdvancedAlert, context: 'KeyedProcessFunction.Context'):
        """
        Process each fraud alert and collect for periodic reporting.

        Args:
            alert: The fraud alert to process
            context: Flink process context
        """
        # Initialize timer if not set
        next_timer = self.next_timer_state.value()
        if next_timer is None:
            current_time = context.timer_service().current_processing_time()
            next_timer = current_time + self.REPORT_INTERVAL_MS
            context.timer_service().register_processing_time_timer(next_timer)
            self.next_timer_state.update(next_timer)
            self.window_start_state.update(current_time)
            logger.info(
                "📅 Timer initialized for account %s, first report at %s",
                context.get_current_key(),
                next_timer
            )

        # Store alert for the periodic report
        self.alerts_state.add(alert)
        logger.debug(
            "📥 Alert %s added to aggregation for account %s",
            alert.alert_id,
            context.get_current_key()
        )

    def on_timer(self, timestamp: int, context: 'KeyedProcessFunction.OnTimerContext'):
        """
        Called when the timer fires - generate and emit the report.

        Args:
            timestamp: The timestamp when the timer fired
            context: Flink timer context
        """
        # Get window start (or use timestamp - interval as default)
        window_start = self.window_start_state.value()
        if window_start is None:
            window_start = timestamp - self.REPORT_INTERVAL_MS

        # Collect all alerts from the period
        period_alerts = list(self.alerts_state.get())
        alert_ids = [alert.alert_id for alert in period_alerts]
        total_fraud_amount = sum(alert.current_transaction.amount for alert in period_alerts)
        alert_count = len(period_alerts)

        # Only emit report if there were alerts
        if alert_count > 0:
            report = FraudReport(
                report_id=str(uuid.uuid4()),
                report_timestamp=timestamp,
                window_start=window_start,
                window_end=timestamp,
                account_id=context.get_current_key(),
                total_alerts=alert_count,
                total_fraud_amount=total_fraud_amount,
                alert_ids=alert_ids,
                summary=f"Account {context.get_current_key()}: {alert_count} fraud alerts "
                        f"in the last minute, total fraud amount: {total_fraud_amount:.2f}"
            )

            logger.info(
                "📊 FRAUD REPORT for account %s: %s alerts, total fraud amount: %s, alert IDs: %s",
                context.get_current_key(),
                alert_count,
                total_fraud_amount,
                alert_ids
            )

            yield report
        else:
            logger.debug(
                "📊 No alerts for account %s in the last minute, skipping report",
                context.get_current_key()
            )

        # Clear alerts for the next period
        self.alerts_state.clear()

        # Update window start and schedule next timer
        self.window_start_state.update(timestamp)
        next_timer = timestamp + self.REPORT_INTERVAL_MS
        context.timer_service().register_processing_time_timer(next_timer)
        self.next_timer_state.update(next_timer)

