"""
FraudReport model class.
"""
import json
from dataclasses import dataclass
from typing import Dict, Any, List


@dataclass
class FraudReport:
    """
    Represents a periodic fraud report containing all alerts during a time window.

    Attributes:
        report_id: Unique identifier for the report
        report_timestamp: Report timestamp in milliseconds
        window_start: Window start timestamp in milliseconds
        window_end: Window end timestamp in milliseconds
        account_id: Account ID for this report
        total_alerts: Total number of alerts in this period
        total_fraud_amount: Total fraud amount in this period
        alert_ids: List of alert IDs included in this report
        summary: Human-readable summary of the report
    """
    report_id: str
    report_timestamp: int
    window_start: int
    window_end: int
    account_id: str
    total_alerts: int
    total_fraud_amount: float
    alert_ids: List[str]
    summary: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "reportId": self.report_id,
            "reportTimestamp": self.report_timestamp,
            "windowStart": self.window_start,
            "windowEnd": self.window_end,
            "accountId": self.account_id,
            "totalAlerts": self.total_alerts,
            "totalFraudAmount": self.total_fraud_amount,
            "alertIds": self.alert_ids,
            "summary": self.summary
        }

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(json_str: str) -> 'FraudReport':
        """
        Create FraudReport from JSON string.

        Args:
            json_str: JSON string representation

        Returns:
            FraudReport object
        """
        data = json.loads(json_str)
        return FraudReport.from_dict(data)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'FraudReport':
        """
        Create FraudReport from dictionary.

        Args:
            data: Dictionary containing report data

        Returns:
            FraudReport object
        """
        return FraudReport(
            report_id=data.get("reportId"),
            report_timestamp=int(data.get("reportTimestamp")),
            window_start=int(data.get("windowStart")),
            window_end=int(data.get("windowEnd")),
            account_id=data.get("accountId"),
            total_alerts=int(data.get("totalAlerts")),
            total_fraud_amount=float(data.get("totalFraudAmount")),
            alert_ids=data.get("alertIds", []),
            summary=data.get("summary")
        )

