"""
FraudNaiveAlert model class.
"""
import json
from dataclasses import dataclass
from typing import Dict, Any

from model.transaction import Transaction


@dataclass
class FraudNaiveAlert:
    """
    Represents a simple fraud alert for a single transaction.

    Attributes:
        alert_id: Unique identifier for the alert
        timestamp: Alert timestamp in milliseconds
        transaction: The transaction that triggered the alert
        comment: Description of why fraud was detected
    """
    alert_id: str
    timestamp: int
    transaction: Transaction
    comment: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "AlertId": self.alert_id,
            "timestamp": self.timestamp,
            "transaction": self.transaction.to_dict(),
            "comment": self.comment
        }

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(json_str: str) -> 'FraudNaiveAlert':
        """
        Create FraudNaiveAlert from JSON string.

        Args:
            json_str: JSON string representation

        Returns:
            FraudNaiveAlert object
        """
        data = json.loads(json_str)
        return FraudNaiveAlert.from_dict(data)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'FraudNaiveAlert':
        """
        Create FraudNaiveAlert from dictionary.

        Args:
            data: Dictionary containing alert data

        Returns:
            FraudNaiveAlert object
        """
        return FraudNaiveAlert(
            alert_id=data.get("AlertId"),
            timestamp=int(data.get("timestamp")),
            transaction=Transaction.from_dict(data.get("transaction")),
            comment=data.get("comment")
        )

