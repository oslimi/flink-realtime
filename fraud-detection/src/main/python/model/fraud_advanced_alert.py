"""
FraudAdvancedAlert model class.
"""
import json
from dataclasses import dataclass
from typing import Dict, Any, Optional

from model.transaction import Transaction


@dataclass
class FraudAdvancedAlert:
    """
    Represents an advanced fraud alert with previous and current transactions.

    Attributes:
        alert_id: Unique identifier for the alert
        timestamp: Alert timestamp in milliseconds
        previous_transaction: The previous transaction (optional)
        current_transaction: The current transaction that triggered the alert
        comment: Description of why fraud was detected
        processing_duration: Time taken to process in milliseconds
    """
    alert_id: str
    timestamp: int
    previous_transaction: Optional[Transaction]
    current_transaction: Transaction
    comment: str
    processing_duration: int

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "AlertId": self.alert_id,
            "timestamp": self.timestamp,
            "previousTransaction": self.previous_transaction.to_dict() if self.previous_transaction else None,
            "currentTransaction": self.current_transaction.to_dict(),
            "comment": self.comment,
            "processingDuration": self.processing_duration
        }

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(json_str: str) -> 'FraudAdvancedAlert':
        """
        Create FraudAdvancedAlert from JSON string.

        Args:
            json_str: JSON string representation

        Returns:
            FraudAdvancedAlert object
        """
        data = json.loads(json_str)
        return FraudAdvancedAlert.from_dict(data)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'FraudAdvancedAlert':
        """
        Create FraudAdvancedAlert from dictionary.

        Args:
            data: Dictionary containing alert data

        Returns:
            FraudAdvancedAlert object
        """
        prev_tx_data = data.get("previousTransaction")
        return FraudAdvancedAlert(
            alert_id=data.get("AlertId"),
            timestamp=int(data.get("timestamp")),
            previous_transaction=Transaction.from_dict(prev_tx_data) if prev_tx_data else None,
            current_transaction=Transaction.from_dict(data.get("currentTransaction")),
            comment=data.get("comment"),
            processing_duration=int(data.get("processingDuration"))
        )

