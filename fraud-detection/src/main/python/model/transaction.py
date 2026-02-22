"""
Transaction model class.
"""
import json
from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class Transaction:
    """
    Represents a financial transaction.

    Attributes:
        transaction_id: Unique identifier for the transaction
        src_account_id: Source account ID
        dest_account_id: Destination account ID
        amount: Transaction amount
        currency: Transaction currency (e.g., USD, EUR)
        event_time: Event timestamp in milliseconds
    """
    transaction_id: str
    src_account_id: str
    dest_account_id: str
    amount: float
    currency: str
    event_time: int

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "transactionId": self.transaction_id,
            "srcAccountId": self.src_account_id,
            "destAccountId": self.dest_account_id,
            "amount": self.amount,
            "currency": self.currency,
            "eventTime": self.event_time
        }

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(json_str: str) -> 'Transaction':
        """
        Create Transaction from JSON string.

        Args:
            json_str: JSON string representation

        Returns:
            Transaction object
        """
        data = json.loads(json_str)
        return Transaction.from_dict(data)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'Transaction':
        """
        Create Transaction from dictionary.

        Args:
            data: Dictionary containing transaction data

        Returns:
            Transaction object
        """
        return Transaction(
            transaction_id=data.get("transactionId"),
            src_account_id=data.get("srcAccountId"),
            dest_account_id=data.get("destAccountId"),
            amount=float(data.get("amount")),
            currency=data.get("currency"),
            event_time=int(data.get("eventTime"))
        )

    @staticmethod
    def multiplyByfactor(self, factor: float) -> 'Transaction':
        """
        Multiply the transaction amount by a factor.

        Args:
            factor: The factor to multiply the amount by

        Returns:
            A new Transaction object with the multiplied amount
        """
        return Transaction(
            transaction_id=self.transaction_id,
            src_account_id=self.src_account_id,
            dest_account_id=self.dest_account_id,
            amount=self.amount * factor,
            currency=self.currency,
            event_time=self.event_time
        )

