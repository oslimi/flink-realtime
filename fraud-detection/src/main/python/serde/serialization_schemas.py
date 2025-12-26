"""
Serialization schemas for Kafka integration.
"""
import json
from pyflink.common.serialization import SerializationSchema, DeserializationSchema
from pyflink.common.typeinfo import Types

from model.transaction import Transaction
from model.fraud_naive_alert import FraudNaiveAlert
from model.fraud_advanced_alert import FraudAdvancedAlert
from model.fraud_report import FraudReport


class TransactionDeserializationSchema(DeserializationSchema):
    """Deserialize JSON string to Transaction object."""

    def deserialize(self, message: bytes) -> Transaction:
        """
        Deserialize bytes to Transaction.

        Args:
            message: JSON bytes

        Returns:
            Transaction object
        """
        json_str = message.decode('utf-8')
        return Transaction.from_json(json_str)

    def get_produced_type(self):
        """Return the type information."""
        return Types.PICKLED_BYTE_ARRAY()


class FraudNaiveAlertSerializationSchema(SerializationSchema):
    """Serialize FraudNaiveAlert to JSON string."""

    def serialize(self, value: FraudNaiveAlert) -> bytes:
        """
        Serialize FraudNaiveAlert to bytes.

        Args:
            value: FraudNaiveAlert object

        Returns:
            JSON bytes
        """
        json_str = value.to_json()
        return json_str.encode('utf-8')


class FraudAdvancedAlertSerializationSchema(SerializationSchema):
    """Serialize FraudAdvancedAlert to JSON string."""

    def serialize(self, value: FraudAdvancedAlert) -> bytes:
        """
        Serialize FraudAdvancedAlert to bytes.

        Args:
            value: FraudAdvancedAlert object

        Returns:
            JSON bytes
        """
        json_str = value.to_json()
        return json_str.encode('utf-8')


class FraudReportSerializationSchema(SerializationSchema):
    """Serialize FraudReport to JSON string."""

    def serialize(self, value: FraudReport) -> bytes:
        """
        Serialize FraudReport to bytes.

        Args:
            value: FraudReport object

        Returns:
            JSON bytes
        """
        json_str = value.to_json()
        return json_str.encode('utf-8')


# Simple string serialization helpers
class SimpleStringSchema:
    """Simple string serialization/deserialization."""

    @staticmethod
    def serialize(value: str) -> bytes:
        """Serialize string to bytes."""
        return value.encode('utf-8')

    @staticmethod
    def deserialize(message: bytes) -> str:
        """Deserialize bytes to string."""
        return message.decode('utf-8')

