"""
zerodb-prefect -- ZeroDB event sensor and result storage for Prefect.

    from zerodb_prefect import ZeroDBSensor, ZeroDBBlock

    sensor = ZeroDBSensor(event_type='zerodb.vector.stored')

    @sensor.on_event
    def process_vector(event):
        return {'processed': event.data['vector_id']}
"""

from zerodb_prefect.sensor import ZeroDBSensor, ZeroDBEvent  # noqa: F401
from zerodb_prefect.block import ZeroDBBlock  # noqa: F401
from zerodb_prefect.credentials import ZeroDBCredentials  # noqa: F401

__version__ = "0.1.0"
__all__ = [
    "ZeroDBSensor",
    "ZeroDBBlock",
    "ZeroDBCredentials",
    "ZeroDBEvent",
]
