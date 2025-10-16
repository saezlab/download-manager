"""
Utility functions for download manager.
Replaces cache_manager utilities.
"""

import json
from typing import Any


def serialize(obj: Any) -> str:
    """
    Serialize an object to JSON string for logging purposes.

    Args:
        obj: Any Python object that can be JSON serialized.

    Returns:
        JSON string representation of the object.
    """
    try:
        return json.dumps(obj, indent=2, default=str)
    except (TypeError, ValueError):
        return str(obj)
