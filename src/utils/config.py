"""
Configuration utilities for environment variable loading.

Provides type-safe environment variable loading with defaults.

Utilities provided:
- get_env_str(): Get string from environment variable
- get_env_int(): Get integer from environment variable
- get_env_float(): Get float from environment variable
- get_env_bool(): Get boolean from environment variable
- get_env_list(): Get list from environment variable
- get_env_optional(): Get optional string from environment variable

Extracted from multiple `from_env()` methods across the codebase to provide
a centralized, reusable configuration loading mechanism.

Requirements:
- 2.1: Provide helper functions for loading typed values from environment variables
- 2.2: Support types: str, int, float, bool, and list
- 2.3: Use provided default value when environment variable is missing
- 2.4: Accept "true", "false", "1", "0" (case-insensitive) for boolean
- 2.5: Split by comma and strip whitespace for list values
"""

import os
from typing import List, Optional


def get_env_str(key: str, default: str = "") -> str:
    """
    Get string from environment variable.
    
    Args:
        key: Environment variable name
        default: Default value if not set (default: "")
        
    Returns:
        Environment variable value or default
        
    Example:
        host = get_env_str("DATABASE_HOST", "localhost")
        
    Requirement 2.1, 2.2, 2.3
    """
    return os.getenv(key, default)


def get_env_int(key: str, default: int = 0) -> int:
    """
    Get integer from environment variable.
    
    Args:
        key: Environment variable name
        default: Default value if not set (default: 0)
        
    Returns:
        Parsed integer value or default
        
    Raises:
        ValueError: If the environment variable value cannot be parsed as int
        
    Example:
        port = get_env_int("DATABASE_PORT", 5432)
        
    Requirement 2.1, 2.2, 2.3
    """
    value = os.getenv(key)
    if value is None:
        return default
    return int(value)


def get_env_float(key: str, default: float = 0.0) -> float:
    """
    Get float from environment variable.
    
    Args:
        key: Environment variable name
        default: Default value if not set (default: 0.0)
        
    Returns:
        Parsed float value or default
        
    Raises:
        ValueError: If the environment variable value cannot be parsed as float
        
    Example:
        timeout = get_env_float("REQUEST_TIMEOUT", 30.0)
        
    Requirement 2.1, 2.2, 2.3
    """
    value = os.getenv(key)
    if value is None:
        return default
    return float(value)


def get_env_bool(key: str, default: bool = False) -> bool:
    """
    Get boolean from environment variable.
    
    Accepts: "true", "false", "1", "0", "yes", "no" (case-insensitive)
    
    Args:
        key: Environment variable name
        default: Default value if not set (default: False)
        
    Returns:
        Parsed boolean value or default
        
    Example:
        debug = get_env_bool("DEBUG_MODE", False)
        enabled = get_env_bool("FEATURE_ENABLED", True)
        
    Requirement 2.1, 2.2, 2.3, 2.4
    """
    value = os.getenv(key)
    if value is None:
        return default
    return value.lower() in ("true", "1", "yes")


def get_env_list(
    key: str,
    default: Optional[List[str]] = None,
    separator: str = ","
) -> List[str]:
    """
    Get list from environment variable.
    
    Splits by separator and strips whitespace from each item.
    Empty items are filtered out.
    
    Args:
        key: Environment variable name
        default: Default value if not set (default: empty list)
        separator: Separator character (default: ",")
        
    Returns:
        List of strings or default
        
    Example:
        hosts = get_env_list("KAFKA_BROKERS", ["localhost:9092"])
        # With env KAFKA_BROKERS="host1:9092, host2:9092, host3:9092"
        # Returns: ["host1:9092", "host2:9092", "host3:9092"]
        
    Requirement 2.1, 2.2, 2.3, 2.5
    """
    if default is None:
        default = []
    
    value = os.getenv(key)
    if value is None or value.strip() == "":
        return default
    
    return [item.strip() for item in value.split(separator) if item.strip()]


def get_env_optional(key: str) -> Optional[str]:
    """
    Get optional string from environment variable.
    
    Returns None if the environment variable is not set,
    unlike get_env_str which returns an empty string by default.
    
    Args:
        key: Environment variable name
        
    Returns:
        Environment variable value or None if not set
        
    Example:
        api_key = get_env_optional("API_KEY")
        if api_key is None:
            raise ValueError("API_KEY is required")
            
    Requirement 2.1, 2.2
    """
    return os.getenv(key)
