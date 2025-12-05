"""
Configuration module for signal generator system.
"""
from pathlib import Path
import json
import logging

logger = logging.getLogger(__name__)

# Default config file path
DEFAULT_CONFIG_PATH = Path(__file__).parent / 'signal_settings.json'


def load_config(config_path: str = None) -> dict:
    """
    Load signal configuration from JSON file.
    
    Args:
        config_path: Path to config file (default: signal_settings.json in config directory)
    
    Returns:
        Dictionary with configuration settings
    """
    if config_path is None:
        config_path = DEFAULT_CONFIG_PATH
    else:
        config_path = Path(config_path)
    
    if not config_path.exists():
        logger.error(f"Config file not found: {config_path}")
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        logger.info(f"Loaded configuration from {config_path}")
        return config
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON config file {config_path}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error loading config file {config_path}: {e}")
        raise


def validate_config(config: dict) -> tuple[bool, list]:
    """
    Validate configuration structure and values.
    
    Args:
        config: Configuration dictionary
    
    Returns:
        Tuple of (is_valid, list_of_warnings)
    """
    warnings = []
    required_keys = [
        'min_points_threshold',
        'max_signals_per_type',
        'atr_multipliers',
        'position_sizing',
        'alignment_weights',
        'strategies'
    ]
    
    # Check required top-level keys
    for key in required_keys:
        if key not in config:
            return False, [f"Missing required config key: {key}"]
    
    # Validate strategies
    if 'trend_following' not in config['strategies']:
        warnings.append("Missing 'trend_following' strategy configuration")
    if 'mean_reversion' not in config['strategies']:
        warnings.append("Missing 'mean_reversion' strategy configuration")
    
    # Validate ATR multipliers
    atr = config.get('atr_multipliers', {})
    if 'stop' not in atr or 'target' not in atr:
        warnings.append("ATR multipliers missing 'stop' or 'target'")
    
    # Validate thresholds
    if config.get('min_points_threshold', 0) < 0:
        warnings.append("min_points_threshold should be >= 0")
    if config.get('max_signals_per_type', 0) < 1:
        warnings.append("max_signals_per_type should be >= 1")
    
    return True, warnings


