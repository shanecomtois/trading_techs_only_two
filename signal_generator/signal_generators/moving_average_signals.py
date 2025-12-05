"""
Simple moving average signal generator - Always On system for debugging.
Generates signals based on price position relative to EMA (no cross required).
"""
import pandas as pd
import numpy as np
from typing import Optional
import logging

from .base_signal import BaseSignal

logger = logging.getLogger(__name__)


class MovingAverageSignals(BaseSignal):
    """
    Always-on moving average strategy for debugging.
    Buy when price is above EMA 20, sell when price is below EMA 20.
    This generates more signals than cross-based systems for testing purposes.
    """
    
    def detect_entry_condition(self, row: pd.Series, prev_row: Optional[pd.Series] = None) -> Optional[str]:
        """
        Detect price position relative to EMA: buy when price is above EMA,
        sell when price is below EMA (always on system for debugging).
        
        Args:
            row: Current row of data
            prev_row: Previous row (not used in always-on mode, kept for interface compatibility)
        
        Returns:
            'buy', 'sell', or None
        """
        # Get close price - handle both Series and scalar
        close_val = row.get('close', np.nan)
        if isinstance(close_val, pd.Series):
            close = close_val.iloc[0] if len(close_val) > 0 else np.nan
        else:
            close = close_val
        
        # Use EMA 20 for the moving average (configurable)
        ema_period = 20
        ema_key = f'ema_{ema_period}'
        ema_val = row.get(ema_key, np.nan)
        
        # Handle both Series and scalar
        if isinstance(ema_val, pd.Series):
            ema = ema_val.iloc[0] if len(ema_val) > 0 else np.nan
        else:
            ema = ema_val
        
        # Debug: log if EMA column is missing
        if ema_key not in row.index and pd.isna(ema):
            logger.debug(f"EMA column '{ema_key}' not found in row. Available columns: {[c for c in row.index if 'ema' in str(c).lower()][:5]}")
        
        if pd.isna(close) or pd.isna(ema):
            return None
        
        # Always-on system: generate signal based on current price position
        # Buy signal: Price is above EMA
        if close > ema:
            return 'buy'
        
        # Sell signal: Price is below EMA
        if close < ema:
            return 'sell'
        
        # If price equals EMA exactly (rare), return None
        return None
    
    def get_base_points(self, row: pd.Series, signal_type: str) -> int:
        """
        Get base points for price vs EMA cross.
        
        Args:
            row: Current row of data
            signal_type: 'buy' or 'sell'
        
        Returns:
            Base points (50 for EMA cross)
        """
        strategy_config = self.config['strategies']['moving_average']
        base_points_config = strategy_config.get('base_points', {})
        return base_points_config.get('price_ema_cross', 50)
    
    def get_strategy_name(self) -> str:
        """Get strategy name for configuration lookup."""
        return 'moving_average'


