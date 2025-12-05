"""
Trend following signal generator using MACD cross.
"""
import pandas as pd
import numpy as np
from typing import Optional
import logging

from .base_signal import BaseSignal

logger = logging.getLogger(__name__)


class TrendFollowingSignals(BaseSignal):
    """
    Trend following strategy using MACD buy/sell cross as entry point.
    """
    
    def detect_entry_condition(self, row: pd.Series, prev_row: Optional[pd.Series] = None) -> Optional[str]:
        """
        Detect MACD cross: buy when MACD line crosses above signal line,
        sell when MACD line crosses below signal line.
        
        Args:
            row: Current row of data
            prev_row: Previous row (for cross detection)
        
        Returns:
            'buy', 'sell', or None
        """
        macd_line = row.get('macd_line', np.nan)
        macd_signal = row.get('macd_signal', np.nan)
        
        if pd.isna(macd_line) or pd.isna(macd_signal):
            logger.debug(f"MACD values missing for symbol {row.get('ice_connect_symbol', 'unknown')}: macd_line={macd_line}, macd_signal={macd_signal}")
            return None
        
        # Need previous row to detect cross
        if prev_row is None:
            logger.debug(f"No previous row available for symbol {row.get('ice_connect_symbol', 'unknown')} - cannot detect cross")
            return None
        
        prev_macd_line = prev_row.get('macd_line', np.nan)
        prev_macd_signal = prev_row.get('macd_signal', np.nan)
        
        if pd.isna(prev_macd_line) or pd.isna(prev_macd_signal):
            logger.debug(f"Previous MACD values missing for symbol {row.get('ice_connect_symbol', 'unknown')}: prev_macd_line={prev_macd_line}, prev_macd_signal={prev_macd_signal}")
            return None
        
        # Buy signal: MACD line crosses above signal line
        # Previous: macd_line < macd_signal (MACD below signal)
        # Current: macd_line > macd_signal (MACD above signal)
        if prev_macd_line < prev_macd_signal and macd_line > macd_signal:
            logger.info(f"BUY cross detected for {row.get('ice_connect_symbol', 'unknown')}: prev({prev_macd_line:.4f} < {prev_macd_signal:.4f}) -> curr({macd_line:.4f} > {macd_signal:.4f})")
            return 'buy'
        
        # Sell signal: MACD line crosses below signal line
        # Previous: macd_line > macd_signal (MACD above signal)
        # Current: macd_line < macd_signal (MACD below signal)
        if prev_macd_line > prev_macd_signal and macd_line < macd_signal:
            logger.info(f"SELL cross detected for {row.get('ice_connect_symbol', 'unknown')}: prev({prev_macd_line:.4f} > {prev_macd_signal:.4f}) -> curr({macd_line:.4f} < {macd_signal:.4f})")
            return 'sell'
        
        # Log when we have valid MACD but no cross (for debugging)
        logger.debug(f"No cross for {row.get('ice_connect_symbol', 'unknown')}: prev({prev_macd_line:.4f} vs {prev_macd_signal:.4f}), curr({macd_line:.4f} vs {macd_signal:.4f})")
        return None
    
    def get_base_points(self, row: pd.Series, signal_type: str) -> int:
        """
        Get base points for MACD cross.
        
        Args:
            row: Current row of data
            signal_type: 'buy' or 'sell'
        
        Returns:
            Base points (50 for MACD cross)
        """
        strategy_config = self.config['strategies']['trend_following']
        base_points_config = strategy_config.get('base_points', {})
        return base_points_config.get('macd_cross', 50)
    
    def get_strategy_name(self) -> str:
        """Get strategy name for configuration lookup."""
        return 'trend_following'


