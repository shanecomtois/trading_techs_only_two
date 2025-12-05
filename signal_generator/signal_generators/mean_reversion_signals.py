"""
Mean reversion signal generator using price percentile extremes.
"""
import pandas as pd
import numpy as np
from typing import Optional
import logging

from .base_signal import BaseSignal

logger = logging.getLogger(__name__)


class MeanReversionSignals(BaseSignal):
    """
    Mean reversion strategy using price percentile extremes as entry point.
    Buy when price percentile < 25, sell when price percentile > 75.
    """
    
    def detect_entry_condition(self, row: pd.Series, prev_row: Optional[pd.Series] = None) -> Optional[str]:
        """
        Detect price percentile extreme: buy when < 25, sell when > 75.
        
        Args:
            row: Current row of data
            prev_row: Previous row (not used for percentile, but required by interface)
        
        Returns:
            'buy', 'sell', or None
        """
        price_percentile = row.get('percentile_close', np.nan)
        
        if pd.isna(price_percentile):
            return None
        
        # Buy signal: price percentile < 25 (oversold, expect bounce up)
        if price_percentile < 25:
            return 'buy'
        
        # Sell signal: price percentile > 75 (overbought, expect pullback down)
        if price_percentile > 75:
            return 'sell'
        
        return None
    
    def get_base_points(self, row: pd.Series, signal_type: str) -> int:
        """
        Get base points for price percentile extreme.
        
        Args:
            row: Current row of data
            signal_type: 'buy' or 'sell'
        
        Returns:
            Base points (50 for extreme percentile)
        """
        strategy_config = self.config['strategies']['mean_reversion']
        base_points_config = strategy_config.get('base_points', {})
        return base_points_config.get('price_percentile_extreme', 50)
    
    def get_strategy_name(self) -> str:
        """Get strategy name for configuration lookup."""
        return 'mean_reversion'


