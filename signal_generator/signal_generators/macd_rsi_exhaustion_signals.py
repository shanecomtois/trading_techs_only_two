"""
MACD/RSI Exhaustion signal generator.
Mean reversion system: Buy when MACD or RSI exhausted to downside, Sell when exhausted to upside.
"""
import pandas as pd
import numpy as np
from typing import Optional, Dict, List
from datetime import datetime
import logging

from .base_signal import BaseSignal

logger = logging.getLogger(__name__)


class MacdRsiExhaustionSignals(BaseSignal):
    """
    Combined MACD/RSI exhaustion strategy.
    Buy when MACD or RSI exhausted to downside, Sell when exhausted to upside.
    """
    
    def detect_entry_condition(self, row: pd.Series, prev_row: Optional[pd.Series] = None) -> Optional[str]:
        """
        Detect MACD or RSI exhaustion conditions.
        
        MACD Exhaustion Buy:
        - MACD percentile < 20 (required)
        - AND (MACD < 0 OR MACD crossing above signal)
        
        MACD Exhaustion Sell:
        - MACD percentile > 80 (required)
        - AND (MACD > 0 OR MACD crossing below signal)
        
        RSI Exhaustion Buy:
        - (RSI percentile < 20 OR RSI < 30) AND RSI turning up
        
        RSI Exhaustion Sell:
        - (RSI percentile > 80 OR RSI > 70) AND RSI turning down
        
        Args:
            row: Current row of data
            prev_row: Previous row (for cross detection and momentum reversal)
        
        Returns:
            'buy', 'sell', or None
        """
        strategy_config = self.config['strategies']['macd_rsi_exhaustion']
        entry_conditions = strategy_config.get('entry_conditions', {})
        
        macd_config = entry_conditions.get('macd_exhaustion', {})
        rsi_config = entry_conditions.get('rsi_exhaustion', {})
        
        # Track which indicators triggered (for metadata)
        macd_buy = False
        macd_sell = False
        rsi_buy = False
        rsi_sell = False
        
        # Check MACD exhaustion conditions
        macd_percentile = row.get('macd_line_percentile', np.nan)
        macd_line = row.get('macd_line', np.nan)
        macd_signal = row.get('macd_signal', np.nan)
        
        if not pd.isna(macd_percentile) and not pd.isna(macd_line) and not pd.isna(macd_signal):
            buy_config = macd_config.get('buy', {})
            sell_config = macd_config.get('sell', {})
            
            percentile_threshold_buy = buy_config.get('percentile_threshold', 20)
            percentile_threshold_sell = sell_config.get('percentile_threshold', 80)
            
            # MACD Buy: percentile < threshold (required) AND (zero line < 0 OR crossover above signal)
            if macd_percentile < percentile_threshold_buy:
                # Check zero line or crossover
                zero_line_ok = macd_line < 0
                crossover_ok = False
                
                if prev_row is not None:
                    prev_macd_line = prev_row.get('macd_line', np.nan)
                    prev_macd_signal = prev_row.get('macd_signal', np.nan)
                    if not pd.isna(prev_macd_line) and not pd.isna(prev_macd_signal):
                        # Crossover: MACD was below signal, now above
                        crossover_ok = prev_macd_line < prev_macd_signal and macd_line > macd_signal
                
                if zero_line_ok or crossover_ok:
                    macd_buy = True
                    logger.debug(f"MACD exhaustion BUY for {row.get('ice_connect_symbol', 'unknown')}: "
                               f"percentile={macd_percentile:.1f} < {percentile_threshold_buy}, "
                               f"zero_line={zero_line_ok}, crossover={crossover_ok}")
            
            # MACD Sell: percentile > threshold (required) AND (zero line > 0 OR crossover below signal)
            if macd_percentile > percentile_threshold_sell:
                # Check zero line or crossover
                zero_line_ok = macd_line > 0
                crossover_ok = False
                
                if prev_row is not None:
                    prev_macd_line = prev_row.get('macd_line', np.nan)
                    prev_macd_signal = prev_row.get('macd_signal', np.nan)
                    if not pd.isna(prev_macd_line) and not pd.isna(prev_macd_signal):
                        # Crossover: MACD was above signal, now below
                        crossover_ok = prev_macd_line > prev_macd_signal and macd_line < macd_signal
                
                if zero_line_ok or crossover_ok:
                    macd_sell = True
                    logger.debug(f"MACD exhaustion SELL for {row.get('ice_connect_symbol', 'unknown')}: "
                               f"percentile={macd_percentile:.1f} > {percentile_threshold_sell}, "
                               f"zero_line={zero_line_ok}, crossover={crossover_ok}")
        
        # Check RSI exhaustion conditions
        rsi_percentile = row.get('rsi_percentile', np.nan)
        rsi = row.get('rsi', np.nan)
        prev_rsi = prev_row.get('rsi', np.nan) if prev_row is not None else np.nan
        
        if not pd.isna(rsi):
            buy_config = rsi_config.get('buy', {})
            sell_config = rsi_config.get('sell', {})
            
            percentile_threshold_buy = buy_config.get('percentile_threshold', 20)
            absolute_threshold_buy = buy_config.get('absolute_threshold', 30)
            percentile_threshold_sell = sell_config.get('percentile_threshold', 80)
            absolute_threshold_sell = sell_config.get('absolute_threshold', 70)
            
            # RSI Buy: (percentile < threshold OR absolute < threshold) AND momentum reversal up
            percentile_ok = not pd.isna(rsi_percentile) and rsi_percentile < percentile_threshold_buy
            absolute_ok = rsi < absolute_threshold_buy
            momentum_ok = not pd.isna(prev_rsi) and rsi > prev_rsi  # RSI turning up
            
            if (percentile_ok or absolute_ok) and momentum_ok:
                rsi_buy = True
                logger.debug(f"RSI exhaustion BUY for {row.get('ice_connect_symbol', 'unknown')}: "
                           f"percentile={rsi_percentile:.1f if not pd.isna(rsi_percentile) else 'N/A'}, "
                           f"absolute={rsi:.1f}, momentum_up={momentum_ok}")
            
            # RSI Sell: (percentile > threshold OR absolute > threshold) AND momentum reversal down
            percentile_ok = not pd.isna(rsi_percentile) and rsi_percentile > percentile_threshold_sell
            absolute_ok = rsi > absolute_threshold_sell
            momentum_ok = not pd.isna(prev_rsi) and rsi < prev_rsi  # RSI turning down
            
            if (percentile_ok or absolute_ok) and momentum_ok:
                rsi_sell = True
                logger.debug(f"RSI exhaustion SELL for {row.get('ice_connect_symbol', 'unknown')}: "
                           f"percentile={rsi_percentile:.1f if not pd.isna(rsi_percentile) else 'N/A'}, "
                           f"absolute={rsi:.1f}, momentum_down={momentum_ok}")
        
        # Store which indicators triggered in row metadata (for later use in signal dict)
        row['_exhaustion_macd_buy'] = macd_buy
        row['_exhaustion_macd_sell'] = macd_sell
        row['_exhaustion_rsi_buy'] = rsi_buy
        row['_exhaustion_rsi_sell'] = rsi_sell
        
        # Return signal type: buy if either MACD buy OR RSI buy, sell if either MACD sell OR RSI sell
        if macd_buy or rsi_buy:
            return 'buy'
        if macd_sell or rsi_sell:
            return 'sell'
        
        return None
    
    def get_base_points(self, row: pd.Series, signal_type: str) -> int:
        """
        Get base points for exhaustion signal.
        
        Args:
            row: Current row of data
            signal_type: 'buy' or 'sell'
        
        Returns:
            Base points (50 for exhaustion signal)
        """
        strategy_config = self.config['strategies']['macd_rsi_exhaustion']
        base_points_config = strategy_config.get('base_points', {})
        return base_points_config.get('exhaustion_signal', 50)
    
    def get_strategy_name(self) -> str:
        """Get strategy name for configuration lookup."""
        return 'macd_rsi_exhaustion'
    
    def generate_signals(
        self,
        data: pd.DataFrame,
        target_date: Optional[datetime] = None
    ) -> Dict[str, List[Dict]]:
        """
        Generate signals and add exhaustion metadata (which indicator triggered).
        
        Overrides base class to add exhaustion indicator metadata to signals.
        """
        # Call parent to generate signals
        signals = super().generate_signals(data, target_date)
        
        # Add exhaustion metadata to each signal
        for signal_list in [signals.get('buy_signals', []), signals.get('sell_signals', [])]:
            for signal in signal_list:
                # Extract exhaustion metadata from row_data (stored by base class)
                row_data = signal.get('row_data', {})
                
                macd_buy = row_data.get('_exhaustion_macd_buy', False)
                macd_sell = row_data.get('_exhaustion_macd_sell', False)
                rsi_buy = row_data.get('_exhaustion_rsi_buy', False)
                rsi_sell = row_data.get('_exhaustion_rsi_sell', False)
                
                # Determine which indicator(s) triggered
                triggered_indicators = []
                if macd_buy or macd_sell:
                    triggered_indicators.append('MACD')
                if rsi_buy or rsi_sell:
                    triggered_indicators.append('RSI')
                
                # Add metadata to signal
                signal['exhaustion_indicators'] = triggered_indicators
                signal['exhaustion_macd'] = macd_buy or macd_sell
                signal['exhaustion_rsi'] = rsi_buy or rsi_sell
                signal['exhaustion_both'] = len(triggered_indicators) == 2
        
        return signals


