"""
Enhanced Trend Following signal generator using multiple entry triggers.
Supports: EMA crossover, Supertrend, MACD cross, and Aroon strong trend.
"""
import pandas as pd
import numpy as np
from typing import Optional, Dict, Tuple
import logging

from .base_signal import BaseSignal

logger = logging.getLogger(__name__)


class EnhancedTrendFollowingSignals(BaseSignal):
    """
    Enhanced trend following strategy with multiple entry triggers:
    - EMA crossover (primary)
    - Supertrend reversal (secondary)
    - MACD cross (tertiary)
    - Aroon strong trend (quaternary)
    
    All signals require ADX confirmation and momentum alignment.
    """
    
    def detect_entry_condition(self, row: pd.Series, prev_row: Optional[pd.Series] = None) -> Optional[Tuple[str, str]]:
        """
        Detect entry condition using multiple triggers.
        
        Args:
            row: Current row of data
            prev_row: Previous row (for cross detection)
        
        Returns:
            Tuple of (signal_type, trigger_type) or None
            signal_type: 'buy' or 'sell'
            trigger_type: 'ema_crossover', 'supertrend', 'macd_cross', 'aroon_strong'
        """
        strategy_config = self.config['strategies']['enhanced_trend_following']
        triggers_config = strategy_config.get('entry_triggers', {})
        enabled = triggers_config.get('enabled', {})
        
        # Check each enabled trigger
        trigger_results = []
        
        # 1. EMA Crossover
        if enabled.get('ema_crossover', True):
            ema_result = self._check_ema_crossover(row, prev_row, triggers_config.get('ema_crossover', {}))
            if ema_result:
                trigger_results.append(ema_result)
        
        # 2. Supertrend
        if enabled.get('supertrend', True):
            st_result = self._check_supertrend(row, prev_row)
            if st_result:
                trigger_results.append(st_result)
        
        # 3. MACD Cross
        if enabled.get('macd_cross', True):
            macd_result = self._check_macd_cross(row, prev_row)
            if macd_result:
                trigger_results.append(macd_result)
        
        # 4. Aroon Strong Trend
        if enabled.get('aroon_strong', True):
            aroon_result = self._check_aroon_strong(row, triggers_config.get('aroon_strong', {}))
            if aroon_result:
                trigger_results.append(aroon_result)
        
        # If no triggers fired, return None
        if not trigger_results:
            return None
        
        # Use the trigger with highest base points (if multiple fire)
        base_points_config = triggers_config.get('base_points', {})
        best_trigger = max(trigger_results, key=lambda x: base_points_config.get(x[1], 0))
        
        signal_type, trigger_type = best_trigger
        
        # Check confirmations (ADX, DI alignment, momentum) - now we know signal_type
        confirmations = self._check_confirmations(row, signal_type, strategy_config.get('confirmations', {}))
        
        if not confirmations['passed']:
            logger.debug(f"Entry trigger(s) detected for {row.get('ice_connect_symbol', 'unknown')} but confirmations failed: {confirmations['reasons']}")
            return None
        
        logger.info(f"{signal_type.upper()} signal detected for {row.get('ice_connect_symbol', 'unknown')} via {trigger_type}")
        
        # Store trigger info in row for later use
        row['_trigger_type'] = trigger_type
        row['_confirmations'] = confirmations
        
        return (signal_type, trigger_type)
    
    def _check_ema_crossover(self, row: pd.Series, prev_row: Optional[pd.Series], ema_config: Dict) -> Optional[Tuple[str, str]]:
        """Check EMA crossover trigger."""
        if prev_row is None:
            return None
        
        fast_ema_col = ema_config.get('fast_ema', 'ema_20')
        slow_ema_col = ema_config.get('slow_ema', 'ema_50')
        
        price = row.get('close', np.nan)
        fast_ema = row.get(fast_ema_col, np.nan)
        slow_ema = row.get(slow_ema_col, np.nan)
        prev_price = prev_row.get('close', np.nan)
        prev_fast_ema = prev_row.get(fast_ema_col, np.nan)
        prev_slow_ema = prev_row.get(slow_ema_col, np.nan)
        
        if pd.isna(price) or pd.isna(fast_ema) or pd.isna(slow_ema):
            return None
        if pd.isna(prev_price) or pd.isna(prev_fast_ema) or pd.isna(prev_slow_ema):
            return None
        
        # Buy: Price crosses above fast EMA AND fast EMA > slow EMA
        if prev_price <= prev_fast_ema and price > fast_ema and fast_ema > slow_ema:
            return ('buy', 'ema_crossover')
        
        # Sell: Price crosses below fast EMA AND fast EMA < slow EMA
        if prev_price >= prev_fast_ema and price < fast_ema and fast_ema < slow_ema:
            return ('sell', 'ema_crossover')
        
        return None
    
    def _check_supertrend(self, row: pd.Series, prev_row: Optional[pd.Series]) -> Optional[Tuple[str, str]]:
        """Check Supertrend reversal trigger."""
        if prev_row is None:
            return None
        
        st_direction = row.get('supertrend_direction', np.nan)
        prev_st_direction = prev_row.get('supertrend_direction', np.nan)
        
        if pd.isna(st_direction) or pd.isna(prev_st_direction):
            return None
        
        # Buy: Direction changes to 'up' (or becomes positive/1)
        # Handle both string and numeric values
        st_up = str(st_direction).lower() in ['up', '1', 'true', 'buy']
        prev_st_up = str(prev_st_direction).lower() in ['up', '1', 'true', 'buy']
        
        if not prev_st_up and st_up:
            return ('buy', 'supertrend')
        
        # Sell: Direction changes to 'down' (or becomes negative/0)
        st_down = str(st_direction).lower() in ['down', '0', 'false', 'sell']
        prev_st_down = str(prev_st_direction).lower() in ['down', '0', 'false', 'sell']
        
        if not prev_st_down and st_down:
            return ('sell', 'supertrend')
        
        return None
    
    def _check_macd_cross(self, row: pd.Series, prev_row: Optional[pd.Series]) -> Optional[Tuple[str, str]]:
        """Check MACD cross trigger."""
        if prev_row is None:
            return None
        
        macd_line = row.get('macd_line', np.nan)
        macd_signal = row.get('macd_signal', np.nan)
        prev_macd_line = prev_row.get('macd_line', np.nan)
        prev_macd_signal = prev_row.get('macd_signal', np.nan)
        
        if pd.isna(macd_line) or pd.isna(macd_signal) or pd.isna(prev_macd_line) or pd.isna(prev_macd_signal):
            return None
        
        # Buy: MACD line crosses above signal line
        if prev_macd_line < prev_macd_signal and macd_line > macd_signal:
            return ('buy', 'macd_cross')
        
        # Sell: MACD line crosses below signal line
        if prev_macd_line > prev_macd_signal and macd_line < macd_signal:
            return ('sell', 'macd_cross')
        
        return None
    
    def _check_aroon_strong(self, row: pd.Series, aroon_config: Dict) -> Optional[Tuple[str, str]]:
        """Check Aroon strong trend trigger."""
        aroon_osc = row.get('aroon_oscillator', np.nan)
        aroon_uptrend = row.get('aroon_strong_uptrend', False)
        aroon_downtrend = row.get('aroon_strong_downtrend', False)
        
        if pd.isna(aroon_osc):
            return None
        
        threshold = aroon_config.get('oscillator_threshold', 50)
        
        # Buy: Oscillator > threshold AND strong uptrend
        if aroon_osc > threshold and aroon_uptrend:
            return ('buy', 'aroon_strong')
        
        # Sell: Oscillator < -threshold AND strong downtrend
        if aroon_osc < -threshold and aroon_downtrend:
            return ('sell', 'aroon_strong')
        
        return None
    
    def _check_confirmations(self, row: pd.Series, signal_type: str, confirmations_config: Dict) -> Dict:
        """
        Check required confirmations (ADX, DI alignment, momentum).
        
        Args:
            row: Current row of data
            signal_type: 'buy' or 'sell'
            confirmations_config: Configuration dictionary for confirmations
        
        Returns:
            Dict with 'passed' (bool) and 'reasons' (list of strings)
        """
        required = confirmations_config.get('required', {})
        reasons = []
        passed = True
        
        # 1. ADX Strong (required)
        if required.get('adx_strong', True):
            adx = row.get('adx', np.nan)
            min_adx = confirmations_config.get('adx_strong', {}).get('min_adx', 25)
            
            if pd.isna(adx) or adx < min_adx:
                passed = False
                reasons.append(f"ADX {adx:.1f} < {min_adx}")
        
        # 2. DI Alignment (required)
        if required.get('di_alignment', True):
            di_plus = row.get('di_plus', np.nan)
            di_minus = row.get('di_minus', np.nan)
            
            if pd.isna(di_plus) or pd.isna(di_minus):
                passed = False
                reasons.append("DI values missing")
            else:
                # Check alignment based on signal_type
                if signal_type == 'buy' and di_plus <= di_minus:
                    passed = False
                    reasons.append(f"DI alignment: DI+ {di_plus:.2f} <= DI- {di_minus:.2f} (need DI+ > DI- for buy)")
                elif signal_type == 'sell' and di_minus <= di_plus:
                    passed = False
                    reasons.append(f"DI alignment: DI- {di_minus:.2f} <= DI+ {di_plus:.2f} (need DI- > DI+ for sell)")
        
        # 3. Momentum indicators (at least N of M must align)
        momentum_required = confirmations_config.get('momentum_required', 2)
        momentum_config = confirmations_config.get('momentum_indicators', {})
        
        momentum_aligned = 0
        
        if momentum_config.get('rsi_aligned', True):
            rsi = row.get('rsi', np.nan)
            if pd.notna(rsi):
                if (signal_type == 'buy' and rsi > 50) or (signal_type == 'sell' and rsi < 50):
                    momentum_aligned += 1
        
        if momentum_config.get('macd_histogram_aligned', True):
            macd_hist = row.get('macd_histogram', np.nan)
            if pd.notna(macd_hist):
                if (signal_type == 'buy' and macd_hist > 0) or (signal_type == 'sell' and macd_hist < 0):
                    momentum_aligned += 1
        
        if momentum_config.get('stochastic_aligned', True):
            stoch_k = row.get('stoch_k', np.nan)
            stoch_d = row.get('stoch_d', np.nan)
            if pd.notna(stoch_k) and pd.notna(stoch_d):
                if (signal_type == 'buy' and stoch_k > stoch_d) or (signal_type == 'sell' and stoch_k < stoch_d):
                    momentum_aligned += 1
        
        if momentum_aligned < momentum_required:
            passed = False
            reasons.append(f"Momentum alignment: {momentum_aligned}/{momentum_required} indicators aligned (need {momentum_required})")
        
        return {
            'passed': passed,
            'reasons': reasons,
            'adx': row.get('adx', np.nan),
            'di_plus': row.get('di_plus', np.nan),
            'di_minus': row.get('di_minus', np.nan),
            'momentum_aligned': momentum_aligned,
            'momentum_required': momentum_required
        }
    
    def get_base_points(self, row: pd.Series, signal_type: str) -> int:
        """
        Get base points based on trigger type.
        
        Args:
            row: Current row of data (should have '_trigger_type' set)
            signal_type: 'buy' or 'sell'
        
        Returns:
            Base points for the trigger
        """
        strategy_config = self.config['strategies']['enhanced_trend_following']
        triggers_config = strategy_config.get('entry_triggers', {})
        base_points_config = triggers_config.get('base_points', {})
        
        # Get trigger type from row (set in detect_entry_condition)
        trigger_type = row.get('_trigger_type', 'ema_crossover')
        
        base_points = base_points_config.get(trigger_type, 40)
        
        logger.debug(f"Base points for {trigger_type}: {base_points}")
        return base_points
    
    def get_strategy_name(self) -> str:
        """Get strategy name for configuration lookup."""
        return 'enhanced_trend_following'


