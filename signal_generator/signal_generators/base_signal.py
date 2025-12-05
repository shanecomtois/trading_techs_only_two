"""
Base signal class for all signal strategies.
Designed to be backtest-ready with date parameter support.
"""
from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Optional
import logging

from .point_calculator import PointCalculator

logger = logging.getLogger(__name__)


class BaseSignal(ABC):
    """
    Abstract base class for signal generation strategies.
    All signal strategies inherit from this class.
    """
    
    def __init__(self, config: dict, point_calculator: PointCalculator):
        """
        Initialize base signal generator.
        
        Args:
            config: Configuration dictionary
            point_calculator: PointCalculator instance
        """
        self.config = config
        self.point_calculator = point_calculator
        self.min_points = config.get('min_points_threshold', 75)
        self.max_signals = config.get('max_signals_per_type', 10)
        self.atr_stop_mult = config['atr_multipliers']['stop']
        self.atr_target_mult = config['atr_multipliers']['target']
    
    @abstractmethod
    def detect_entry_condition(self, row: pd.Series, prev_row: Optional[pd.Series] = None) -> Optional[str]:
        """
        Detect if entry condition is met for a signal.
        
        Args:
            row: Current row of data
            prev_row: Previous row (for cross detection)
        
        Returns:
            'buy', 'sell', or None
        """
        pass
    
    @abstractmethod
    def get_base_points(self, row: pd.Series, signal_type: str) -> int:
        """
        Get base points for entry condition.
        
        Args:
            row: Current row of data
            signal_type: 'buy' or 'sell'
        
        Returns:
            Base points awarded
        """
        pass
    
    def calculate_stop_target(self, row: pd.Series, signal_type: str) -> Dict[str, float]:
        """
        Calculate stop loss and target using ATR.
        
        Args:
            row: Current row of data
            signal_type: 'buy' or 'sell'
        
        Returns:
            Dictionary with 'stop', 'target', 'atr', 'stop_pct', 'target_pct'
        """
        atr = row.get('atr', np.nan)
        close = row.get('close', np.nan)
        
        if pd.isna(atr) or pd.isna(close) or atr <= 0:
            return {
                'stop': np.nan,
                'target': np.nan,
                'atr': np.nan,
                'stop_pct': np.nan,
                'target_pct': np.nan
            }
        
        stop_distance = atr * self.atr_stop_mult
        target_distance = atr * self.atr_target_mult
        
        if signal_type == 'buy':
            stop_price = close - stop_distance
            target_price = close + target_distance
        else:  # sell
            stop_price = close + stop_distance
            target_price = close - target_distance
        
        stop_pct = (stop_price - close) / close * 100
        target_pct = (target_price - close) / close * 100
        
        return {
            'stop': stop_price,
            'target': target_price,
            'atr': atr,
            'stop_pct': stop_pct,
            'target_pct': target_pct
        }
    
    def calculate_position_size(self, row: pd.Series) -> float:
        """
        Calculate volatility-adjusted position size.
        
        Uses improved inverse ATR formula: position = base × (target_atr_pct / actual_atr_pct)
        This is more intuitive than the previous formula and easier to tune.
        
        Args:
            row: Current row of data
        
        Returns:
            Position size percentage
        """
        base_size = self.config['position_sizing']['base_size']
        method = self.config['position_sizing']['method']
        
        if method == 'inverse_atr_pct':
            atr_pct = row.get('atr_pct_of_price', np.nan)
            if pd.isna(atr_pct) or atr_pct <= 0:
                return base_size
            
            # Get target ATR% from config (default 5.0 if not specified)
            # Target ATR% represents "normal" volatility level
            target_atr_pct = self.config['position_sizing'].get('target_atr_pct', 5.0)
            
            # Improved formula: position = base × (target / actual)
            # If volatility is half of normal, use 2x position
            # If volatility is double normal, use 0.5x position
            position_size = base_size * (target_atr_pct / atr_pct)
            
            # Cap between 10% and 200%
            position_size = max(10.0, min(200.0, position_size))
            
            return round(position_size, 2)
        
        # Default: return base size
        return float(base_size)
    
    def generate_signals(
        self,
        data: pd.DataFrame,
        target_date: Optional[datetime] = None
    ) -> Dict[str, List[Dict]]:
        """
        Generate signals for all symbols in data.
        
        Args:
            data: DataFrame with indicator data
            target_date: Target date for analysis (None = use most recent date)
        
        Returns:
            Dictionary with 'buy_signals' and 'sell_signals' lists
        """
        # Filter to target date if specified (but keep previous weeks for cross detection)
        # The prepare_data function already handles this, so we just need to ensure
        # we're only generating signals for the target date
        if target_date and 'Date' in data.columns:
            # Keep all data for cross detection, but only generate signals for target_date
            target_date_only = target_date.date()
            # We'll filter later when creating signals
            pass
        
        if len(data) == 0:
            logger.warning("No data available for signal generation")
            return {'buy_signals': [], 'sell_signals': []}
        
        logger.info(f"Generating signals for {self.get_strategy_name()}: {len(data)} rows, {data['ice_connect_symbol'].nunique()} unique symbols")
        
        # Diagnostic counters (will be set in loop)
        symbols_checked = 0
        symbols_with_macd = 0
        symbols_with_prev_week = 0
        symbols_with_cross = 0
        
        # Sort by date (most recent first) for cross detection
        if 'Date' in data.columns:
            data = data.sort_values('Date', ascending=False).reset_index(drop=True)
        
        buy_signals = []
        sell_signals = []
        
        # Group by symbol to detect crosses
        for symbol in data['ice_connect_symbol'].unique():
            symbol_data = data[data['ice_connect_symbol'] == symbol].copy()
            symbol_data = symbol_data.sort_values('Date', ascending=False).reset_index(drop=True)
            
            if len(symbol_data) < 1:
                continue  # Need at least 1 row
            
            # If target_date specified, only check if most recent row matches target_date
            if target_date and 'Date' in symbol_data.columns:
                most_recent_date = symbol_data.iloc[0]['Date']
                if pd.notna(most_recent_date):
                    if isinstance(most_recent_date, str):
                        most_recent_date = pd.to_datetime(most_recent_date)
                    if most_recent_date.date() != target_date.date():
                        continue  # Skip symbols that don't have data for target_date
            
            # Check current row (most recent) against previous
            # Use .copy() to avoid SettingWithCopyWarning when modifying metadata
            current_row = symbol_data.iloc[0].copy()
            prev_row = symbol_data.iloc[1].copy() if len(symbol_data) > 1 else None
            
            # Detect entry condition
            entry_result = self.detect_entry_condition(current_row, prev_row)
            
            # Handle both old format (just signal_type) and new format (tuple of signal_type, trigger_type)
            if entry_result is None:
                continue
            
            if isinstance(entry_result, tuple):
                signal_type, trigger_type = entry_result
                # trigger_type is already stored in current_row by detect_entry_condition
            else:
                # Backward compatibility: old format returns just signal_type
                signal_type = entry_result
                current_row['_trigger_type'] = 'macd_cross'  # Default for old format
            
            symbols_with_cross += 1
            logger.info(f"Signal detected for {symbol}: {signal_type} (strategy: {self.get_strategy_name()})")
            
            # Get base points
            base_points = self.get_base_points(current_row, signal_type)
            
            # Calculate confluence bonuses
            is_spread = current_row.get('is_outright', True) == False
            confluence = self.point_calculator.calculate_confluence_bonuses(
                current_row,
                self.get_strategy_name(),
                signal_type,
                is_spread
            )
            
            # Calculate tenor/liquidity bonus
            # Note: was_active_prior_week will be set later by prior_week_checker, but we pass it if it exists
            was_active_prior_week = current_row.get('was_active_prior_week', False)
            tenor_liquidity = self.point_calculator.calculate_tenor_liquidity_bonus(
                {'symbol': symbol, 'row_data': current_row.to_dict(), 'was_active_prior_week': was_active_prior_week},
                target_date
            )
            
            # Calculate trend exhaustion penalty (applies to trend_following and enhanced_trend_following)
            exhaustion_penalty = self.point_calculator.calculate_trend_exhaustion_penalty(
                current_row,
                self.get_strategy_name(),
                signal_type
            )
            
            total_points = base_points + confluence['total_bonus'] + tenor_liquidity['total_bonus'] - exhaustion_penalty['total_penalty']
            
            # Calculate stop/target
            stop_target = self.calculate_stop_target(current_row, signal_type)
            
            # Calculate position size
            pos_pct = self.calculate_position_size(current_row)
            
            # Calculate duration (days since entry)
            entry_date = current_row.get('Date')
            if pd.isna(entry_date):
                duration = 0
            else:
                if isinstance(entry_date, str):
                    entry_date = pd.to_datetime(entry_date)
                duration = (datetime.now().date() - entry_date.date()).days if target_date is None else 0
            
            # Create signal dictionary
            signal = {
                'symbol': symbol,
                'signal_type': signal_type,
                'entry_date': entry_date,
                'entry_price': current_row.get('close'),
                'stop': stop_target['stop'],
                'target': stop_target['target'],
                'stop_pct': stop_target['stop_pct'],
                'target_pct': stop_target['target_pct'],
                'atr': stop_target['atr'],
                'pos_pct': pos_pct,
                'points': total_points,
                'base_points': base_points,
                'confluence_bonus': confluence['total_bonus'],
                'confluence_breakdown': confluence['breakdown'],
                'tenor_liquidity_bonus': tenor_liquidity['total_bonus'],
                'tenor_liquidity_breakdown': tenor_liquidity['breakdown'],
                'exhaustion_penalty': exhaustion_penalty['total_penalty'],
                'exhaustion_penalty_breakdown': exhaustion_penalty['breakdown'],
                'alignment_score': confluence['alignment_score'],
                'duration': duration,
                'is_fallback': False,  # Will be set to True for fallback signals
                'was_active_prior_week': False,  # Will be set by prior_week_checker
                'row_data': current_row.to_dict()  # Store full row for later use
            }
            # Add strategy name to row_data for easier lookup
            signal['row_data']['strategy_name'] = self.get_strategy_name()
            
            if signal_type == 'buy':
                buy_signals.append(signal)
            else:
                sell_signals.append(signal)
        
        # Log diagnostic summary for trend following strategy
        if self.get_strategy_name() == 'trend_following':
            logger.info(f"Trend Following Diagnostics:")
            logger.info(f"  Symbols checked: {symbols_checked}")
            logger.info(f"  Symbols with valid MACD (current week): {symbols_with_macd}")
            logger.info(f"  Symbols with valid MACD (previous week): {symbols_with_prev_week}")
            logger.info(f"  Symbols with MACD cross detected: {symbols_with_cross}")
            if symbols_checked > 0:
                logger.info(f"  MACD coverage: {symbols_with_macd/symbols_checked*100:.1f}% (current), {symbols_with_prev_week/symbols_checked*100:.1f}% (previous)")
        
        # Store all signals before filtering (for stats calculation)
        all_buy_signals = buy_signals.copy()
        all_sell_signals = sell_signals.copy()
        
        # Filter and rank: get top N signals with fallback logic
        buy_signals = self._filter_and_rank_with_fallback(buy_signals)
        sell_signals = self._filter_and_rank_with_fallback(sell_signals)
        
        return {
            'buy_signals': buy_signals,
            'sell_signals': sell_signals,
            'all_buy_signals': all_buy_signals,  # All signals before filtering
            'all_sell_signals': all_sell_signals  # All signals before filtering
        }
    
    def _filter_and_rank_with_fallback(self, signals: List[Dict]) -> List[Dict]:
        """
        Filter and rank signals by points, with fallback to show top 1 if no qualified signals.
        
        Args:
            signals: List of signal dictionaries
        
        Returns:
            Top N qualified signals, or top 1 fallback if no qualified signals exist
        """
        if len(signals) == 0:
            logger.info(f"No signals generated for {self.get_strategy_name()}")
            return []
        
        logger.info(f"Generated {len(signals)} signals for {self.get_strategy_name()} before filtering")
        
        # Sort by points (descending)
        signals_sorted = sorted(signals, key=lambda x: x['points'], reverse=True)
        
        # Separate qualified (>= min_points) and fallback (< min_points)
        qualified = [s for s in signals_sorted if s['points'] >= self.min_points]
        fallback = [s for s in signals_sorted if s['points'] < self.min_points]
        
        logger.info(f"  Qualified (>= {self.min_points}): {len(qualified)}, Fallback (< {self.min_points}): {len(fallback)}")
        
        # If we have qualified signals, return top N
        if len(qualified) > 0:
            result = qualified[:self.max_signals]
            logger.info(f"  Returning {len(result)} qualified signals")
            return result
        
        # If no qualified signals, return top 1 fallback (if exists)
        if len(fallback) > 0:
            fallback_signal = fallback[0].copy()
            fallback_signal['is_fallback'] = True
            logger.info(f"  Returning 1 fallback signal (points: {fallback_signal['points']})")
            return [fallback_signal]
        
        # No signals at all
        logger.info(f"  No signals to return")
        return []
    
    @abstractmethod
    def get_strategy_name(self) -> str:
        """
        Get strategy name for configuration lookup.
        
        Returns:
            Strategy name ('trend_following' or 'mean_reversion')
        """
        pass


