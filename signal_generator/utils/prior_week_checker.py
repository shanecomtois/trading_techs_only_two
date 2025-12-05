"""
Utility to check if signals were active in the prior week.
Files are named with Fridays, so we find the prior Friday (7 days before).
"""
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Set, Tuple
import logging
import pandas as pd

from ..data_loaders import load_data, prepare_data
from ..signal_generators import TrendFollowingSignals, MeanReversionSignals, MacdRsiExhaustionSignals, PointCalculator
from ..config import load_config

logger = logging.getLogger(__name__)


def find_prior_friday(current_date: datetime) -> datetime:
    """
    Find the Friday that's 7 days before the current date.
    Since files are named with Fridays, we simply go back 7 days.
    
    Args:
        current_date: Current data date (should be a Friday)
        
    Returns:
        Prior Friday date (7 days before)
    """
    # Go back 7 days - since files are named with Fridays, this should be the prior Friday
    prior_friday = current_date - timedelta(days=7)
    
    logger.info(f"Prior Friday calculation: current={current_date.strftime('%Y-%m-%d %A')}, prior_friday={prior_friday.strftime('%Y-%m-%d %A')}")
    
    return prior_friday


def check_prior_week_signals(
    current_signals: Dict,
    data_date: datetime,
    data_dir: str,
    config: dict
) -> Dict[str, bool]:
    """
    Check which current signals were active in the prior week.
    
    Args:
        current_signals: Dict with 'buy_signals' and 'sell_signals' for all strategies
        data_date: Current data date
        data_dir: Directory containing CSV files
        config: Configuration dictionary
        
    Returns:
        Dictionary mapping (symbol, strategy_type, signal_type) -> True/False
        Format: {('symbol', 'strategy', 'buy'): True, ...}
    """
    logger.info("\n[Prior Week Check] Checking prior week signals...")
    
    # Find prior Friday
    prior_friday = find_prior_friday(data_date)
    prior_date_str = prior_friday.strftime('%Y-%m-%d')
    
    logger.info(f"Looking for prior week data: {prior_date_str}")
    
    # Try to load prior week data
    try:
        prior_df = load_data(target_date=prior_friday, data_dir=data_dir)
        if prior_df is None:
            logger.warning(f"Could not load prior week data for {prior_date_str}")
            return {}
        
        # Prepare prior week data
        prior_prepared = prepare_data(prior_df, target_date=prior_friday)
        logger.info(f"✓ Prior week data loaded: {len(prior_prepared)} rows")
        
    except Exception as e:
        logger.warning(f"Error loading prior week data: {e}")
        return {}
    
    # Generate signals for prior week
    try:
        point_calculator = PointCalculator(config)
        trend_gen = TrendFollowingSignals(config, point_calculator)
        from ..signal_generators.enhanced_trend_signals import EnhancedTrendFollowingSignals
        enhanced_trend_gen = EnhancedTrendFollowingSignals(config, point_calculator)
        mean_rev_gen = MeanReversionSignals(config, point_calculator)
        macd_rsi_exhaustion_gen = MacdRsiExhaustionSignals(config, point_calculator)
        
        prior_trend = trend_gen.generate_signals(prior_prepared, target_date=prior_friday)
        prior_enhanced_trend = enhanced_trend_gen.generate_signals(prior_prepared, target_date=prior_friday)
        prior_mean_rev = mean_rev_gen.generate_signals(prior_prepared, target_date=prior_friday)
        prior_macd_rsi_exhaustion = macd_rsi_exhaustion_gen.generate_signals(prior_prepared, target_date=prior_friday)
        
        logger.info(f"✓ Prior week signals generated: "
                   f"Trend={len(prior_trend.get('buy_signals', [])) + len(prior_trend.get('sell_signals', []))}, "
                   f"EnhancedTrend={len(prior_enhanced_trend.get('buy_signals', [])) + len(prior_enhanced_trend.get('sell_signals', []))}, "
                   f"MeanRev={len(prior_mean_rev.get('buy_signals', [])) + len(prior_mean_rev.get('sell_signals', []))}, "
                   f"MACD/RSIExhaustion={len(prior_macd_rsi_exhaustion.get('buy_signals', [])) + len(prior_macd_rsi_exhaustion.get('sell_signals', []))}")
        
    except Exception as e:
        logger.warning(f"Error generating prior week signals: {e}")
        return {}
    
    # Create lookup set: (symbol, strategy_type, signal_type) -> True
    prior_lookup: Set[Tuple[str, str, str]] = set()
    
    # Process prior week signals
    for strategy_key, prior_signals, strategy_name in [
        ('trend_following', prior_trend, 'Trend Following'),
        ('enhanced_trend_following', prior_enhanced_trend, 'Enhanced Trend Following'),
        ('mean_reversion', prior_mean_rev, 'Mean Reversion'),
        ('macd_rsi_exhaustion', prior_macd_rsi_exhaustion, 'MACD/RSI Exhaustion')
    ]:
        for signal_type in ['buy', 'sell']:
            for signal in prior_signals.get(f'{signal_type}_signals', []):
                symbol = signal.get('symbol', '')
                if symbol:
                    prior_lookup.add((symbol, strategy_name, signal_type))
    
    logger.info(f"✓ Prior week lookup created: {len(prior_lookup)} unique signals")
    
    # Check current signals against prior week
    result: Dict[str, bool] = {}
    
    # Process current signals and add was_active_prior_week flag
    # We'll process signals from each strategy separately to get correct strategy type
    strategy_signal_lists = [
        ('trend_following', 'Trend Following', current_signals.get('trend_following', {})),
        ('enhanced_trend_following', 'Enhanced Trend Following', current_signals.get('enhanced_trend_following', {})),
        ('mean_reversion', 'Mean Reversion', current_signals.get('mean_reversion', {})),
        ('macd_rsi_exhaustion', 'MACD/RSI Exhaustion', current_signals.get('macd_rsi_exhaustion', {}))
    ]
    
    for strategy_key, strategy_display, strategy_signals in strategy_signal_lists:
        for signal_type in ['buy', 'sell']:
            for signal in strategy_signals.get(f'{signal_type}_signals', []):
                symbol = signal.get('symbol', '')
                
                # Create lookup key: (symbol, strategy_type, signal_type)
                lookup_key = (symbol, strategy_display, signal_type)
                
                # Check if in prior week
                was_active = lookup_key in prior_lookup
                
                # Store directly in signal for easy access
                signal['was_active_prior_week'] = was_active
                
                # Recalculate bonus breakdown if was_active is True (to include PRWK bonus)
                # Note: We need to recalculate because the bonus was calculated with was_active=False initially
                if was_active:
                    # Recalculate tenor/liquidity bonus with updated was_active_prior_week
                    # point_calculator was already created above (line 81)
                    # Build signal dict for bonus calculation (point_calculator expects row_data)
                    bonus_signal_dict = {
                        'symbol': signal.get('symbol', ''),
                        'row_data': signal.get('row_data', {}),  # row_data is stored in signal dict
                        'was_active_prior_week': was_active
                    }
                    
                    tenor_liquidity = point_calculator.calculate_tenor_liquidity_bonus(
                        bonus_signal_dict,
                        data_date
                    )
                    # Update the signal with new breakdown
                    signal['tenor_liquidity_bonus'] = tenor_liquidity['total_bonus']
                    signal['tenor_liquidity_breakdown'] = tenor_liquidity['breakdown']
                    # Recalculate total points
                    base_points = signal.get('base_points', 0)
                    confluence_bonus = signal.get('confluence_bonus', 0)
                    signal['points'] = base_points + confluence_bonus + tenor_liquidity['total_bonus']
                
                # Also store in result dict for reference
                result[str(lookup_key)] = was_active
    
    active_count = sum(1 for v in result.values() if v)
    logger.info(f"✓ Prior week check complete: {active_count}/{len(result)} signals were active last week")
    
    return result


