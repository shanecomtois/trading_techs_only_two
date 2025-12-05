"""
Point calculator for confluence bonus points.
Calculates bonus points based on indicator alignment.
"""
import pandas as pd
import numpy as np
import re
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class PointCalculator:
    """
    Calculates confluence bonus points for signals.
    Shows all indicators even if points are 0.
    """
    
    def __init__(self, config: dict):
        """
        Initialize point calculator with configuration.
        
        Args:
            config: Configuration dictionary with alignment_weights and strategies
        """
        self.config = config
        self.alignment_weights = config.get('alignment_weights', {})
    
    def calculate_confluence_bonuses(
        self,
        row: pd.Series,
        strategy_name: str,
        signal_type: str,  # 'buy' or 'sell'
        is_spread: bool = False
    ) -> Dict[str, any]:
        """
        Calculate confluence bonus points for a signal.
        
        Args:
            row: DataFrame row with indicator values
            strategy_name: Name of strategy ('trend_following' or 'mean_reversion')
            signal_type: 'buy' or 'sell'
            is_spread: Whether this is a spread (for correlation/cointegration)
        
        Returns:
            Dictionary with:
                - breakdown: Dict of indicator -> points
                - total_bonus: Total bonus points
                - alignment_score: Weighted alignment score (0-100)
        """
        strategy_config = self.config['strategies'].get(strategy_name, {})
        confluence_config = strategy_config.get('confluence_bonuses', {})
        
        breakdown = {}
        total_bonus = 0
        alignment_contributions = []
        
        # Check each confluence indicator
        for indicator_name, indicator_config in confluence_config.items():
            # Skip non-dictionary items (like "comment" keys)
            if not isinstance(indicator_config, dict):
                continue
            
            points = indicator_config.get('points', 0)
            points_awarded = 0
            
            # Calculate points based on indicator type
            if indicator_name == 'rsi_aligned':
                points_awarded = self._check_rsi_aligned(row, signal_type, points)
            elif indicator_name == 'stochastic_aligned':
                points_awarded = self._check_stochastic_aligned(row, signal_type, points)
            elif indicator_name == 'cci_aligned':
                points_awarded = self._check_cci_aligned(row, signal_type, points)
            elif indicator_name == 'adx_strong':
                points_awarded = self._check_adx_strong(row, points)
            elif indicator_name == 'bollinger_aligned':
                points_awarded = self._check_bollinger_aligned(row, signal_type, points)
            elif indicator_name == 'bollinger_extreme':
                points_awarded = self._check_bollinger_extreme(row, signal_type, points)
            elif indicator_name == 'correlation_high':
                points_awarded = self._check_correlation_high(row, points) if is_spread else 0
            elif indicator_name == 'cointegration':
                points_awarded = self._check_cointegration(row, points) if is_spread else 0
            elif indicator_name == 'rsi_percentile_aligned':
                points_awarded = self._check_rsi_percentile_aligned(row, signal_type, points)
            elif indicator_name == 'macd_reversal':
                points_awarded = self._check_macd_reversal(row, signal_type, points)
            elif indicator_name == 'adx_very_strong':
                points_awarded = self._check_adx_very_strong(row, points)
            elif indicator_name == 'di_alignment':
                points_awarded = self._check_di_alignment(row, signal_type, points)
            elif indicator_name == 'macd_histogram_aligned':
                points_awarded = self._check_macd_histogram_aligned(row, signal_type, points)
            elif indicator_name == 'ema_50_aligned':
                points_awarded = self._check_ema_aligned(row, signal_type, 'ema_50', points)
            elif indicator_name == 'ema_100_aligned':
                points_awarded = self._check_ema_aligned(row, signal_type, 'ema_100', points)
            elif indicator_name == 'ema_200_aligned':
                points_awarded = self._check_ema_aligned(row, signal_type, 'ema_200', points)
            elif indicator_name == 'both_indicators_exhausted':
                points_awarded = self._check_both_indicators_exhausted(row, points)
            
            # Store breakdown (always show, even if 0)
            breakdown[indicator_name] = points_awarded
            total_bonus += points_awarded
            
            # Add to alignment calculation if points awarded
            if points_awarded > 0:
                weight = self._get_indicator_weight(indicator_name)
                alignment_contributions.append(weight)
        
        # Calculate weighted alignment score (0-100)
        alignment_score = self._calculate_alignment_score(alignment_contributions, len(confluence_config))
        
        return {
            'breakdown': breakdown,
            'total_bonus': total_bonus,
            'alignment_score': alignment_score
        }
    
    def _check_rsi_aligned(self, row: pd.Series, signal_type: str, points: int) -> int:
        """Check if RSI is aligned with signal direction."""
        if pd.isna(row.get('rsi')):
            return 0
        
        rsi = row['rsi']
        if signal_type == 'buy' and rsi < 30:
            return points
        elif signal_type == 'sell' and rsi > 70:
            return points
        return 0
    
    def _check_stochastic_aligned(self, row: pd.Series, signal_type: str, points: int) -> int:
        """Check if Stochastic is aligned with signal direction."""
        stoch_k = row.get('stoch_k', np.nan)
        if pd.isna(stoch_k):
            return 0
        
        if signal_type == 'buy' and stoch_k < 20:
            return points
        elif signal_type == 'sell' and stoch_k > 80:
            return points
        return 0
    
    def _check_cci_aligned(self, row: pd.Series, signal_type: str, points: int) -> int:
        """Check if CCI is aligned with signal direction."""
        cci = row.get('cci', np.nan)
        if pd.isna(cci):
            return 0
        
        if signal_type == 'buy' and cci < -100:
            return points
        elif signal_type == 'sell' and cci > 100:
            return points
        return 0
    
    def _check_adx_strong(self, row: pd.Series, points: int) -> int:
        """Check if ADX indicates strong trend."""
        adx = row.get('adx', np.nan)
        if pd.isna(adx):
            return 0
        
        if adx > 25:
            return points
        return 0
    
    def _check_bollinger_aligned(self, row: pd.Series, signal_type: str, points: int) -> int:
        """Check if price position relative to Bollinger Bands is aligned."""
        close = row.get('close', np.nan)
        bb_upper = row.get('bb_upper', np.nan)
        bb_lower = row.get('bb_lower', np.nan)
        
        if pd.isna(close) or pd.isna(bb_upper) or pd.isna(bb_lower):
            return 0
        
        # Check if price is near appropriate band
        if signal_type == 'buy' and close <= bb_lower * 1.02:  # Within 2% of lower band
            return points
        elif signal_type == 'sell' and close >= bb_upper * 0.98:  # Within 2% of upper band
            return points
        return 0
    
    def _check_bollinger_extreme(self, row: pd.Series, signal_type: str, points: int) -> int:
        """Check if price is at Bollinger Band extreme."""
        return self._check_bollinger_aligned(row, signal_type, points)
    
    def _check_correlation_high(self, row: pd.Series, points: int) -> int:
        """Check if correlation is high for spread."""
        correlation = row.get('correlation', np.nan)
        if pd.isna(correlation):
            return 0
        
        if correlation > 0.7:
            return points
        return 0
    
    def _check_cointegration(self, row: pd.Series, points: int) -> int:
        """Check if cointegration is significant for spread."""
        cointegration_pvalue = row.get('cointegration_pvalue', np.nan)
        
        # Get significance level from config (default 0.05 = 5% for standard statistical significance)
        # This should match the level used in pull_ohlc_data.py for consistency
        spread_analysis_config = self.config.get('spread_analysis', {})
        cointegration_config = spread_analysis_config.get('cointegration', {})
        significance_level = cointegration_config.get('significance_level', 0.05)
        
        if pd.isna(cointegration_pvalue):
            return 0
        
        # Cointegration is significant if p-value < significance level
        # Lower p-value = stronger evidence of cointegration
        if cointegration_pvalue < significance_level:
            return points
        return 0
    
    def _check_rsi_percentile_aligned(self, row: pd.Series, signal_type: str, points: int) -> int:
        """Check if RSI percentile is aligned with price percentile extreme."""
        rsi_percentile = row.get('rsi_percentile', np.nan)
        if pd.isna(rsi_percentile):
            return 0
        
        if signal_type == 'buy' and rsi_percentile < 25:
            return points
        elif signal_type == 'sell' and rsi_percentile > 75:
            return points
        return 0
    
    def _check_macd_reversal(self, row: pd.Series, signal_type: str, points: int) -> int:
        """Check if MACD histogram shows reversal signal."""
        macd_hist = row.get('macd_histogram', np.nan)
        if pd.isna(macd_hist):
            return 0
        
        # For buy: histogram turning positive (reversal from negative)
        # For sell: histogram turning negative (reversal from positive)
        # This is a simplified check - could be enhanced with previous value comparison
        if signal_type == 'buy' and macd_hist > 0:
            return points
        elif signal_type == 'sell' and macd_hist < 0:
            return points
        return 0
    
    def _check_adx_very_strong(self, row: pd.Series, points: int) -> int:
        """Check if ADX is very strong (>= 30)."""
        adx = row.get('adx', np.nan)
        if pd.isna(adx):
            return 0
        
        if adx >= 30:
            return points
        return 0
    
    def _check_di_alignment(self, row: pd.Series, signal_type: str, points: int) -> int:
        """Check DI alignment: DI+ > DI- for buy, DI- > DI+ for sell."""
        di_plus = row.get('di_plus', np.nan)
        di_minus = row.get('di_minus', np.nan)
        
        if pd.isna(di_plus) or pd.isna(di_minus):
            return 0
        
        if signal_type == 'buy' and di_plus > di_minus:
            return points
        elif signal_type == 'sell' and di_minus > di_plus:
            return points
        return 0
    
    def _check_macd_histogram_aligned(self, row: pd.Series, signal_type: str, points: int) -> int:
        """Check if MACD histogram is aligned with signal direction."""
        macd_hist = row.get('macd_histogram', np.nan)
        if pd.isna(macd_hist):
            return 0
        
        # Positive histogram for buy, negative for sell
        if signal_type == 'buy' and macd_hist > 0:
            return points
        elif signal_type == 'sell' and macd_hist < 0:
            return points
        return 0
    
    def _check_both_indicators_exhausted(self, row: pd.Series, points: int) -> int:
        """
        Check if both MACD and RSI exhaustion conditions are met.
        
        Args:
            row: DataFrame row with indicator values
            points: Points to award if both indicators exhausted
        
        Returns:
            Points if both indicators exhausted, 0 otherwise
        """
        # Check if both MACD and RSI exhaustion flags are set
        macd_buy = row.get('_exhaustion_macd_buy', False)
        macd_sell = row.get('_exhaustion_macd_sell', False)
        rsi_buy = row.get('_exhaustion_rsi_buy', False)
        rsi_sell = row.get('_exhaustion_rsi_sell', False)
        
        # Both indicators must be exhausted (either both buy or both sell)
        macd_exhausted = macd_buy or macd_sell
        rsi_exhausted = rsi_buy or rsi_sell
        
        if macd_exhausted and rsi_exhausted:
            return points
        return 0
    
    def _check_ema_aligned(self, row: pd.Series, signal_type: str, ema_col: str, points: int) -> int:
        """Check if price is above/below EMA aligned with signal direction."""
        price = row.get('close', np.nan)
        ema = row.get(ema_col, np.nan)
        
        if pd.isna(price) or pd.isna(ema):
            return 0
        
        # Price above EMA for buy, below for sell
        if signal_type == 'buy' and price > ema:
            return points
        elif signal_type == 'sell' and price < ema:
            return points
        return 0
    
    def _get_indicator_weight(self, indicator_name: str) -> float:
        """Get weight for indicator in alignment calculation."""
        # Map indicator names to weight keys
        weight_map = {
            'rsi_aligned': 'rsi',
            'rsi_percentile_aligned': 'rsi',
            'stochastic_aligned': 'stochastic',
            'cci_aligned': 'cci',
            'adx_strong': 'adx',
            'adx_very_strong': 'adx',
            'di_alignment': 'adx',
            'bollinger_aligned': 'bollinger',
            'bollinger_extreme': 'bollinger',
            'correlation_high': 'correlation',
            'cointegration': 'cointegration',
            'macd_reversal': 'macd',
            'macd_histogram_aligned': 'macd',
            'ema_50_aligned': 'bollinger',  # Use bollinger weight for EMA
            'ema_100_aligned': 'bollinger',
            'ema_200_aligned': 'bollinger'
        }
        
        weight_key = weight_map.get(indicator_name, 'rsi')  # Default weight
        return self.alignment_weights.get(weight_key, 1.0)
    
    def _calculate_alignment_score(self, alignment_contributions: List[float], total_indicators: int) -> float:
        """
        Calculate weighted alignment score (0-100).
        
        Args:
            alignment_contributions: List of weights for aligned indicators
            total_indicators: Total number of indicators checked
        
        Returns:
            Alignment score from 0-100
        """
        if total_indicators == 0:
            return 0.0
        
        # Sum of weights for aligned indicators
        aligned_weight_sum = sum(alignment_contributions)
        
        # Total possible weight sum (all indicators aligned)
        # Sum only numeric values (exclude 'comment' and other non-numeric keys)
        numeric_weights = {k: v for k, v in self.alignment_weights.items() 
                          if isinstance(v, (int, float)) and k != 'comment'}
        total_possible_weight = sum(numeric_weights.values()) if numeric_weights else total_indicators
        
        # Calculate percentage
        if total_possible_weight > 0:
            score = (aligned_weight_sum / total_possible_weight) * 100
        else:
            score = (len(alignment_contributions) / total_indicators) * 100
        
        return round(score, 1)
    
    def calculate_tenor_liquidity_bonus(
        self,
        signal: Dict,
        data_date: datetime = None
    ) -> Dict[str, any]:
        """
        Calculate tenor and liquidity bonus points for a signal.
        
        Args:
            signal: Signal dictionary with symbol, row_data, etc.
            data_date: Data date for tenor calculation (if None, uses current date)
        
        Returns:
            Dictionary with:
                - breakdown: Dict of bonus type -> points (e.g., {'TNR': 3, 'LIQ': 5})
                - total_bonus: Total bonus points (capped at max_bonus)
        """
        from datetime import datetime, timedelta
        import re
        
        if not data_date:
            data_date = datetime.now()
        
        # Parse data_date if needed
        if isinstance(data_date, str):
            data_date = datetime.strptime(data_date.split()[0], '%Y-%m-%d')
        elif hasattr(data_date, 'to_pydatetime'):
            data_date = data_date.to_pydatetime()
        
        # Get config
        tenor_config = self.config.get('tenor_liquidity_bonus', {})
        if not tenor_config:
            return {'breakdown': {}, 'total_bonus': 0}
        
        bonus_config = tenor_config.get('bonus_points', {})
        liquidity_tiers = tenor_config.get('liquidity_tiers', {})
        tier_1 = set(liquidity_tiers.get('tier_1', []))
        
        # Tenor months: 2-6 months ahead from data_date
        tenor_months = tenor_config.get('tenor_months', [2, 3, 4, 5, 6])
        data_month = data_date.month
        data_year = data_date.year
        
        # Calculate tenor month range
        tenor_start_month = (data_month - 1 + min(tenor_months)) % 12 + 1
        tenor_start_year = data_year + ((data_month - 1 + min(tenor_months)) // 12)
        tenor_end_month = (data_month - 1 + max(tenor_months)) % 12 + 1
        tenor_end_year = data_year + ((data_month - 1 + max(tenor_months)) // 12)
        
        breakdown = {}
        total_bonus = 0
        
        # Extract symbol and metadata
        symbol = signal.get('symbol', '')
        row_data = signal.get('row_data', {})
        
        # Check if spread or outright
        is_spread = not row_data.get('is_outright', True)
        
        if is_spread:
            # Spread: check both legs
            symbol_1 = row_data.get('symbol_1', '')
            symbol_2 = row_data.get('symbol_2', '')
            meta_1 = row_data.get('meta_1', {})
            meta_2 = row_data.get('meta_2', {})
            
            # Check leg 1
            leg1_in_tenor = self._is_contract_in_tenor(symbol_1, meta_1, data_date, tenor_months)
            leg1_tier1 = self._is_tier1_liquid(symbol_1, tier_1)
            
            # Check leg 2
            leg2_in_tenor = self._is_contract_in_tenor(symbol_2, meta_2, data_date, tenor_months)
            leg2_tier1 = self._is_tier1_liquid(symbol_2, tier_1)
            
            # Calculate bonuses
            if leg1_in_tenor and leg2_in_tenor:
                breakdown['TNR'] = bonus_config.get('both_legs_in_tenor', 5)
            elif leg1_in_tenor or leg2_in_tenor:
                breakdown['TNR'] = bonus_config.get('one_leg_in_tenor', 3)
            
            if leg1_tier1 and leg2_tier1:
                liq_bonus = bonus_config.get('both_legs_tier1', 5)
                # Check if also in tenor for combo bonus
                if (leg1_tier1 and leg1_in_tenor) and (leg2_tier1 and leg2_in_tenor):
                    liq_bonus += bonus_config.get('tier1_in_tenor_both_legs', 3)
                elif (leg1_tier1 and leg1_in_tenor) or (leg2_tier1 and leg2_in_tenor):
                    liq_bonus += bonus_config.get('tier1_in_tenor_one_leg', 2)
                breakdown['LIQ'] = liq_bonus
            elif leg1_tier1 or leg2_tier1:
                liq_bonus = bonus_config.get('one_leg_tier1', 3)
                # Check if also in tenor for combo bonus
                if (leg1_tier1 and leg1_in_tenor) or (leg2_tier1 and leg2_in_tenor):
                    liq_bonus += bonus_config.get('tier1_in_tenor_one_leg', 2)
                breakdown['LIQ'] = liq_bonus
        else:
            # Outright: check single symbol
            in_tenor = self._is_contract_in_tenor(symbol, row_data, data_date, tenor_months)
            is_tier1 = self._is_tier1_liquid(symbol, tier_1)
            
            if in_tenor:
                breakdown['TNR'] = bonus_config.get('one_leg_in_tenor', 3)
            
            if is_tier1:
                liq_bonus = bonus_config.get('one_leg_tier1', 3)
                if in_tenor:
                    liq_bonus += bonus_config.get('tier1_in_tenor_one_leg', 2)
                breakdown['LIQ'] = liq_bonus
        
        # Check PRWK bonus (separate from tenor/liquidity, stacks on top)
        was_active_prior_week = signal.get('was_active_prior_week', False)
        if was_active_prior_week:
            prwk_bonus = bonus_config.get('prior_week_active', 5)
            breakdown['PRWK'] = prwk_bonus
        
        # Calculate total and cap tenor/liquidity at max (PRWK is separate)
        # Separate PRWK from other bonuses for capping
        tenor_liq_breakdown = {k: v for k, v in breakdown.items() if k != 'PRWK'}
        prwk_bonus_value = breakdown.get('PRWK', 0)
        
        tenor_liq_total = sum(tenor_liq_breakdown.values())
        max_bonus = bonus_config.get('max_bonus', 10)
        
        if tenor_liq_total > max_bonus:
            # Scale down proportionally if over max (only tenor/liquidity, not PRWK)
            scale_factor = max_bonus / tenor_liq_total
            scaled_tenor_liq = {k: round(v * scale_factor) for k, v in tenor_liq_breakdown.items()}
            breakdown = {**scaled_tenor_liq}
            if prwk_bonus_value > 0:
                breakdown['PRWK'] = prwk_bonus_value
            total_bonus = max_bonus + prwk_bonus_value
        else:
            total_bonus = tenor_liq_total + prwk_bonus_value
        
        return {
            'breakdown': breakdown,
            'total_bonus': total_bonus
        }
    
    def _is_contract_in_tenor(self, symbol: str, metadata: Dict, data_date: datetime, tenor_months: List[int]) -> bool:
        """Check if contract month is within tenor (2-6 months ahead)."""
        from datetime import datetime
        import re
        
        if not symbol:
            return False
        
        # If metadata is None or empty dict, try to extract from symbol directly
        if not metadata:
            metadata = {}
        
        # Parse data_date
        if isinstance(data_date, str):
            data_date = datetime.strptime(data_date.split()[0], '%Y-%m-%d')
        elif hasattr(data_date, 'to_pydatetime'):
            data_date = data_date.to_pydatetime()
        
        data_month = data_date.month
        data_year = data_date.year
        
        # Check if quarterly
        quarter_numb = metadata.get('quarter_numb', 'N')
        if quarter_numb == 'Y':
            # Quarterly: use quarter start month
            quarter_pos = metadata.get('quarter_pos', 'n/a')
            component_months = metadata.get('component_months_names', '')
            
            if component_months and component_months != 'n/a':
                # Get first month from component months (e.g., "JAN,FEB,MAR" -> "JAN")
                first_month = component_months.split(',')[0].strip()
                month_map = {
                    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                    'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
                }
                contract_month = month_map.get(first_month.upper(), None)
                if contract_month:
                    # Check both same year and next year possibilities (quarter start month)
                    # For 2-6 months ahead from data_date, quarter could be in same year or next year
                    
                    # Option 1: Same year quarter
                    if contract_month > data_month:
                        months_ahead_same = contract_month - data_month
                        if months_ahead_same in tenor_months:
                            return True
                    
                    # Option 2: Next year quarter (for quarters that wrap around)
                    months_ahead_next = (12 - data_month) + contract_month
                    if months_ahead_next in tenor_months:
                        return True
                    
                    return False
            return False
        else:
            # Monthly: extract month code from symbol
            month_code_map = {
                'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
                'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
            }
            
            # Extract month code (e.g., '%AFE F!-IEU' -> 'F')
            month_match = re.search(r'%[A-Z]+\s+([FGHJKMNQUVXZ])!', symbol)
            if month_match:
                month_code = month_match.group(1)
                contract_month = month_code_map.get(month_code)
                if contract_month:
                    # Check both same year and next year possibilities
                    # For 2-6 months ahead from data_date, contract could be in same year or next year
                    
                    # Option 1: Same year contract
                    if contract_month > data_month:
                        months_ahead_same = contract_month - data_month
                        if months_ahead_same in tenor_months:
                            return True
                    
                    # Option 2: Next year contract (for months that wrap around)
                    # Example: data_date = Nov 2024 (month 11), contract = Jan (month 1)
                    # months_ahead = (12 - 11) + 1 = 2 months (Jan 2025 is 2 months from Nov 2024)
                    months_ahead_next = (12 - data_month) + contract_month
                    if months_ahead_next in tenor_months:
                        return True
                    
                    return False
        
        return False
    
    def _is_tier1_liquid(self, symbol: str, tier_1: set) -> bool:
        """Check if symbol root is in Tier 1 (most liquid)."""
        if not symbol:
            return False
        
        # Extract root code (e.g., '%AFE F!-IEU' -> 'AFE')
        root_match = re.search(r'%([A-Z]+)', symbol)
        if root_match:
            root = root_match.group(1)
            return root in tier_1
        
        return False
    
    def calculate_trend_exhaustion_penalty(
        self,
        row: pd.Series,
        strategy_name: str,
        signal_type: str
    ) -> Dict[str, any]:
        """
        Calculate trend exhaustion penalty for trend following signals.
        Penalizes signals when trend appears already extended (late entry).
        
        Args:
            row: DataFrame row with indicator values
            strategy_name: Name of strategy (applies to 'trend_following' and 'enhanced_trend_following')
            signal_type: 'buy' or 'sell'
        
        Returns:
            Dictionary with:
                - breakdown: Dict of penalty type -> points deducted
                - total_penalty: Total penalty points (capped at max_penalty)
        """
        # Only apply to trend following systems
        if strategy_name not in ['trend_following', 'enhanced_trend_following']:
            return {'breakdown': {}, 'total_penalty': 0}
        
        strategy_config = self.config['strategies'].get(strategy_name, {})
        penalty_config = strategy_config.get('trend_exhaustion_penalty', {})
        
        if not penalty_config.get('enabled', True):
            return {'breakdown': {}, 'total_penalty': 0}
        
        penalties = penalty_config.get('penalties', {})
        max_penalty = penalty_config.get('max_penalty', 15)
        
        breakdown = {}
        total_penalty = 0
        
        # 1. RSI Extreme penalty
        if 'rsi_extreme' in penalties:
            rsi_penalty = self._check_rsi_extreme_penalty(row, signal_type, penalties['rsi_extreme'])
            if rsi_penalty > 0:
                breakdown['rsi_extreme'] = rsi_penalty
                total_penalty += rsi_penalty
        
        # 2. Price distance from EMA penalty
        if 'price_distance_from_ema' in penalties:
            ema_penalty = self._check_ema_distance_penalty(row, signal_type, penalties['price_distance_from_ema'])
            if ema_penalty > 0:
                breakdown['price_distance_from_ema'] = ema_penalty
                total_penalty += ema_penalty
        
        # 3. Bollinger Band extreme penalty
        if 'bollinger_extreme' in penalties:
            bb_penalty = self._check_bollinger_extreme_penalty(row, signal_type, penalties['bollinger_extreme'])
            if bb_penalty > 0:
                breakdown['bollinger_extreme'] = bb_penalty
                total_penalty += bb_penalty
        
        # Cap at max_penalty
        if total_penalty > max_penalty:
            total_penalty = max_penalty
            # Scale down breakdown proportionally if needed
            if breakdown:
                scale_factor = max_penalty / sum(breakdown.values())
                breakdown = {k: round(v * scale_factor) for k, v in breakdown.items()}
        
        return {
            'breakdown': breakdown,
            'total_penalty': total_penalty
        }
    
    def _check_rsi_extreme_penalty(self, row: pd.Series, signal_type: str, penalty_config: Dict) -> int:
        """Check if RSI is extreme (overbought for buy, oversold for sell)."""
        rsi = row.get('rsi', np.nan)
        if pd.isna(rsi):
            return 0
        
        buy_threshold = penalty_config.get('buy_threshold', 75)
        sell_threshold = penalty_config.get('sell_threshold', 25)
        points = penalty_config.get('points', 10)
        
        # For buy signals: RSI > 75 means already overbought (trend extended)
        if signal_type == 'buy' and rsi > buy_threshold:
            return points
        
        # For sell signals: RSI < 25 means already oversold (trend extended)
        if signal_type == 'sell' and rsi < sell_threshold:
            return points
        
        return 0
    
    def _check_ema_distance_penalty(self, row: pd.Series, signal_type: str, penalty_config: Dict) -> int:
        """Check if price is far from EMA (late entry)."""
        price = row.get('close', np.nan)
        ema_col = penalty_config.get('ema_column', 'ema_50')
        ema = row.get(ema_col, np.nan)
        distance_percent = penalty_config.get('distance_percent', 5.0)
        points = penalty_config.get('points', 5)
        
        if pd.isna(price) or pd.isna(ema) or ema == 0:
            return 0
        
        # Calculate percentage distance from EMA
        distance_pct = abs((price - ema) / ema) * 100
        
        # For buy signals: price > 5% above EMA means already extended
        if signal_type == 'buy' and price > ema and distance_pct > distance_percent:
            return points
        
        # For sell signals: price < 5% below EMA means already extended
        if signal_type == 'sell' and price < ema and distance_pct > distance_percent:
            return points
        
        return 0
    
    def _check_bollinger_extreme_penalty(self, row: pd.Series, signal_type: str, penalty_config: Dict) -> int:
        """Check if price is at Bollinger Band extreme."""
        price = row.get('close', np.nan)
        bb_upper = row.get('bb_upper', np.nan)
        bb_lower = row.get('bb_lower', np.nan)
        points = penalty_config.get('points', 5)
        
        if pd.isna(price) or pd.isna(bb_upper) or pd.isna(bb_lower):
            return 0
        
        # Check if price is at or very close to band (within 1%)
        tolerance = 0.01
        
        # For buy signals: price at upper band means already extended
        if signal_type == 'buy' and price >= bb_upper * (1 - tolerance):
            return points
        
        # For sell signals: price at lower band means already extended
        if signal_type == 'sell' and price <= bb_lower * (1 + tolerance):
            return points
        
        return 0


