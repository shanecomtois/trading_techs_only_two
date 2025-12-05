"""
ICE Chat message formatter for trade signals.
Formats signals into ICE Chat format with delta-sized quantities for spreads.
"""
import pandas as pd
import numpy as np
import re
from pathlib import Path
from typing import Dict, Optional, Tuple
import logging
from datetime import datetime

# Import curve loader function at module level
try:
    from data_loaders.curve_loader import get_leg_price_from_curve
except ImportError:
    # Fallback if import fails (e.g., during testing)
    get_leg_price_from_curve = None
    logger.warning("Could not import get_leg_price_from_curve from curve_loader. Delta sizing may use fallback prices.")

logger = logging.getLogger(__name__)

# Month code to name mapping (for display)
MONTH_MAP = {
    'F': 'Jan', 'G': 'Feb', 'H': 'Mar', 'J': 'Apr',
    'K': 'May', 'M': 'Jun', 'N': 'Jul', 'Q': 'Aug',
    'U': 'Sep', 'V': 'Oct', 'X': 'Nov', 'Z': 'Dec'
}

# Quarter position to quarter name
QUARTER_MAP = {
    '1': 'Q1',
    '2': 'Q2',
    '3': 'Q3',
    '4': 'Q4'
}

# Product root to display name mapping
PRODUCT_NAME_MAP = {
    "PRL": "MB LST Propane",
    "PRN": "MB NTET Propane",
    "PRC": "Conway Propane",
    "AFE": "Far East Propane",
    "NBI": "MB NTET Normal Butane",
    "NBR": "MB LST Normal Butane",
    "IBC": "Conway Normal Butane",
    "ABF": "Far East Butane",
    "ISO": "MB NTET Iso Butane",
    "ISL": "MB LST Iso Butane",
    "ISC": "Conway Iso Butane",
    "NGE": "MB NTET Natural Gasoline (C5)",
    "NGL": "MB LST Natural Gasoline (C5)",
    "NGC": "Conway Natural Gasoline (C5)",
    "ETE": "Ethane",
    "ETH": "Ethane",
    "CL": "WTI Crude",
    "XRB": "RBOB",
    "HO": "Heating Oil",
    "NG": "Henry Hub Natural Gas",
    "BRENT": "Brent Crude",
}


class ICEChatFormatter:
    """
    Formats trade signals into ICE Chat messages.
    """
    
    def __init__(self, config: dict, symbol_matrix_path: str = None, curve_data: dict = None, prepared_df: pd.DataFrame = None, data_date: str = None):
        """
        Initialize ICE Chat formatter.
        
        Args:
            config: Configuration dictionary
            symbol_matrix_path: Path to symbol_matrix.csv (default: lists_and_matrix/symbol_matrix.csv)
            curve_data: Curve price data from CurveBuilder (for delta sizing)
            prepared_df: Prepared DataFrame with close prices (fallback for price lookup)
        """
        self.config = config
        self.ice_chat_config = config.get('ice_chat', {})
        self.quantities = self.ice_chat_config.get('quantities', {})
        
        # Load position sizing config for base volumes
        position_sizing = config.get('position_sizing', {})
        self.base_volume_outright = position_sizing.get('base_volume_outright', 10)
        self.base_volume_spread_per_side = position_sizing.get('base_volume_spread_per_side', 10)
        self.max_volume = position_sizing.get('max_volume', 30)
        self.min_volume = position_sizing.get('min_volume', 10)
        # Multiplier for positions at 100% pos% (allows sizing up "normal" positions)
        self.base_multiplier_at_100pct = position_sizing.get('base_multiplier_at_100pct', 1.0)
        
        # Load minimum volumes
        min_volumes = position_sizing.get('min_volumes', {})
        self.min_volumes = min_volumes.copy()
        self.min_volumes.pop('comment', None)  # Remove comment if present
        
        # Store curve data and prepared_df for delta sizing
        self.curve_data = curve_data or {}
        self.prepared_df = prepared_df
        self.data_date = data_date  # Store data_date for year inference
        
        # Load symbol matrix
        if symbol_matrix_path is None:
            # Default to parent directory
            symbol_matrix_path = Path(__file__).parent.parent.parent / 'lists_and_matrix' / 'symbol_matrix.csv'
        
        self.symbol_matrix_path = Path(symbol_matrix_path)
        self.symbol_matrix = None
        self._load_symbol_matrix()
    
    def _load_symbol_matrix(self):
        """Load symbol matrix CSV."""
        try:
            if self.symbol_matrix_path.exists():
                self.symbol_matrix = pd.read_csv(self.symbol_matrix_path, low_memory=False)
                logger.info(f"Loaded symbol matrix: {len(self.symbol_matrix)} rows")
            else:
                logger.warning(f"Symbol matrix not found: {self.symbol_matrix_path}")
                self.symbol_matrix = pd.DataFrame()
        except Exception as e:
            logger.error(f"Error loading symbol matrix: {e}")
            self.symbol_matrix = pd.DataFrame()
    
    def _get_symbol_metadata(self, symbol: str) -> Dict:
        """Get metadata for a symbol from symbol matrix."""
        if self.symbol_matrix is None or len(self.symbol_matrix) == 0:
            return {}
        
        # Try exact match first
        match = self.symbol_matrix[self.symbol_matrix['ice_symbol'] == symbol]
        if len(match) > 0:
            row = match.iloc[0]
            return {
                'product': row.get('product', ''),
                'location': row.get('location', ''),
                'molecule': row.get('molecule', ''),
                'quarter_numb': row.get('quarter_numb', 'N'),
                'quarter_pos': row.get('quarter_pos', 'n/a'),
                'component_months_names': row.get('component_months_names', ''),
                'is_spread': row.get('spread_type', 'outright') == 'spread',
                'symbol_1': row.get('symbol_1', ''),
                'symbol_2': row.get('symbol_2', ''),
                'product_2': row.get('product_2', ''),
                'location_2': row.get('location_2', ''),
                'molecule_2': row.get('molecule_2', ''),
                'quarter_numb_2': row.get('quarter_numb_2', 'N'),
                'quarter_pos_2': row.get('quarter_pos_2', 'n/a'),
                'component_months_names_2': row.get('component_months_names_2', ''),
                'convert_to_$usg': row.get('convert_to_$usg', 'n/a'),
                'native_uom': row.get('native_uom', '')
            }
        
        return {}
    
    def _extract_month_code(self, symbol: str) -> Optional[str]:
        """Extract month code from symbol (e.g., '%PRL F!-IEU' -> 'F')."""
        if symbol.startswith('='):
            return None  # Formula, not a single month
        
        match = re.search(r'%[A-Z]+ ([FGHJKMNQUVXZ])!', symbol)
        return match.group(1) if match else None
    
    def _format_date(self, symbol: str, metadata: Dict) -> str:
        """Format date string for symbol (month or quarter)."""
        from datetime import datetime
        
        quarter_numb = metadata.get('quarter_numb', 'N')
        quarter_pos = metadata.get('quarter_pos', 'n/a')
        component_months_names = metadata.get('component_months_names', '')
        
        # Infer year from data_date (if available) or current date (fallback)
        # This ensures historical analysis uses the correct reference date
        if self.data_date:
            try:
                # Parse data_date if it's a string
                if isinstance(self.data_date, str):
                    date_obj = datetime.strptime(self.data_date.split()[0], '%Y-%m-%d')
                elif isinstance(self.data_date, datetime):
                    date_obj = self.data_date
                else:
                    # Try pandas Timestamp conversion
                    import pandas as pd
                    if isinstance(self.data_date, pd.Timestamp):
                        date_obj = self.data_date.to_pydatetime()
                    else:
                        date_obj = pd.to_datetime(self.data_date).to_pydatetime()
                reference_year = date_obj.year
            except Exception as e:
                logger.warning(f"Could not parse data_date '{self.data_date}', using current date: {e}")
                reference_year = datetime.now().year
        else:
            # Fallback to current date if data_date not provided
            reference_year = datetime.now().year
        
        # For months F-J (Jan-Oct), contracts are typically next year from reference date
        # For months X-Z (Nov-Dec), contracts might be current year, but we'll use next year for consistency
        inferred_year = reference_year + 1
        year_short = str(inferred_year)[-2:]
        
        # Check if this is a quarterly contract
        if quarter_numb == 'Y' and quarter_pos and quarter_pos != 'n/a' and quarter_pos != '':
            # Quarterly - normalize quarter_pos (strip any 'Q' from start or end, extract numeric part)
            quarter_pos_str = str(quarter_pos).strip().upper()
            # Remove 'Q' from both ends and extract just the number
            quarter_pos_clean = quarter_pos_str.lstrip('Q').rstrip('Q').strip()
            # Extract just the numeric part (in case there are other characters)
            import re
            quarter_num_match = re.search(r'(\d+)', quarter_pos_clean)
            if quarter_num_match:
                quarter_pos_clean = quarter_num_match.group(1)
            quarter_name = QUARTER_MAP.get(quarter_pos_clean, f'Q{quarter_pos_clean}')
            return f"{quarter_name} '{year_short}"
        elif symbol.startswith('=') and component_months_names:
            # Quarterly formula - try to infer quarter from component months
            # Component months like "JAN,FEB,MAR" = Q1, "APR,MAY,JUN" = Q2, etc.
            months_list = [m.strip() for m in component_months_names.split(',') if m.strip()]
            if len(months_list) >= 3:
                first_month = months_list[0]
                # Map first month to quarter
                month_to_quarter = {
                    'JAN': '1', 'FEB': '1', 'MAR': '1',
                    'APR': '2', 'MAY': '2', 'JUN': '2',
                    'JUL': '3', 'AUG': '3', 'SEP': '3',
                    'OCT': '4', 'NOV': '4', 'DEC': '4'
                }
                quarter_num = month_to_quarter.get(first_month, '1')
                quarter_name = QUARTER_MAP.get(quarter_num, f'Q{quarter_num}')
                return f"{quarter_name} '{year_short}"
        else:
            # Monthly - extract month code
            month_code = self._extract_month_code(symbol)
            if month_code and month_code in MONTH_MAP:
                month_name = MONTH_MAP[month_code]
                return f"{month_name} '{year_short}"
        
        return f"'{year_short}"  # Fallback
    
    def _get_product_display_name(self, symbol_root: str, location: str = '', molecule: str = '') -> str:
        """Get display name for product based on symbol root, location, and molecule."""
        root_upper = symbol_root.upper().lstrip('%')
        
        # Check product name map first (this should handle most cases)
        if root_upper in PRODUCT_NAME_MAP:
            return PRODUCT_NAME_MAP[root_upper]
        
        # For products not in map, try to build a reasonable name
        # But prefer shorter, cleaner names over full location+molecule strings
        if molecule:
            # Use molecule if available (e.g., "Propane", "Butane")
            if location and location not in ['n/a', '']:
                # Only add location if it's a meaningful code (not full location string)
                location_upper = location.upper()
                # If location is a short code (like "NTET", "LST"), use it
                if len(location_upper) <= 5 and location_upper.isalpha():
                    return f"{molecule} ({location_upper})"
                # Otherwise just use molecule
            return molecule
        elif location and location not in ['n/a', '']:
            # Use location only if it's a short code
            location_upper = location.upper()
            if len(location_upper) <= 5 and location_upper.isalpha():
                return location_upper
            # For long location strings, try to extract meaningful part
            parts = location.split()
            if parts:
                # Use last part if it looks like a code
                last_part = parts[-1]
                if last_part.isupper() and len(last_part) <= 5:
                    return last_part
        
        # Fallback to symbol root
        return symbol_root if symbol_root else 'Unknown'
    
    def _get_location_code(self, location: str) -> str:
        """
        Extract location code from location string (e.g., 'MT BELVIEU NTET' -> 'NTET').
        Excludes specific codes that should not appear in ICE Chat messages: NYH, C, EAST.
        """
        if not location:
            return ''
        
        # Try to extract code (usually last word or abbreviation)
        parts = location.split()
        if len(parts) > 0:
            # Check if last part looks like a code (all caps, short)
            last_part = parts[-1]
            if last_part.isupper() and len(last_part) <= 5:
                # Exclude specific location codes that should not appear in ICE Chat
                excluded_codes = {'NYH', 'C', 'EAST'}
                if last_part not in excluded_codes:
                    return last_part
        
        # Fallback: use first letters or abbreviation (but still exclude if it matches excluded codes)
        code = ''.join([p[0] for p in parts if p]).upper()[:4]
        excluded_codes = {'NYH', 'C', 'EAST'}
        if code not in excluded_codes:
            return code
        
        return ''  # Return empty if it matches excluded codes
    
    def format_ice_chat_message(self, signal: Dict) -> str:
        """
        Format signal into ICE Chat message.
        
        Args:
            signal: Signal dictionary with symbol, signal_type, etc.
        
        Returns:
            Formatted ICE Chat message string
        """
        symbol = signal.get('symbol', '')
        signal_type = signal.get('signal_type', 'buy')
        metadata = self._get_symbol_metadata(symbol)
        
        is_spread = metadata.get('is_spread', False)
        
        if is_spread:
            return self._format_spread_message(signal, metadata)
        else:
            return self._format_outright_message(signal, metadata)
    
    def _format_outright_message(self, signal: Dict, metadata: Dict) -> str:
        """Format ICE Chat message for outright."""
        symbol = signal.get('symbol', '')
        signal_type = signal.get('signal_type', 'buy')
        pos_pct = signal.get('pos_pct', 0)
        
        # Extract symbol root
        symbol_root = symbol.lstrip('%').split()[0] if symbol else ''
        
        # Get product display name
        product_name = self._get_product_display_name(
            symbol_root,
            metadata.get('location', ''),
            metadata.get('molecule', '')
        )
        
        location_code = self._get_location_code(metadata.get('location', ''))
        date_str = self._format_date(symbol, metadata)
        
        # Calculate quantity based on position size (outright, not spread)
        quantity_str = self._calculate_quantity(
            pos_pct, 
            is_quarterly=(metadata.get('quarter_numb') == 'Y'),
            is_spread=False,
            symbol_root=symbol_root
        )
        quantity = quantity_str  # Keep as string for formatting
        
        action = 'buy' if signal_type == 'buy' else 'sell'
        
        # Format: "ICE Chat: What can I buy/sell [quantity] [product_name] ([location_code]) in [date]?"
        # Or: "ICE Chat: What can I buy/sell [quantity] [product_name] in [date]?" if no location code
        if location_code:
            message = f"ICE Chat: What can I {action} {quantity} {product_name} ({location_code}) in {date_str}?"
        else:
            message = f"ICE Chat: What can I {action} {quantity} {product_name} in {date_str}?"
        
        return message.strip()
    
    def _calculate_quantity(self, pos_pct: float, is_quarterly: bool = False, is_spread: bool = False, symbol_root: str = '') -> str:
        """
        Calculate quantity based on position size percentage.
        
        Uses base volume and scales by pos% multiplier:
        - Base volume: 10kb for outrights, 10kb per side for spreads
        - Multiplier: pos% / 100 (e.g., pos% = 200% → multiplier = 2.0)
        - Final: base_volume × multiplier, capped between min_volume and max_volume
        
        Args:
            pos_pct: Position size percentage (from volatility calculation)
            is_quarterly: Whether this is a quarterly contract
            is_spread: Whether this is a spread (uses per-side base volume)
        
        Returns:
            Formatted quantity string (e.g., "10kb" or "20kb per mo")
        """
        try:
            # Convert pos% to multiplier (pos% = 200% → multiplier = 2.0)
            pct = float(pos_pct) if not pd.isna(pos_pct) else 100.0
            multiplier = pct / 100.0
            
            # Apply base_multiplier_at_100pct when pos% >= 100%
            # This allows sizing up "normal" positions (100% pos%) without affecting lower pos% values
            if pct >= 100.0 and self.base_multiplier_at_100pct != 1.0:
                # Apply multiplier: 100% = base_multiplier, 200% = 2.0 × base_multiplier, etc.
                multiplier = multiplier * self.base_multiplier_at_100pct
            
            # Select base volume based on instrument type
            if is_spread:
                base_vol = self.base_volume_spread_per_side
            else:
                base_vol = self.base_volume_outright
            
            # Calculate scaled volume BEFORE applying min/max caps
            scaled_vol = base_vol * multiplier
            
            # Apply min/max caps AFTER scaling
            # Note: min_volume cap can override pos% scaling (e.g., 10% pos% → 1kb, but min is 10kb)
            final_vol_capped = max(self.min_volume, min(self.max_volume, scaled_vol))
            
            # Round to appropriate increment (AFE=13kb increments, others=1kb increments)
            if symbol_root:
                final_vol = self._round_to_increment(final_vol_capped, symbol_root)
            else:
                # Fallback: round to nearest integer if symbol_root not provided
                final_vol = round(final_vol_capped)
            
            # Log if pos% scaling was overridden by min_volume
            if scaled_vol < self.min_volume and pct < 100.0:
                logger.debug(f"Pos% scaling ({pct}% → {scaled_vol:.1f}kb) overridden by min_volume ({self.min_volume}kb) → {final_vol}kb")
            
        except (ValueError, TypeError):
            # Fallback to base volume on error
            if is_spread:
                final_vol = self.base_volume_spread_per_side
            else:
                final_vol = self.base_volume_outright
        
        # Format with "per mo" suffix for quarterlies
        if is_quarterly:
            return f"{final_vol}kb per mo"
        return f"{final_vol}kb"
    
    def _get_min_volume(self, symbol_root: str) -> float:
        """Get minimum volume for a symbol root."""
        return self.min_volumes.get(symbol_root.upper(), self.min_volumes.get('default', self.min_volume))
    
    def _round_to_increment(self, quantity: float, symbol_root: str) -> int:
        """
        Round quantity to appropriate increment based on symbol.
        
        Rules:
        - ABF: Must be in 11kb increments (11, 22, 33, 44, etc.) - no 1kb increments
        - AFE: Must be in 13kb increments (13, 26, 39, 52, etc.) - no 1kb increments
        - All others: Minimum 10kb, then 1kb increments (10, 11, 12, 13, 14, etc.)
        
        Args:
            quantity: Quantity to round
            symbol_root: Symbol root code (e.g., 'ABF', 'AFE', 'CL', 'PRL')
        
        Returns:
            Rounded quantity as integer
        """
        symbol_root_upper = symbol_root.upper()
        
        if symbol_root_upper == 'ABF':
            # ABF: Round to nearest 11kb increment (11, 22, 33, 44, etc.)
            # Ensure at least 11kb
            if quantity < 11:
                return 11
            return round(quantity / 11) * 11
        elif symbol_root_upper == 'AFE':
            # AFE: Round to nearest 13kb increment (13, 26, 39, 52, etc.)
            # Ensure at least 13kb
            if quantity < 13:
                return 13
            return round(quantity / 13) * 13
        else:
            # All others: Round to nearest 1kb increment, ensure minimum 10kb
            min_volume = self._get_min_volume(symbol_root)
            rounded = round(quantity)
            return max(int(min_volume), rounded)
    
    def _round_quarterly_total(self, total_quantity: float, symbol_root: str) -> int:
        """
        Round quarterly total volume to appropriate increment.
        
        Rules:
        - ABF quarterly: Total must be multiple of 33kb (11kb × 3)
          Valid totals: 33kb, 66kb, 99kb, 132kb...
          Valid "per mo": 11kb, 22kb, 33kb, 44kb...
        - AFE quarterly: Total must be multiple of 39kb (13kb × 3)
          Valid totals: 39kb, 78kb, 117kb, 156kb...
          Valid "per mo": 13kb, 26kb, 39kb, 52kb...
        - Other quarterlies: Total must be multiple of 3kb (1kb × 3)
          Valid totals: 30kb, 33kb, 36kb, 39kb...
          Valid "per mo": 10kb, 11kb, 12kb, 13kb...
        
        Args:
            total_quantity: Total quarterly volume to round
            symbol_root: Symbol root code (e.g., 'ABF', 'AFE', 'CL', 'PRL')
        
        Returns:
            Rounded total volume as integer
        """
        symbol_root_upper = symbol_root.upper()
        
        if symbol_root_upper == 'ABF':
            # ABF quarterly: Round to nearest multiple of 33kb (11kb × 3)
            if total_quantity < 33:
                return 33  # Minimum for ABF quarterly
            return round(total_quantity / 33) * 33
        elif symbol_root_upper == 'AFE':
            # AFE quarterly: Round to nearest multiple of 39kb (13kb × 3)
            if total_quantity < 39:
                return 39  # Minimum for AFE quarterly
            return round(total_quantity / 39) * 39
        else:
            # Other quarterlies: Round to nearest multiple of 3kb (1kb × 3)
            if total_quantity < 30:
                return 30  # Minimum 10kb per mo × 3 = 30kb total
            return round(total_quantity / 3) * 3
    
    def _get_leg_price(self, symbol: str, metadata: Dict) -> Optional[float]:
        """
        Get price for a leg from curve data or prepared_df, converted to $/usg.
        
        Args:
            symbol: ICE symbol (e.g., '%AFE F!-IEU', '%CL V!', or quarterly formula)
            metadata: Symbol metadata from symbol matrix
        
        Returns:
            Price in $/usg (dollars per US gallon), or None if unavailable
        """
        # Use imported function (imported at module level)
        if get_leg_price_from_curve is None:
            logger.debug(f"_get_leg_price: get_leg_price_from_curve is None")
            return None
        
        # Try curve data first
        if self.curve_data:
            # Extract root code
            root_match = re.search(r'%([A-Z]+)', symbol)
            if root_match:
                root_code = root_match.group(1).upper()
                
                # For quarterlies, average component month prices
                if metadata.get('quarter_numb') == 'Y':
                    component_symbols = metadata.get('component_symbols', '')
                    if component_symbols and component_symbols != 'n/a':
                        # Parse component symbols (e.g., '%PRN J!-IEU,%PRN K!-IEU,%PRN M!-IEU')
                        comp_symbols = [s.strip() for s in str(component_symbols).split(',') if s.strip().startswith('%')]
                        prices = []
                        for comp_sym in comp_symbols[:3]:  # Take first 3 months
                            # Get price and apply conversion for each component
                            price = get_leg_price_from_curve(comp_sym, self.curve_data)
                            if price is not None:
                                # Get metadata for component symbol to apply conversion
                                comp_meta = self._get_symbol_metadata(comp_sym)
                                convert_factor = comp_meta.get('convert_to_$usg', metadata.get('convert_to_$usg', 'n/a'))
                                if convert_factor and convert_factor != 'n/a' and convert_factor != '':
                                    try:
                                        # Parse conversion factor (e.g., "/521" or "/42" or already a float like 521.0)
                                        if isinstance(convert_factor, str) and convert_factor.startswith('/'):
                                            divisor = float(convert_factor[1:])
                                            price = price / divisor
                                        elif isinstance(convert_factor, (int, float)) and convert_factor > 0:
                                            # Already a numeric divisor
                                            price = price / float(convert_factor)
                                    except (ValueError, TypeError) as e:
                                        logger.warning(f"_get_leg_price: Error parsing conversion factor {convert_factor} for {comp_sym}: {e}")
                                prices.append(price)
                        if prices:
                            avg_price = sum(prices) / len(prices)
                            logger.debug(f"_get_leg_price: Quarterly {symbol} = {avg_price:.4f} $/usg (from {len(prices)} components, all converted to $/usg)")
                            return avg_price
                
                # For monthly symbols, direct lookup
                # Try to extract year from data_date or infer from current date
                year = None
                if self.data_date:
                    try:
                        # Parse data_date (format: '2024-01-12' or similar)
                        from datetime import datetime
                        if isinstance(self.data_date, str):
                            date_obj = datetime.strptime(self.data_date.split()[0], '%Y-%m-%d')
                        else:
                            date_obj = self.data_date
                        # For forward contracts, typically next year from data date
                        year = date_obj.year + 1
                    except:
                        pass
                
                price = get_leg_price_from_curve(symbol, self.curve_data, year=year)
                if price is not None:
                    # Apply conversion to $/usg if needed
                    convert_factor = metadata.get('convert_to_$usg', 'n/a')
                    if convert_factor and convert_factor != 'n/a' and convert_factor != '':
                        try:
                            # Parse conversion factor (e.g., "/521" or "/42" or already a float like 521.0)
                            if isinstance(convert_factor, str) and convert_factor.startswith('/'):
                                divisor = float(convert_factor[1:])
                                price_usg = price / divisor
                                logger.debug(f"_get_leg_price: {symbol} = {price_usg:.4f} $/usg (converted from {price:.2f} using {convert_factor})")
                                return price_usg
                            elif isinstance(convert_factor, (int, float)) and convert_factor > 0:
                                # Already a numeric divisor
                                price_usg = price / float(convert_factor)
                                logger.debug(f"_get_leg_price: {symbol} = {price_usg:.4f} $/usg (converted from {price:.2f} using divisor {convert_factor})")
                                return price_usg
                            else:
                                logger.warning(f"_get_leg_price: Unexpected conversion format: {convert_factor} (type: {type(convert_factor)})")
                        except (ValueError, TypeError) as e:
                            logger.warning(f"_get_leg_price: Error parsing conversion factor {convert_factor}: {e}")
                    logger.debug(f"_get_leg_price: {symbol} = {price} $/usg from curve (no conversion needed)")
                    return price
                else:
                    logger.debug(f"_get_leg_price: {symbol} not found in curve_data (root={root_code})")
        
        # Fallback to prepared_df if available - this is critical for delta sizing
        if self.prepared_df is not None and len(self.prepared_df) > 0:
            # Try different possible column names for symbol
            symbol_col = None
            for col in ['ice_connect_symbol', 'Symbol', 'symbol', 'ice_symbol']:
                if col in self.prepared_df.columns:
                    symbol_col = col
                    break
            
            if symbol_col:
                # Try exact match first
                symbol_data = self.prepared_df[self.prepared_df[symbol_col] == symbol]
                if len(symbol_data) > 0:
                    close_price = symbol_data.iloc[0].get('close')
                    if pd.notna(close_price) and close_price > 0:
                        # Apply conversion to $/usg if needed (prepared_df prices are already in native units)
                        convert_factor = metadata.get('convert_to_$usg', 'n/a')
                        if convert_factor and convert_factor != 'n/a' and convert_factor != '' and not pd.isna(convert_factor):
                            try:
                                # Parse conversion factor (e.g., "/521" or "/42" or already a float like 521.0)
                                if isinstance(convert_factor, str) and convert_factor.startswith('/'):
                                    divisor = float(convert_factor[1:])
                                    close_price = close_price / divisor
                                    logger.debug(f"_get_leg_price: {symbol} = {close_price:.4f} $/usg from prepared_df (converted using {convert_factor})")
                                elif isinstance(convert_factor, (int, float)) and not pd.isna(convert_factor) and convert_factor > 0:
                                    # Already a numeric divisor
                                    close_price = close_price / float(convert_factor)
                                    logger.debug(f"_get_leg_price: {symbol} = {close_price:.4f} $/usg from prepared_df (converted using divisor {convert_factor})")
                                else:
                                    # Only warn if it's not NaN (NaN is expected for symbols that don't need conversion)
                                    if not pd.isna(convert_factor):
                                        logger.warning(f"_get_leg_price: Unexpected conversion format: {convert_factor} (type: {type(convert_factor)})")
                            except (ValueError, TypeError) as e:
                                logger.warning(f"_get_leg_price: Error parsing conversion factor {convert_factor}: {e}")
                        else:
                            logger.debug(f"_get_leg_price: {symbol} = {close_price} $/usg from prepared_df (exact match, no conversion)")
                        return float(close_price)
                
                # If no exact match, try to find by root code and month
                # Extract root and month from symbol
                root_match = re.search(r'%([A-Z]+)', symbol)
                month_match = re.search(r'%[A-Z]+\s+([FGHJKMNQUVXZ])!', symbol)
                if root_match and month_match:
                    root_code = root_match.group(1).upper()
                    month_code = month_match.group(1)
                    # Look for symbols that match this root and month
                    # Pattern: %ROOT MONTH! or %ROOT MONTH!-EXCHANGE
                    pattern = rf'%{root_code}\s+{month_code}!'
                    for idx, row in self.prepared_df.iterrows():
                        row_symbol = str(row.get(symbol_col, ''))
                        if re.search(pattern, row_symbol):
                            close_price = row.get('close')
                            if pd.notna(close_price) and close_price > 0:
                                # Apply conversion to $/usg if needed (prepared_df prices are already in native units)
                                convert_factor = metadata.get('convert_to_$usg', 'n/a')
                                if convert_factor and convert_factor != 'n/a' and convert_factor != '' and not pd.isna(convert_factor):
                                    try:
                                        # Parse conversion factor (e.g., "/521" or "/42" or already a float like 521.0)
                                        if isinstance(convert_factor, str) and convert_factor.startswith('/'):
                                            divisor = float(convert_factor[1:])
                                            close_price = close_price / divisor
                                            logger.debug(f"_get_leg_price: {symbol} ≈ {row_symbol} = {close_price:.4f} $/usg from prepared_df (pattern match, converted using {convert_factor})")
                                        elif isinstance(convert_factor, (int, float)) and not pd.isna(convert_factor) and convert_factor > 0:
                                            # Already a numeric divisor
                                            close_price = close_price / float(convert_factor)
                                            logger.debug(f"_get_leg_price: {symbol} ≈ {row_symbol} = {close_price:.4f} $/usg from prepared_df (pattern match, converted using divisor {convert_factor})")
                                        else:
                                            # Only warn if it's not NaN (NaN is expected for symbols that don't need conversion)
                                            if not pd.isna(convert_factor):
                                                logger.warning(f"_get_leg_price: Unexpected conversion format: {convert_factor} (type: {type(convert_factor)})")
                                    except (ValueError, TypeError) as e:
                                        logger.warning(f"_get_leg_price: Error parsing conversion factor {convert_factor}: {e}")
                                else:
                                    logger.debug(f"_get_leg_price: {symbol} ≈ {row_symbol} = {close_price} $/usg from prepared_df (pattern match, no conversion)")
                                return float(close_price)
        
        # Log why price wasn't found
        if not self.curve_data:
            logger.debug(f"_get_leg_price: {symbol} - no curve_data available")
        if self.prepared_df is None or len(self.prepared_df) == 0:
            logger.debug(f"_get_leg_price: {symbol} - no prepared_df available")
        else:
            logger.debug(f"_get_leg_price: {symbol} - not found in prepared_df")
        return None
    
    def _calculate_delta_sized_quantities(
        self,
        pos_pct: float,
        symbol_1: str,
        symbol_2: str,
        meta_1: Dict,
        meta_2: Dict,
        spread_metadata: Dict = None
    ) -> Tuple[str, str]:
        """
        Calculate delta-sized quantities for spread legs.
        
        Args:
            pos_pct: Position size percentage
            symbol_1: First leg symbol
            symbol_2: Second leg symbol
            meta_1: Metadata for first leg
            meta_2: Metadata for second leg
        
        Returns:
            Tuple of (quantity_1, quantity_2) as formatted strings
        """
        # Enrich metadata from spread metadata if provided (for quarterlies)
        # This is critical for quarterly formulas that aren't found in individual symbol lookups
        if spread_metadata:
            if spread_metadata.get('quarter_numb_1'):
                meta_1['quarter_numb'] = spread_metadata.get('quarter_numb_1', 'N')
                meta_1['quarter_pos'] = spread_metadata.get('quarter_pos_1', 'n/a')
                meta_1['component_months_names'] = spread_metadata.get('component_months_names_1', '')
                logger.debug(f"Enriched meta_1 from spread: quarterly={meta_1['quarter_numb']}")
            if spread_metadata.get('quarter_numb_2'):
                meta_2['quarter_numb'] = spread_metadata.get('quarter_numb_2', 'N')
                meta_2['quarter_pos'] = spread_metadata.get('quarter_pos_2', 'n/a')
                meta_2['component_months_names'] = spread_metadata.get('component_months_names_2', '')
                logger.debug(f"Enriched meta_2 from spread: quarterly={meta_2['quarter_numb']}")
        
        # Extract root codes - handle quarterly formulas (may contain ((('%ROOT... pattern)
        # Try regex pattern first (works for both =((('%ROOT... and ((('%ROOT... patterns)
        if symbol_1:
            root_match = re.search(r"%([A-Z]+)\s+[FGHJKMNQUVXZ]!", symbol_1)
            root_1 = root_match.group(1) if root_match else symbol_1.lstrip('%').split()[0]
        else:
            root_1 = ''
        
        if symbol_2:
            root_match = re.search(r"%([A-Z]+)\s+[FGHJKMNQUVXZ]!", symbol_2)
            root_2 = root_match.group(1) if root_match else symbol_2.lstrip('%').split()[0]
        else:
            root_2 = ''
        
        # Get minimum volumes
        min_vol_1 = self._get_min_volume(root_1)
        min_vol_2 = self._get_min_volume(root_2)
        
        # Check quarterly status for both legs (after enrichment)
        is_quarterly_1 = meta_1.get('quarter_numb') == 'Y'
        is_quarterly_2 = meta_2.get('quarter_numb') == 'Y'
        logger.info(f"Delta sizing setup: {symbol_1} (quarterly={is_quarterly_1}, min={min_vol_1}kb, root={root_1}) vs {symbol_2} (quarterly={is_quarterly_2}, min={min_vol_2}kb, root={root_2})")
        
        # Double-check quarterly detection - if symbol contains quarterly formula pattern, mark as quarterly
        # Pattern matches: ((('...')+('...')+('...'))/3) or =((('...')+('...')+('...'))/3)
        quarterly_pattern = r"\(\(\(.*?\)\)\)/3|\(\(\(.*?\)\+.*?\)\)\)/3"
        if not is_quarterly_1:
            if re.search(quarterly_pattern, str(symbol_1), re.IGNORECASE):
                is_quarterly_1 = True
                logger.info(f"Detected quarterly pattern in {symbol_1}, marking as quarterly")
        if not is_quarterly_2:
            if re.search(quarterly_pattern, str(symbol_2), re.IGNORECASE):
                is_quarterly_2 = True
                logger.info(f"Detected quarterly pattern in {symbol_2}, marking as quarterly")
        
        # Determine base leg
        # Priority: 1) Quarterly leg (if one is quarterly), 2) Higher minimum volume
        # This simplifies monthly vs quarterly calculations
        if is_quarterly_2 and not is_quarterly_1:
            # leg_2 is quarterly, leg_1 is monthly: use leg_2 as base
            base_symbol = symbol_2
            base_meta = meta_2
            base_min = min_vol_2
            base_root = root_2
            other_symbol = symbol_1
            other_meta = meta_1
            other_min = min_vol_1
            other_root_initial = root_1  # Store initial root extraction
            swap_legs = True
            logger.info(f"Base leg selection: Using {symbol_2[:50]}... as base (quarterly leg preferred over monthly {symbol_1[:50]}...)")
        elif is_quarterly_1 and not is_quarterly_2:
            # leg_1 is quarterly, leg_2 is monthly: use leg_1 as base
            base_symbol = symbol_1
            base_meta = meta_1
            base_min = min_vol_1
            base_root = root_1
            other_symbol = symbol_2
            other_meta = meta_2
            other_min = min_vol_2
            other_root_initial = root_2  # Store initial root extraction
            swap_legs = False
            logger.info(f"Base leg selection: Using {symbol_1} as base (quarterly leg preferred over monthly)")
        elif min_vol_2 > min_vol_1:
            # Both same type: use leg with higher minimum
            base_symbol = symbol_2
            base_meta = meta_2
            base_min = min_vol_2
            base_root = root_2
            other_symbol = symbol_1
            other_meta = meta_1
            other_min = min_vol_1
            other_root_initial = root_1  # Store initial root extraction
            swap_legs = True
            logger.info(f"Base leg selection: Using {symbol_2} as base (higher minimum: {min_vol_2}kb > {min_vol_1}kb)")
        elif min_vol_1 > min_vol_2:
            # Both same type: use leg with higher minimum
            base_symbol = symbol_1
            base_meta = meta_1
            base_min = min_vol_1
            base_root = root_1
            other_symbol = symbol_2
            other_meta = meta_2
            other_min = min_vol_2
            other_root_initial = root_2  # Store initial root extraction
            swap_legs = False
            logger.info(f"Base leg selection: Using {symbol_1} as base (higher minimum: {min_vol_1}kb > {min_vol_2}kb)")
        else:
            # Same minimum and same type: default to leg_1
            base_symbol = symbol_1
            base_meta = meta_1
            base_min = min_vol_1
            base_root = root_1
            other_symbol = symbol_2
            other_meta = meta_2
            other_min = min_vol_2
            other_root_initial = root_2  # Store initial root extraction
            swap_legs = False
            logger.info(f"Base leg selection: Using {symbol_1} as base (same minimum, default to leg_1)")
        
        # Calculate base quantity from pos%
        try:
            pct = float(pos_pct) if not pd.isna(pos_pct) else 100.0
            multiplier = pct / 100.0
            
            # Apply base_multiplier_at_100pct when pos% >= 100%
            # This allows sizing up "normal" positions (100% pos%) without affecting lower pos% values
            if pct >= 100.0 and self.base_multiplier_at_100pct != 1.0:
                # Apply multiplier: 100% = base_multiplier, 200% = 2.0 × base_multiplier, etc.
                multiplier = multiplier * self.base_multiplier_at_100pct
            
            # Calculate scaled base quantity BEFORE applying min/max caps
            scaled_base_quantity = self.base_volume_spread_per_side * multiplier
            # Apply min/max caps AFTER scaling
            # Note: base_min (leg-specific minimum) can override pos% scaling
            base_quantity_capped = max(base_min, min(self.max_volume, scaled_base_quantity))
            
            # Check if base leg is quarterly
            is_base_quarterly = base_meta.get('quarter_numb') == 'Y'
            # Use base_root that was already extracted (handles quarterly formulas correctly)
            # Don't re-extract here as it will fail for quarterly formulas
            
            if is_base_quarterly:
                # Convert "per mo" to total volume (×3)
                base_total = base_quantity_capped * 3
                # Round total to quarterly increment
                base_total_rounded = self._round_quarterly_total(base_total, base_root)
                # Store total for delta calculation, convert back to "per mo" for display later
                base_quantity = base_total_rounded  # Store as total
                base_quantity_per_mo = base_total_rounded / 3  # For display
            else:
                # Monthly: Round to appropriate increment
                base_quantity = self._round_to_increment(base_quantity_capped, base_root)
                base_quantity_per_mo = base_quantity  # Same for monthly
            
            # Log if pos% scaling was overridden by base_min
            if scaled_base_quantity < base_min and pct < 100.0:
                logger.debug(f"Pos% scaling ({pct}% → {scaled_base_quantity:.1f}kb) overridden by base_min ({base_min}kb) for {base_symbol} → {base_quantity}kb")
        except (ValueError, TypeError):
            base_quantity_capped = max(base_min, self.base_volume_spread_per_side)
            # Use base_root that was already extracted (handles quarterly formulas correctly)
            is_base_quarterly = base_meta.get('quarter_numb') == 'Y'
            
            if is_base_quarterly:
                base_total = base_quantity_capped * 3
                base_total_rounded = self._round_quarterly_total(base_total, base_root)
                base_quantity = base_total_rounded
                base_quantity_per_mo = base_total_rounded / 3
            else:
                base_quantity = self._round_to_increment(base_quantity_capped, base_root)
                base_quantity_per_mo = base_quantity
        
        # Get prices for delta calculation (both should be in $/usg)
        base_price = self._get_leg_price(base_symbol, base_meta)
        other_price = self._get_leg_price(other_symbol, other_meta)
        
        # Log price retrieval for debugging
        logger.info(f"Delta sizing prices: base={base_symbol} @ ${base_price:.4f}/usg, other={other_symbol} @ ${other_price:.4f}/usg")
        if base_price is None or other_price is None:
            logger.warning(f"Delta sizing: Missing prices - base_price={base_price}, other_price={other_price} for {base_symbol} / {other_symbol}")
        
        # Calculate delta-sized quantity
        if base_price is not None and other_price is not None and base_price > 0:
            # Delta ratio: base_price / other_price (for balanced dollar exposure)
            # Formula: base_quantity × base_price = other_quantity × other_price
            # Therefore: other_quantity = base_quantity × (base_price / other_price)
            # This means: if base is more expensive (base_price > other_price), ratio > 1, need MORE other volume
            #            if base is less expensive (base_price < other_price), ratio < 1, need LESS other volume
            delta_ratio = base_price / other_price
            
            # Check if other leg is quarterly
            is_other_quarterly = other_meta.get('quarter_numb') == 'Y'
            # Extract root code - handle quarterly formulas (may or may not start with '=')
            # First try to use the initial root extraction (more reliable)
            if other_root_initial:
                other_root = other_root_initial
                logger.debug(f"Using initial other_root='{other_root}' from base leg selection")
            elif other_symbol:
                # Try regex pattern (works for both =((('%ROOT... and ((('%ROOT... patterns)
                root_match = re.search(r"%([A-Z]+)\s+[FGHJKMNQUVXZ]!", other_symbol)
                if root_match:
                    other_root = root_match.group(1)
                    logger.debug(f"Extracted other_root='{other_root}' from quarterly formula: {other_symbol[:50]}...")
                else:
                    # Fallback: simple extraction for regular symbols
                    other_root = other_symbol.lstrip('%').split()[0] if other_symbol else ''
                    logger.debug(f"Extracted other_root='{other_root}' using fallback method from: {other_symbol[:50]}...")
            else:
                other_root = ''
                logger.warning(f"other_symbol is empty, cannot extract root")
            
            # For delta calculation, convert base to match other leg's units
            # This ensures we're comparing equivalent volumes for balanced dollar exposure
            if not is_base_quarterly and is_other_quarterly:
                # Base is monthly, other is quarterly: convert base to quarterly total (×3)
                base_quantity_for_delta = base_quantity * 3
                logger.info(f"Delta sizing: Monthly base ({base_quantity}kb) converted to quarterly equivalent ({base_quantity_for_delta}kb) for delta calculation")
            elif is_base_quarterly and not is_other_quarterly:
                # Base is quarterly (stored as total), other is monthly: use base total directly
                # The delta calculation will produce monthly quantity for other leg
                base_quantity_for_delta = base_quantity  # Already total for quarterly
                logger.info(f"Delta sizing: Quarterly base ({base_quantity}kb total = {base_quantity/3}kb per mo) used for delta calculation, other will be monthly")
            else:
                # Both same type (both monthly or both quarterly): use as-is
                base_quantity_for_delta = base_quantity
            
            # Calculate delta-sized quantity using base_quantity_for_delta
            scaled_other_quantity = base_quantity_for_delta * delta_ratio
            logger.info(f"Delta calculation: base={base_symbol} ({base_quantity_for_delta}kb @ ${base_price:.4f}/usg), other={other_symbol} (target @ ${other_price:.4f}/usg)")
            logger.info(f"Delta calculation: delta_ratio={delta_ratio:.4f} (base_price/other_price = {base_price:.4f}/{other_price:.4f})")
            logger.info(f"Delta calculation: scaled_other_quantity={scaled_other_quantity:.2f}kb")
            if delta_ratio < 1:
                logger.info(f"Delta calculation: Base is MORE expensive (ratio < 1), so need LESS other volume")
            elif delta_ratio > 1:
                logger.info(f"Delta calculation: Base is LESS expensive (ratio > 1), so need MORE other volume")
            else:
                logger.info(f"Delta calculation: Base and other are same price (ratio = 1), volumes should be equal")
            
            if is_other_quarterly:
                # Other leg is quarterly: round total to quarterly increment
                other_quantity_capped = max(other_min * 3, min(self.max_volume * 3, scaled_other_quantity))
                logger.info(f"Quarterly other: scaled={scaled_other_quantity:.2f}kb, min_cap={other_min * 3}kb, max_cap={self.max_volume * 3}kb, capped={other_quantity_capped:.2f}kb, root='{other_root}'")
                other_total_rounded = self._round_quarterly_total(other_quantity_capped, other_root)
                logger.info(f"Quarterly other: after rounding to increment (root='{other_root}'), total={other_total_rounded}kb, per_mo={other_total_rounded / 3}kb")
                other_quantity = other_total_rounded  # Store as total
                other_quantity_per_mo = other_total_rounded / 3  # For display
                
                if not is_base_quarterly:
                    # Base is monthly, other is quarterly
                    # Only adjust to match if same commodity (for balanced dollar exposure)
                    # For cross-commodity spreads, keep delta-sized quantities
                    if base_root.upper() == other_root.upper():
                        # Same commodity: monthly leg should equal quarterly total for balanced exposure
                        base_quantity = other_total_rounded  # Monthly leg = quarterly total
                        base_quantity_per_mo = base_quantity  # Same for monthly (already total)
                        logger.info(f"Adjusted monthly base to match quarterly total (same commodity): {base_quantity}kb")
                    else:
                        # Cross-commodity: keep delta-sized quantities (already balanced by price ratio)
                        logger.info(f"Cross-commodity spread ({base_root} vs {other_root}): keeping delta-sized quantities (base={base_quantity}kb, other={other_total_rounded}kb total)")
                elif is_base_quarterly:
                    # Both are quarterly: try to balance, but may have small differences
                    # ABF vs ABF: Both round to 33kb increments → can be perfectly balanced
                    # AFE vs AFE: Both round to 39kb increments → can be perfectly balanced
                    # AFE vs ABF: AFE rounds to 39kb, ABF rounds to 33kb → cannot be perfectly balanced
                    # For AFE vs ABF, we'll have to live with the small difference
                    if base_root.upper() == other_root.upper():
                        # Same commodity: can perfectly balance
                        base_quantity = other_total_rounded  # Match the other leg
                        base_quantity_per_mo = base_quantity / 3
                        logger.info(f"Adjusted quarterly base to match other quarterly (same commodity): {base_quantity}kb total ({base_quantity_per_mo}kb per mo)")
                    else:
                        # Different commodities (AFE vs ABF): cannot perfectly balance due to different increments
                        # AFE = 39kb increments, ABF = 33kb increments
                        # Keep base as calculated, accept small imbalance
                        logger.info(f"Quarterly vs quarterly different commodities ({base_root} vs {other_root}): base={base_quantity}kb total, other={other_total_rounded}kb total (small imbalance acceptable)")
            else:
                # Other leg is monthly: apply min/max caps and round to increment
                other_quantity_capped = max(other_min, min(self.max_volume, scaled_other_quantity))
                other_quantity = self._round_to_increment(other_quantity_capped, other_root)
                other_quantity_per_mo = other_quantity  # Same for monthly
                
                # If base is quarterly and other is monthly
                # Only adjust to match if same commodity (for balanced dollar exposure)
                # For cross-commodity spreads, keep delta-sized quantities
                if is_base_quarterly:
                    if base_root.upper() == other_root.upper():
                        # Same commodity: monthly leg should equal quarterly total for balanced exposure
                        other_quantity = base_quantity  # Monthly leg = quarterly total
                        other_quantity_per_mo = other_quantity  # Same for monthly (already total)
                        logger.info(f"Adjusted monthly other to match quarterly base total (same commodity): {other_quantity}kb (quarterly base = {base_quantity}kb total = {base_quantity_per_mo}kb per mo)")
                    else:
                        # Cross-commodity: keep delta-sized quantities (already balanced by price ratio)
                        logger.info(f"Cross-commodity spread ({base_root} vs {other_root}): keeping delta-sized quantities (base={base_quantity}kb total, other={other_quantity}kb)")
            
            # Log using per_mo values for clarity
            base_display = base_quantity_per_mo if is_base_quarterly else base_quantity
            other_display = other_quantity_per_mo if is_other_quarterly else other_quantity
            base_total_display = base_quantity if is_base_quarterly else base_quantity
            other_total_display = other_quantity if is_other_quarterly else other_quantity
            logger.info(f"Delta sizing (pos%={pct:.1f}%): {base_symbol} @ ${base_price:.4f} = {base_display}kb{' per mo' if is_base_quarterly else ''} ({base_total_display}kb total), {other_symbol} @ ${other_price:.4f} = {other_display}kb{' per mo' if is_other_quarterly else ''} ({other_total_display}kb total), ratio={delta_ratio:.3f})")
        else:
            # Fallback: equal sizing if prices unavailable
            logger.warning(f"Price data unavailable for delta sizing (base={base_price}, other={other_price}). Using equal sizing for {symbol_1} / {symbol_2}")
            is_other_quarterly = other_meta.get('quarter_numb') == 'Y'
            if is_other_quarterly:
                # Other is quarterly: match base quarterly total
                if is_base_quarterly:
                    # Both quarterly: match totals
                    other_quantity = base_quantity
                    other_quantity_per_mo = base_quantity / 3
                else:
                    # Base is monthly, other is quarterly: monthly should match quarterly total
                    # Convert base monthly to quarterly equivalent, then match
                    base_quarterly_equiv = base_quantity * 3
                    other_quantity = base_quarterly_equiv
                    other_quantity_per_mo = other_quantity / 3
            else:
                # Other is monthly
                if is_base_quarterly:
                    # Base is quarterly, other is monthly: monthly should match quarterly total
                    other_quantity = base_quantity  # Monthly = quarterly total
                    other_quantity_per_mo = other_quantity
                else:
                    # Both monthly: match quantities
                    other_quantity = base_quantity
                    other_quantity_per_mo = other_quantity
        
        # Format quantities using per_mo values for quarterlies
        is_quarterly_1 = meta_1.get('quarter_numb') == 'Y'
        is_quarterly_2 = meta_2.get('quarter_numb') == 'Y'
        
        # Use per_mo values for display (already calculated above)
        if swap_legs:
            # We swapped, so swap back for output
            qty_1 = f"{int(other_quantity_per_mo)}kb per mo" if is_quarterly_1 else f"{int(other_quantity)}kb"
            qty_2 = f"{int(base_quantity_per_mo)}kb per mo" if is_quarterly_2 else f"{int(base_quantity)}kb"
        else:
            qty_1 = f"{int(base_quantity_per_mo)}kb per mo" if is_quarterly_1 else f"{int(base_quantity)}kb"
            qty_2 = f"{int(other_quantity_per_mo)}kb per mo" if is_quarterly_2 else f"{int(other_quantity)}kb"
        
        return (qty_1, qty_2)
    
    def _format_spread_message(self, signal: Dict, metadata: Dict) -> str:
        """Format ICE Chat message for spread."""
        signal_type = signal.get('signal_type', 'buy')
        pos_pct = signal.get('pos_pct', 0)
        
        # For spreads, signal_type determines which leg is buy/sell
        # If signal_type is 'buy', we're buying the spread (buy symbol_1, sell symbol_2)
        # If signal_type is 'sell', we're selling the spread (sell symbol_1, buy symbol_2)
        
        symbol_1 = metadata.get('symbol_1', '')
        symbol_2 = metadata.get('symbol_2', '')
        
        # Get metadata for both legs
        meta_1 = self._get_symbol_metadata(symbol_1) if symbol_1 else {}
        meta_2 = self._get_symbol_metadata(symbol_2) if symbol_2 else {}
        
        # For spreads, enrich metadata from spread's metadata fields if individual lookup failed
        # This handles cases where the formula itself isn't in the symbol matrix but the spread is
        if metadata.get('quarter_numb_1'):
            meta_1['quarter_numb'] = metadata.get('quarter_numb_1', 'N')
            meta_1['quarter_pos'] = metadata.get('quarter_pos_1', 'n/a')
            meta_1['component_months_names'] = metadata.get('component_months_names_1', '')
        if metadata.get('quarter_numb_2'):
            meta_2['quarter_numb'] = metadata.get('quarter_numb_2', 'N')
            meta_2['quarter_pos'] = metadata.get('quarter_pos_2', 'n/a')
            meta_2['component_months_names'] = metadata.get('component_months_names_2', '')
        
        # For quarterly formulas in spreads, enrich metadata from spread's metadata fields
        # This handles cases where the formula itself isn't in the symbol matrix
        if symbol_1.startswith('=') and not meta_1.get('quarter_numb'):
            # Try to get quarter info from spread metadata
            if metadata.get('quarter_numb_1') == 'Y':
                meta_1['quarter_numb'] = 'Y'
                meta_1['quarter_pos'] = metadata.get('quarter_pos_1', 'n/a')
                meta_1['component_months_names'] = metadata.get('component_months_names_1', '')
        if symbol_2.startswith('=') and not meta_2.get('quarter_numb'):
            # Try to get quarter info from spread metadata
            if metadata.get('quarter_numb_2') == 'Y':
                meta_2['quarter_numb'] = 'Y'
                meta_2['quarter_pos'] = metadata.get('quarter_pos_2', 'n/a')
                meta_2['component_months_names'] = metadata.get('component_months_names_2', '')
        
        # Determine buy/sell for each leg
        if signal_type == 'buy':
            # Buying spread: buy symbol_1, sell symbol_2
            action_1 = 'buy'
            action_2 = 'sell'
        else:
            # Selling spread: sell symbol_1, buy symbol_2
            action_1 = 'sell'
            action_2 = 'buy'
        
        # Extract symbol roots for product names
        # Handle quarterly formulas (start with '=') and regular symbols
        if symbol_1 and symbol_1.startswith('='):
            # Quarterly formula - extract root from first component symbol
            # Pattern: =((('%ROOT MONTH!-EXCHANGE')+...
            root_match = re.search(r"%([A-Z]+)\s+[FGHJKMNQUVXZ]!", symbol_1)
            root_1 = root_match.group(1) if root_match else ''
        else:
            root_1 = symbol_1.lstrip('%').split()[0] if symbol_1 else ''
        
        if symbol_2 and symbol_2.startswith('='):
            # Quarterly formula - extract root from first component symbol
            root_match = re.search(r"%([A-Z]+)\s+[FGHJKMNQUVXZ]!", symbol_2)
            root_2 = root_match.group(1) if root_match else ''
        else:
            root_2 = symbol_2.lstrip('%').split()[0] if symbol_2 else ''
        
        # Get product display names
        product_1 = self._get_product_display_name(
            root_1,
            meta_1.get('location', ''),
            meta_1.get('molecule', '')
        )
        product_2 = self._get_product_display_name(
            root_2,
            meta_2.get('location', ''),
            meta_2.get('molecule', '')
        )
        
        # Calculate delta-sized quantities
        quantity_1, quantity_2 = self._calculate_delta_sized_quantities(
            pos_pct,
            symbol_1,
            symbol_2,
            meta_1,
            meta_2,
            spread_metadata=metadata  # Pass spread metadata for quarterly enrichment
        )
        
        location_code_1 = self._get_location_code(meta_1.get('location', ''))
        location_code_2 = self._get_location_code(meta_2.get('location', ''))
        date_1 = self._format_date(symbol_1, meta_1)
        date_2 = self._format_date(symbol_2, meta_2)
        
        # Format: "What can I sell [qty1] [product1] ([code1]) in [date1] and buy [qty2] [product2] ([code2]) in [date2]?"
        # Handle optional location codes
        part_1 = f"{action_1} {quantity_1} {product_1}"
        if location_code_1:
            part_1 += f" ({location_code_1})"
        part_1 += f" in {date_1}"
        
        part_2 = f"{action_2} {quantity_2} {product_2}"
        if location_code_2:
            part_2 += f" ({location_code_2})"
        part_2 += f" in {date_2}"
        
        message = f"ICE Chat: What can I {part_1} and {part_2}?"
        
        return message.strip()
    
    def format_score_breakdown(self, signal: Dict) -> str:
        """
        Format score breakdown string.
        
        Example: "Base 50 + ADX +0 + RSI +15 + CMF +6 + BBW +0 = Final 85.9"
        
        Args:
            signal: Signal dictionary
        
        Returns:
            Formatted score breakdown string
        """
        base_points = signal.get('base_points', 0)
        confluence_breakdown = signal.get('confluence_breakdown', {})
        total_points = signal.get('points', 0)
        
        # Map indicator names to display names (matching reference format)
        display_names = {
            'rsi_aligned': 'RSI',
            'rsi_percentile_aligned': 'RSI',
            'stochastic_aligned': 'STOCH',
            'cci_aligned': 'CCI',
            'adx_strong': 'ADX',
            'bollinger_aligned': 'BBW',
            'bollinger_extreme': 'BBW',
            'correlation_high': 'CORR',
            'cointegration': 'COINT',
            'macd_reversal': 'MACD'
        }
        
        parts = [f"Base {int(base_points)}"]
        
        # Add all confluence indicators in specific order (even if 0)
        # Order: ADX, RSI, CMF, BBW (matching reference format)
        indicator_order = ['adx_strong', 'rsi_aligned', 'rsi_percentile_aligned', 
                          'stochastic_aligned', 'cci_aligned', 'correlation_high', 
                          'cointegration', 'macd_reversal', 'bollinger_aligned', 'bollinger_extreme']
        
        # Add ordered indicators first
        for indicator_name in indicator_order:
            if indicator_name in confluence_breakdown:
                points = confluence_breakdown[indicator_name]
                display_name = display_names.get(indicator_name, indicator_name.upper())
                parts.append(f"{display_name} +{int(points)}")
        
        # Add any remaining indicators not in the order
        for indicator_name, points in confluence_breakdown.items():
            if indicator_name not in indicator_order:
                display_name = display_names.get(indicator_name, indicator_name.upper())
                parts.append(f"{display_name} +{int(points)}")
        
        # Add tenor/liquidity bonus if present
        tenor_liquidity_breakdown = signal.get('tenor_liquidity_breakdown', {})
        if tenor_liquidity_breakdown:
            for bonus_type, points in tenor_liquidity_breakdown.items():
                if points > 0:
                    parts.append(f"{bonus_type} +{int(points)}")
        
        # Add trend exhaustion penalty if present (for trend systems)
        exhaustion_penalty_breakdown = signal.get('exhaustion_penalty_breakdown', {})
        if exhaustion_penalty_breakdown:
            # Map penalty names to display names
            penalty_display_names = {
                'rsi_extreme': 'RSI_EXTREME',
                'price_distance_from_ema': 'EMA_DIST',
                'bollinger_extreme': 'BB_EXTREME'
            }
            for penalty_type, points in exhaustion_penalty_breakdown.items():
                if points > 0:
                    display_name = penalty_display_names.get(penalty_type, penalty_type.upper())
                    parts.append(f"{display_name} -{int(points)}")
        
        parts.append(f"= Final {total_points:.1f}")
        
        return " + ".join(parts)
    
    def format_risk_details(self, signal: Dict) -> str:
        """
        Format risk details string.
        
        Example: "Risk: Stop=0.56x ATR, Target=0.83x ATR, ATR=0.0496"
        
        Args:
            signal: Signal dictionary
        
        Returns:
            Formatted risk details string
        """
        atr = signal.get('atr', 0)
        stop_mult = self.config['atr_multipliers']['stop']
        target_mult = self.config['atr_multipliers']['target']
        
        if pd.isna(atr) or atr == 0:
            return f"Risk: Stop={stop_mult}x ATR, Target={target_mult}x ATR, ATR=N/A"
        
        return f"Risk: Stop={stop_mult}x ATR, Target={target_mult}x ATR, ATR={atr:.4f}"


