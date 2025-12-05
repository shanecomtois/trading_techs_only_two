"""
Curve price loader for delta sizing calculations.
Loads prices from CurveBuilder Excel file for spread leg price lookups.
"""
import pandas as pd
import json
from pathlib import Path
from typing import Dict, Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Module-level cache to avoid reloading on every call
_CURVE_CACHE = None


def load_curve_prices(cache_path: Optional[Path] = None, force_reload: bool = False) -> Dict:
    """
    Load prices from CurveBuilder Excel file and cache as JSON.
    
    Args:
        cache_path: Optional path to cache JSON file
        force_reload: Force reload even if cached
    
    Returns:
        Dictionary: {root_code: {month_col: price_value}}
        Example: {"AFE": {"Nov_25": 45.2}, "PRL": {"Dec_25": 42.1}}
    """
    global _CURVE_CACHE
    
    # Return cached version if available and not forcing reload
    if _CURVE_CACHE is not None and not force_reload:
        return _CURVE_CACHE
    
    # Default cache path
    if cache_path is None:
        cache_path = Path(__file__).parent.parent.parent / 'cache' / 'curvebuilder_prices_latest.json'
    
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Try to load from CurveBuilder Excel
    curvebuilder_output_dir = Path(r"C:\ICEPython\TradeBookM2M_PROD\Outputs")
    excel_files = sorted(curvebuilder_output_dir.glob("forward_curves_*.xlsx"), reverse=True) if curvebuilder_output_dir.exists() else []
    
    price_cache = {}
    
    if excel_files:
        try:
            latest_excel = excel_files[0]
            logger.info(f"Loading curve prices from: {latest_excel}")
            df = pd.read_excel(latest_excel, sheet_name='Closing Curves (Prior Day)')
            
            # Map commodity names to root codes (matching UETTechOnlySignals format)
            commodity_to_root = {
                'Propane (MB LST)': 'PRL',
                'Propane (MB Non-TET)': 'PRN',
                'Propane (Conway)': 'PRC',
                'AFE Propane (FEI)': 'AFE',
                'Normal Butane (MB Non-TET)': 'NBI',
                'LST Normal Butane (MB LST)': 'NBR',
                'Normal Butane (Conway)': 'IBC',
                'Far East Butane': 'ABF',
                'Isobutane (MB Non-TET)': 'ISO',
                'Isobutane (Conway)': 'ISC',
                'Natural Gasoline (MB Non-TET)': 'NGE',
                'Natural Gasoline (Conway)': 'NGC',
                'WTI Crude Oil': 'CL',
                'Natural Gas (HH)': 'NG',
                'RBOB Gasoline': 'XRB',
                'Heating Oil': 'HO',
            }
            
            # Extract month columns (Nov_25, Dec_25, etc.)
            month_cols = [c for c in df.columns if c != 'Commodity' and '_' in str(c)]
            
            # Build price cache: {root_code: {month_col: price}}
            for _, row in df.iterrows():
                commodity = str(row.get('Commodity', ''))
                if 'M/M Spreads' in commodity:  # Skip spread rows
                    continue
                
                root = commodity_to_root.get(commodity)
                if not root:
                    continue
                
                price_cache[root] = {}
                for month_col in month_cols:
                    price_val = row.get(month_col)
                    if pd.notna(price_val) and price_val > 0:
                        price_cache[root][month_col] = float(price_val)
            
            # Save to cache
            cache_path.write_text(json.dumps(price_cache, indent=2), encoding='utf-8')
            logger.info(f"Loaded {len(price_cache)} commodities from CurveBuilder Excel, cached to {cache_path}")
            
        except Exception as e:
            logger.warning(f"Error loading Excel, trying cached JSON: {e}")
            # Fall through to cached JSON load
    
    # Try to load from cached JSON (fallback or if Excel failed)
    if not price_cache and cache_path.exists():
        try:
            price_cache = json.loads(cache_path.read_text(encoding='utf-8'))
            logger.info(f"Loaded {len(price_cache)} commodities from cached JSON")
        except Exception as e:
            logger.warning(f"Error loading cached JSON: {e}")
    
    # Store in module-level cache
    _CURVE_CACHE = price_cache
    return price_cache


def map_month_code_to_excel_column(month_code: str, year: int) -> Optional[str]:
    """
    Convert month code + year to Excel column format.
    
    Args:
        month_code: Month code (F, G, H, J, K, M, N, Q, U, V, X, Z) or quarter (Q1, Q2, Q3, Q4)
        year: Full year (e.g., 2026)
    
    Returns:
        Excel column format (e.g., "Jan_26", "Apr_26") or None if invalid
    
    Examples:
        'J' + 2026 → "Apr_26"
        'U' + 2026 → "Sep_26"
        'Q1' + 2026 → "Jan_26" (first month of quarter)
    """
    month_code_to_name = {
        'F': 'Jan', 'G': 'Feb', 'H': 'Mar', 'J': 'Apr',
        'K': 'May', 'M': 'Jun', 'N': 'Jul', 'Q': 'Aug',
        'U': 'Sep', 'V': 'Oct', 'X': 'Nov', 'Z': 'Dec',
    }
    
    # Handle quarterly contracts (Q1, Q2, Q3, Q4)
    if month_code.startswith('Q') and len(month_code) > 1:
        try:
            quarter_num = int(month_code[1])
            quarter_to_first_month = {1: 'Jan', 2: 'Apr', 3: 'Jul', 4: 'Oct'}
            month_name = quarter_to_first_month.get(quarter_num, 'Jan')
        except ValueError:
            return None
    else:
        month_name = month_code_to_name.get(month_code, '')
    
    if not month_name:
        return None
    
    year_short = str(year)[-2:]  # 2026 → "26"
    return f"{month_name}_{year_short}"


def get_leg_price_from_curve(
    symbol: str,
    curve_data: Dict,
    year: Optional[int] = None
) -> Optional[float]:
    """
    Get price for a symbol from curve data.
    
    Args:
        symbol: ICE symbol (e.g., '%AFE F!-IEU', '%CL V!', or quarterly formula)
        curve_data: Price cache from load_curve_prices()
        year: Optional year (if None, will try to infer from current date)
    
    Returns:
        Price in cpg (cents per gallon), or None if unavailable
    """
    if not curve_data:
        return None
    
    # Extract root code (strip % prefix and suffix)
    import re
    # Pattern: %ROOT or %ROOT followed by space and month code
    match = re.search(r'%([A-Z]+)', symbol)
    if not match:
        return None
    
    root_code = match.group(1).upper()
    
    if root_code not in curve_data:
        return None
    
    # Extract month code
    month_code = None
    if symbol.startswith('='):
        # Quarterly formula - extract from component symbols
        # For now, return None (will handle in calling code)
        return None
    else:
        # Monthly symbol - extract month code
        # Pattern: %ROOT MONTH! or %ROOT MONTH!-EXCHANGE
        month_match = re.search(r'%[A-Z]+\s+([FGHJKMNQUVXZ])!', symbol)
        if month_match:
            month_code = month_match.group(1)
    
    if not month_code:
        return None
    
    # Get year if not provided - try to extract from symbol or default
    if year is None:
        # Try to extract year from symbol (e.g., '26 in date format)
        year_match = re.search(r"'(\d{2})", symbol)
        if year_match:
            year_short = int(year_match.group(1))
            # Assume 2000s for years 00-50, 1900s for 51-99
            year = 2000 + year_short if year_short <= 50 else 1900 + year_short
        else:
            # Default to next year for contracts
            year = datetime.now().year + 1
    
    # Map month code to Excel column
    target_col = map_month_code_to_excel_column(month_code, year)
    if not target_col:
        return None
    
    commodity_prices = curve_data[root_code]
    
    # Try exact match first
    if target_col in commodity_prices:
        return commodity_prices[target_col]
    
    # Fallback: find closest available month
    available_cols = sorted(commodity_prices.keys())
    if not available_cols:
        return None
    
    # Try to find closest month (simple string comparison)
    # This is a basic fallback - could be improved
    for col in available_cols:
        if col.startswith(target_col.split('_')[0]):  # Same month name
            return commodity_prices[col]
    
    # Last resort: return first available price
    return commodity_prices[available_cols[0]] if available_cols else None


