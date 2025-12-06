"""
Trade payload builder for AI alignment analysis.
Converts signal dictionaries into structured trade payloads for OpenAI API.
"""
import logging
from typing import Dict, List, Optional
from datetime import datetime
try:
    from ..data_loaders.curve_loader import load_curve_prices
    CURVE_LOADER_AVAILABLE = True
except ImportError:
    CURVE_LOADER_AVAILABLE = False
    logging.warning("curve_loader not available. Forward curve data will not be included.")

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


def determine_structure_type(signal: Dict) -> str:
    """
    Determine if trade is an outright or spread.
    
    Args:
        signal: Signal dictionary with 'symbol' and 'row_data' keys
    
    Returns:
        "outright" or "spread"
    """
    symbol = signal.get("symbol", "")
    row_data = signal.get("row_data", {})
    
    # Check row_data first (most reliable)
    is_outright = row_data.get("is_outright", True)
    if isinstance(is_outright, bool):
        return "outright" if is_outright else "spread"
    
    # Fallback: check symbol format
    if symbol.startswith("="):
        return "spread"
    
    return "outright"


def extract_legs(
    signal: Dict,
    ice_chat_formatter,
    data_date: Optional[datetime] = None
) -> List[Dict]:
    """
    Extract leg information from signal for outrights and spreads.
    
    Args:
        signal: Signal dictionary
        ice_chat_formatter: ICEChatFormatter instance (for symbol matrix access)
        data_date: Data date for year inference
    
    Returns:
        List of leg dictionaries
    """
    symbol = signal.get("symbol", "")
    structure_type = determine_structure_type(signal)
    
    legs = []
    
    if structure_type == "outright":
        # Single leg for outright
        leg = _extract_outright_leg(symbol, ice_chat_formatter, data_date)
        if leg:
            legs.append(leg)
    else:
        # Two legs for spread
        leg1, leg2 = _extract_spread_legs(symbol, ice_chat_formatter, data_date)
        if leg1:
            legs.append(leg1)
        if leg2:
            legs.append(leg2)
    
    return legs


def _extract_outright_leg(
    symbol: str,
    ice_chat_formatter,
    data_date: Optional[datetime] = None
) -> Optional[Dict]:
    """
    Extract leg information for an outright trade.
    
    Args:
        symbol: Symbol string (e.g., "%CL F!" or quarterly formula)
        ice_chat_formatter: ICEChatFormatter instance
        data_date: Data date for year inference
    
    Returns:
        Leg dictionary or None
    """
    if not hasattr(ice_chat_formatter, '_get_symbol_metadata'):
        logger.warning("ICEChatFormatter does not have _get_symbol_metadata method")
        return None
    
    metadata = ice_chat_formatter._get_symbol_metadata(symbol)
    if not metadata:
        logger.warning(f"Could not get metadata for symbol: {symbol}")
        return None
    
    # Extract commodity, location, tenor
    commodity = _get_product_display_name(
        metadata.get('product', ''),
        metadata.get('location', ''),
        metadata.get('molecule', '')
    )
    location = metadata.get('location', '')
    tenor = _format_tenor(symbol, metadata, ice_chat_formatter, data_date)
    
    # Determine if quarterly
    is_quarterly = metadata.get('quarter_numb', 'N') == 'Y'
    quarter_position = metadata.get('quarter_pos', 'n/a')
    
    # Extract volume from ICE Chat if available (we'll parse it later)
    volume = ""  # Will be populated from ICE Chat message if needed
    
    leg = {
        "leg_index": 1,
        "leg_role": "single",
        "symbol": symbol,
        "commodity": commodity,
        "location": location,
        "tenor": tenor,
        "volume": volume,
        "is_quarterly": is_quarterly,
        "quarter_position": str(quarter_position) if quarter_position != 'n/a' else "n/a"
    }
    
    return leg


def _extract_spread_legs(
    symbol: str,
    ice_chat_formatter,
    data_date: Optional[datetime] = None
) -> tuple[Optional[Dict], Optional[Dict]]:
    """
    Extract leg information for a spread trade.
    
    Args:
        symbol: Spread formula (e.g., "=('%IBC F!-IEU')-('%XRB U!')")
        ice_chat_formatter: ICEChatFormatter instance
        data_date: Data date for year inference
    
    Returns:
        Tuple of (leg1_dict, leg2_dict) or (None, None) if extraction fails
    """
    if not hasattr(ice_chat_formatter, '_get_symbol_metadata'):
        logger.warning("ICEChatFormatter does not have _get_symbol_metadata method")
        return None, None
    
    metadata = ice_chat_formatter._get_symbol_metadata(symbol)
    if not metadata:
        logger.warning(f"Could not get metadata for spread symbol: {symbol}")
        return None, None
    
    # Extract symbol_1 and symbol_2 from metadata
    symbol_1 = metadata.get('symbol_1', '')
    symbol_2 = metadata.get('symbol_2', '')
    
    if not symbol_1 or not symbol_2:
        logger.warning(f"Spread metadata missing symbol_1 or symbol_2: {symbol}")
        return None, None
    
    # Get metadata for each leg
    meta_1 = ice_chat_formatter._get_symbol_metadata(symbol_1)
    meta_2 = ice_chat_formatter._get_symbol_metadata(symbol_2)
    
    if not meta_1 or not meta_2:
        logger.warning(f"Could not get metadata for spread legs: {symbol_1}, {symbol_2}")
        return None, None
    
    # Build leg 1
    commodity_1 = _get_product_display_name(
        meta_1.get('product', ''),
        meta_1.get('location', ''),
        meta_1.get('molecule', '')
    )
    location_1 = meta_1.get('location', '')
    tenor_1 = _format_tenor(symbol_1, meta_1, ice_chat_formatter, data_date)
    is_quarterly_1 = meta_1.get('quarter_numb', 'N') == 'Y'
    quarter_pos_1 = meta_1.get('quarter_pos', 'n/a')
    
    leg_1 = {
        "leg_index": 1,
        "leg_role": "spread_leg_1",
        "symbol": symbol_1,
        "commodity": commodity_1,
        "location": location_1,
        "tenor": tenor_1,
        "volume": "",  # Will be populated from ICE Chat
        "is_quarterly": is_quarterly_1,
        "quarter_position": str(quarter_pos_1) if quarter_pos_1 != 'n/a' else "n/a"
    }
    
    # Build leg 2
    commodity_2 = _get_product_display_name(
        meta_2.get('product', ''),
        meta_2.get('location', ''),
        meta_2.get('molecule', '')
    )
    location_2 = meta_2.get('location', '')
    tenor_2 = _format_tenor(symbol_2, meta_2, ice_chat_formatter, data_date)
    is_quarterly_2 = meta_2.get('quarter_numb', 'N') == 'Y'
    quarter_pos_2 = meta_2.get('quarter_pos', 'n/a')
    
    leg_2 = {
        "leg_index": 2,
        "leg_role": "spread_leg_2",
        "symbol": symbol_2,
        "commodity": commodity_2,
        "location": location_2,
        "tenor": tenor_2,
        "volume": "",  # Will be populated from ICE Chat
        "is_quarterly": is_quarterly_2,
        "quarter_position": str(quarter_pos_2) if quarter_pos_2 != 'n/a' else "n/a"
    }
    
    return leg_1, leg_2


def _get_product_display_name(product: str, location: str, molecule: str) -> str:
    """
    Get product display name from product, location, and molecule.
    
    Args:
        product: Product name
        location: Location name
        molecule: Molecule name
    
    Returns:
        Formatted product display name
    """
    # Use molecule if available, otherwise product
    if molecule and molecule != 'n/a' and molecule != '':
        name = molecule
    elif product and product != 'n/a' and product != '':
        name = product
    else:
        name = "Unknown"
    
    # Add location if available and meaningful
    if location and location != 'n/a' and location != '':
        name = f"{location} {name}"
    
    return name


def _format_tenor(
    symbol: str,
    metadata: Dict,
    ice_chat_formatter,
    data_date: Optional[datetime] = None
) -> str:
    """
    Format tenor string (month or quarter) for a symbol.
    
    Args:
        symbol: Symbol string
        metadata: Symbol metadata dictionary
        ice_chat_formatter: ICEChatFormatter instance (for date formatting)
        data_date: Data date for year inference
    
    Returns:
        Formatted tenor string (e.g., "Jan 2025" or "Q1 2025")
    """
    # Use ICEChatFormatter's _format_date method if available
    if hasattr(ice_chat_formatter, '_format_date'):
        try:
            formatted = ice_chat_formatter._format_date(symbol, metadata)
            # Convert from "Jan '25" format to "Jan 2025" format
            if "'" in formatted:
                year_short = formatted.split("'")[-1]
                year_full = f"20{year_short}"
                formatted = formatted.replace(f"'{year_short}", year_full)
            return formatted
        except Exception as e:
            logger.warning(f"Error using ICEChatFormatter._format_date: {e}")
    
    # Fallback: manual formatting
    quarter_numb = metadata.get('quarter_numb', 'N')
    quarter_pos = metadata.get('quarter_pos', 'n/a')
    
    # Infer year
    if data_date:
        if isinstance(data_date, datetime):
            reference_year = data_date.year
        else:
            try:
                reference_year = int(str(data_date)[:4])
            except:
                reference_year = datetime.now().year
    else:
        reference_year = datetime.now().year
    
    inferred_year = reference_year + 1
    
    # Check if quarterly
    if quarter_numb == 'Y' and quarter_pos and quarter_pos != 'n/a' and quarter_pos != '':
        quarter_pos_clean = str(quarter_pos).strip().lstrip('Q').rstrip('Q').strip()
        quarter_name = QUARTER_MAP.get(quarter_pos_clean, f'Q{quarter_pos_clean}')
        return f"{quarter_name} {inferred_year}"
    
    # Monthly - extract month code from symbol
    import re
    month_match = re.search(r'%[A-Z]+ ([FGHJKMNQUVXZ])!', symbol)
    if month_match:
        month_code = month_match.group(1)
        month_name = MONTH_MAP.get(month_code, month_code)
        return f"{month_name} {inferred_year}"
    
    # Fallback
    return f"Unknown {inferred_year}"


def format_price_labels(
    signal: Dict,
    is_spread: bool
) -> Dict[str, str]:
    """
    Format price labels with Pay/Rcv logic.
    
    Args:
        signal: Signal dictionary with 'entry_price', 'stop', 'target', 'signal_type'
        is_spread: Whether this is a spread trade
    
    Returns:
        Dictionary with 'entry_price_label', 'stop_price_label', 'target_price_label'
    """
    entry_price = signal.get('entry_price', 0)
    stop_price = signal.get('stop', 0)
    target_price = signal.get('target', 0)
    signal_type = signal.get('signal_type', 'buy')
    
    # Use report_generator's _format_price_value logic
    # For spreads: Buy = Receive (positive), Sell = Pay (negative)
    # For outrights: Buy = Pay (positive), Sell = Receive (negative)
    
    def format_value(value: float, price_type: str) -> str:
        """Format a price value with Pay/Rcv prefix."""
        if pd.isna(value) or value == 0:
            return "N/A"
        
        abs_value = abs(value)
        formatted = f"${abs_value:.4f}"
        
        if is_spread:
            # Spread logic: Buy = Receive, Sell = Pay
            if signal_type == 'buy':
                if price_type == 'price':
                    prefix = "Rcv"  # Entry: Receive on buy spread
                elif price_type == 'stop':
                    prefix = "Pay"  # Stop: Pay more (worse)
                else:  # target
                    prefix = "Pay"  # Target: Pay less (better)
            else:  # sell
                if price_type == 'price':
                    prefix = "Pay"  # Entry: Pay on sell spread
                elif price_type == 'stop':
                    prefix = "Rcv"  # Stop: Receive less (worse)
                else:  # target
                    prefix = "Rcv"  # Target: Receive more (better)
        else:
            # Outright logic: Buy = Pay, Sell = Receive
            if signal_type == 'buy':
                prefix = "Pay"
            else:  # sell
                prefix = "Rcv"
        
        return f"{prefix} {formatted}"
    
    # Import pandas for isna check
    import pandas as pd
    
    entry_label = format_value(entry_price, 'price')
    stop_label = format_value(stop_price, 'stop')
    target_label = format_value(target_price, 'target')
    
    return {
        "entry_price_label": entry_label,
        "stop_price_label": stop_label,
        "target_price_label": target_label
    }


def build_trade_payload(
    signal: Dict,
    ice_chat_formatter,
    data_date: Optional[datetime] = None
) -> Dict:
    """
    Build structured trade payload from signal dictionary.
    
    Args:
        signal: Signal dictionary from signal generator
        ice_chat_formatter: ICEChatFormatter instance
        data_date: Data date for year inference and week_date
    
    Returns:
        Structured trade_payload dictionary
    """
    # Determine structure type
    structure_type = determine_structure_type(signal)
    
    # Get symbol and spread expression
    symbol = signal.get("symbol", "")
    spread_expression = symbol if structure_type == "spread" else ""
    
    # Get strategy type
    row_data = signal.get("row_data", {})
    strategy_name = row_data.get("strategy_name", "")
    strategy_type_map = {
        'trend_following': 'Trend',
        'enhanced_trend_following': 'Enhanced Trend',
        'mean_reversion': 'Mean Reversion',
        'macd_rsi_exhaustion': 'MACD+RSI Exhaustion'
    }
    strategy_type = strategy_type_map.get(strategy_name, "Unknown")
    
    # Get signal direction
    signal_type = signal.get("signal_type", "buy")
    signal_direction = "Buy" if signal_type == "buy" else "Sell"
    
    # Extract legs
    legs = extract_legs(signal, ice_chat_formatter, data_date)
    
    # Format price labels
    is_spread = structure_type == "spread"
    price_labels = format_price_labels(signal, is_spread)
    
    # Get pricing values
    entry_price = signal.get("entry_price", 0)
    stop_price = signal.get("stop", 0)
    target_price = signal.get("target", 0)
    position_pct = signal.get("pos_pct", 0)
    signal_score = signal.get("points", 0)
    
    # Get risk metrics
    atr = signal.get("atr", 0)
    stop_pct = signal.get("stop_pct", 0)
    target_pct = signal.get("target_pct", 0)
    
    # Get ATR multipliers from config (if available)
    # Default values if not in config
    stop_multiple = 0.56
    target_multiple = 0.83
    if hasattr(ice_chat_formatter, 'config'):
        atr_multipliers = ice_chat_formatter.config.get('atr_multipliers', {})
        stop_multiple = atr_multipliers.get('stop', 0.56)
        target_multiple = atr_multipliers.get('target', 0.83)
    
    # Format score details and risk details
    score_details_raw = ""
    risk_details_raw = ""
    if hasattr(ice_chat_formatter, 'format_score_breakdown'):
        try:
            score_details_raw = ice_chat_formatter.format_score_breakdown(signal)
        except Exception as e:
            logger.warning(f"Error formatting score breakdown: {e}")
    
    if hasattr(ice_chat_formatter, 'format_risk_details'):
        try:
            risk_details_raw = ice_chat_formatter.format_risk_details(signal)
        except Exception as e:
            logger.warning(f"Error formatting risk details: {e}")
    
    # Format ICE Chat message
    ice_chat_raw = ""
    if hasattr(ice_chat_formatter, 'format_ice_chat_message'):
        try:
            ice_chat_raw = ice_chat_formatter.format_ice_chat_message(signal)
        except Exception as e:
            logger.warning(f"Error formatting ICE Chat message: {e}")
    
    # Extract volumes from ICE Chat message if available
    # Parse "buy 15kb ... and sell 10kb ..." pattern
    if ice_chat_raw and legs:
        import re
        volume_matches = re.findall(r'(\d+)kb', ice_chat_raw)
        if len(volume_matches) >= len(legs):
            for i, leg in enumerate(legs):
                if i < len(volume_matches):
                    legs[i]["volume"] = f"{volume_matches[i]}kb"
    
    # Get week date
    entry_date = signal.get("entry_date")
    if entry_date:
        if isinstance(entry_date, datetime):
            week_date = entry_date.strftime("%Y-%m-%d")
        elif isinstance(entry_date, str):
            week_date = entry_date[:10] if len(entry_date) >= 10 else entry_date
        else:
            try:
                import pandas as pd
                if isinstance(entry_date, pd.Timestamp):
                    week_date = entry_date.strftime("%Y-%m-%d")
                else:
                    week_date = str(entry_date)[:10]
            except:
                week_date = ""
    elif data_date:
        if isinstance(data_date, datetime):
            week_date = data_date.strftime("%Y-%m-%d")
        else:
            week_date = str(data_date)[:10]
    else:
        week_date = datetime.now().strftime("%Y-%m-%d")
    
    # Get current alignment icon
    alignment_score = signal.get("alignment_score", 0)
    alignment_icon_map = {
        (90, 100): "🔥",
        (80, 90): "⭐",
        (70, 80): "⚡",
        (60, 70): "⚠️",
        (0, 60): "💥"
    }
    current_align_icon = "⚡"  # Default
    for (min_score, max_score), icon in alignment_icon_map.items():
        if min_score <= alignment_score < max_score:
            current_align_icon = icon
            break
    
    # Get PRWK flag
    was_active_prior_week = signal.get("was_active_prior_week", False)
    prwk_flag = "✓" if was_active_prior_week else "✗"
    
    # Get forward curve data for market structure analysis
    forward_curves = {}
    if CURVE_LOADER_AVAILABLE:
        try:
            curve_data = load_curve_prices()
            if curve_data:
                # Extract relevant forward curve data for the legs in this trade
                for leg in legs:
                    leg_symbol = leg.get("symbol", "")
                    commodity_root = leg.get("commodity", "").split()[0] if leg.get("commodity") else ""
                    # Try to extract root code from symbol
                    import re
                    root_match = re.search(r'%([A-Z]+)', leg_symbol)
                    if root_match:
                        root_code = root_match.group(1)
                        if root_code in curve_data:
                            # Get all available months for this commodity
                            commodity_curve = curve_data[root_code]
                            # Format as readable forward curve
                            forward_curves[root_code] = {
                                "commodity": commodity_root or root_code,
                                "prices": commodity_curve  # {month_col: price}
                            }
        except Exception as e:
            logger.warning(f"Error loading forward curve data: {e}")
    
    # Build trade payload
    trade_payload = {
        "structure_type": structure_type,
        "spread_expression": spread_expression,
        "strategy_type": strategy_type,
        "signal_direction": signal_direction,
        "legs": legs,
        "entry_price_label": price_labels["entry_price_label"],
        "entry_price_numeric": float(entry_price) if entry_price else 0.0,
        "stop_price_label": price_labels["stop_price_label"],
        "stop_price_numeric": float(stop_price) if stop_price else 0.0,
        "target_price_label": price_labels["target_price_label"],
        "target_price_numeric": float(target_price) if target_price else 0.0,
        "position_pct": float(position_pct) if position_pct else 0.0,
        "signal_score": float(signal_score) if signal_score else 0.0,
        "risk_metrics": {
            "atr": float(atr) if atr else 0.0,
            "stop_multiple": stop_multiple,
            "target_multiple": target_multiple,
            "stop_pct": float(stop_pct) if stop_pct else 0.0,
            "target_pct": float(target_pct) if target_pct else 0.0
        },
        "score_details_raw": score_details_raw,
        "risk_details_raw": risk_details_raw,
        "ice_chat_raw": ice_chat_raw,
        "week_date": week_date,
        "current_align_icon": current_align_icon,
        "prwk_flag": prwk_flag,
        "entry_date": week_date,
        "forward_curves": forward_curves  # Market structure data
    }
    
    return trade_payload

