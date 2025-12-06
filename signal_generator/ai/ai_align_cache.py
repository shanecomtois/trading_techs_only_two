"""
Daily cache implementation for AI alignment responses.
Prevents redundant API calls for the same trade on the same day.
"""
import json
import logging
from pathlib import Path
from datetime import datetime, date
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# Default cache directory (relative to project root)
DEFAULT_CACHE_DIR = Path(__file__).parent.parent.parent / 'outputs' / 'ai_cache'


def load_today_cache(cache_date: date, cache_dir: Optional[Path] = None) -> Dict:
    """
    Load cache for a specific date.
    
    Args:
        cache_date: Date to load cache for
        cache_dir: Cache directory path (default: outputs/ai_cache)
    
    Returns:
        Dictionary with cache entries, or empty dict if cache doesn't exist
    """
    if cache_dir is None:
        cache_dir = DEFAULT_CACHE_DIR
    else:
        cache_dir = Path(cache_dir)
    
    # Ensure cache directory exists
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    # Build cache file path
    cache_filename = f"ai_align_{cache_date.strftime('%Y%m%d')}.json"
    cache_file = cache_dir / cache_filename
    
    if not cache_file.exists():
        logger.debug(f"Cache file does not exist: {cache_file}")
        return {}
    
    try:
        with open(cache_file, 'r', encoding='utf-8') as f:
            cache_data = json.load(f)
        
        # Return entries dict, or empty if structure is wrong
        entries = cache_data.get("entries", {})
        logger.info(f"Loaded cache for {cache_date}: {len(entries)} entries")
        return entries
        
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing cache file {cache_file}: {e}")
        return {}
    except Exception as e:
        logger.error(f"Error loading cache file {cache_file}: {e}")
        return {}


def save_today_cache(cache: Dict, cache_date: date, cache_dir: Optional[Path] = None) -> None:
    """
    Save cache for a specific date.
    
    Args:
        cache: Dictionary with cache entries (key: cache_key, value: AI response)
        cache_date: Date to save cache for
        cache_dir: Cache directory path (default: outputs/ai_cache)
    """
    if cache_dir is None:
        cache_dir = DEFAULT_CACHE_DIR
    else:
        cache_dir = Path(cache_dir)
    
    # Ensure cache directory exists
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    # Build cache file path
    cache_filename = f"ai_align_{cache_date.strftime('%Y%m%d')}.json"
    cache_file = cache_dir / cache_filename
    
    # Build cache structure
    cache_data = {
        "cache_date": cache_date.strftime("%Y-%m-%d"),
        "entries": cache
    }
    
    try:
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved cache for {cache_date}: {len(cache)} entries to {cache_file}")
        
    except Exception as e:
        logger.error(f"Error saving cache file {cache_file}: {e}")


def build_cache_key(trade_signature: Dict) -> str:
    """
    Build cache key from trade signature.
    
    Args:
        trade_signature: Dictionary with trade identifying information:
            {
                "week_date": str,
                "structure_type": str,
                "symbol": str,
                "signal_direction": str,
                "strategy_type": str
            }
    
    Returns:
        Cache key string
    """
    week_date = trade_signature.get("week_date", "")
    structure_type = trade_signature.get("structure_type", "")
    symbol = trade_signature.get("symbol", "")
    signal_direction = trade_signature.get("signal_direction", "")
    strategy_type = trade_signature.get("strategy_type", "")
    
    # Build cache key
    cache_key = f"{week_date}|{structure_type}|{symbol}|{signal_direction}|{strategy_type}"
    
    return cache_key


def get_or_fetch_ai_alignment(
    trade_signature: Dict,
    trade_payload: Dict,
    cache_date: Optional[date] = None,
    cache_dir: Optional[Path] = None,
    multi_pass: bool = True,
    num_passes: int = 3,
    **api_kwargs
) -> Dict:
    """
    Get AI alignment from cache or fetch from API if not cached.
    
    Args:
        trade_signature: Dictionary with trade identifying information for cache key
        trade_payload: Full trade payload for API call (if needed)
        cache_date: Date for cache lookup (default: today)
        cache_dir: Cache directory path (default: outputs/ai_cache)
        **api_kwargs: Additional arguments to pass to get_ai_trade_alignment()
    
    Returns:
        Dictionary with AI response (same structure as get_ai_trade_alignment)
    """
    from .ai_align_client import get_ai_trade_alignment
    
    # Determine cache date
    if cache_date is None:
        cache_date = date.today()
    
    # Build cache key
    cache_key = build_cache_key(trade_signature)
    
    # Try to load from cache
    cache = load_today_cache(cache_date, cache_dir)
    
    if cache_key in cache:
        logger.info(f"Cache HIT for key: {cache_key[:100]}...")
        cached_response = cache[cache_key]
        
        # Add timestamp if not present (for backward compatibility)
        if "timestamp" not in cached_response:
            cached_response["timestamp"] = datetime.now().isoformat()
        
        return cached_response
    
    # Cache miss - call API
    logger.info(f"Cache MISS for key: {cache_key[:100]}... Calling OpenAI API...")
    ai_response = get_ai_trade_alignment(
        trade_payload, 
        multi_pass=multi_pass,
        num_passes=num_passes,
        **api_kwargs
    )
    
    # Store in cache (even if error, to avoid repeated failed calls)
    cache[cache_key] = ai_response
    save_today_cache(cache, cache_date, cache_dir)
    
    return ai_response

