"""
Data loader for signal generator system.
Supports loading data for current date or any historical date for backtesting.
"""
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import logging
from typing import Optional, Tuple

# Setup logger
logger = logging.getLogger(__name__)


def find_most_recent_csv(data_dir: str = 'full_unfiltered_historicals') -> Optional[Path]:
    """
    Find the most recent CSV file in the data directory based on date in filename.
    
    Args:
        data_dir: Directory containing CSV files (default: 'full_unfiltered_historicals')
    
    Returns:
        Path to most recent CSV file, or None if no files found
    """
    data_path = Path(data_dir)
    
    if not data_path.exists():
        logger.error(f"Data directory not found: {data_dir}")
        return None
    
    # Find all CSV files matching the pattern unfiltered_YYYY-MM-DD.csv
    csv_files = list(data_path.glob('unfiltered_*.csv'))
    
    if not csv_files:
        logger.error(f"No CSV files found in {data_dir}")
        return None
    
    # Extract date from filename and sort by date (most recent first)
    # Format: unfiltered_YYYY-MM-DD.csv
    def get_file_date(csv_file: Path) -> datetime:
        try:
            filename = csv_file.stem  # Remove .csv extension
            date_str = filename.replace('unfiltered_', '')
            return datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            # If date parsing fails, use a very old date so it's sorted last
            return datetime(1900, 1, 1)
    
    # Sort by date extracted from filename (most recent first)
    csv_files.sort(key=get_file_date, reverse=True)
    
    most_recent = csv_files[0]
    file_date = get_file_date(most_recent)
    logger.info(f"Found most recent CSV by date: {most_recent.name} (date: {file_date.strftime('%Y-%m-%d')})")
    
    return most_recent


def find_csv_by_date(target_date: datetime, data_dir: str = 'full_unfiltered_historicals') -> Optional[Path]:
    """
    Find CSV file for a specific date.
    
    Args:
        target_date: Target date to find CSV for
        data_dir: Directory containing CSV files (default: 'full_unfiltered_historicals')
    
    Returns:
        Path to CSV file for target date, or None if not found
    """
    data_path = Path(data_dir)
    
    if not data_path.exists():
        logger.error(f"Data directory not found: {data_dir}")
        return None
    
    # Format date as YYYY-MM-DD
    date_str = target_date.strftime('%Y-%m-%d')
    csv_file = data_path / f'unfiltered_{date_str}.csv'
    
    if csv_file.exists():
        logger.info(f"Found CSV for date {date_str}: {csv_file.name}")
        return csv_file
    
    # If exact date not found, try to find closest date (within 7 days)
    logger.warning(f"Exact CSV not found for {date_str}, searching for closest date...")
    
    csv_files = list(data_path.glob('unfiltered_*.csv'))
    if not csv_files:
        logger.error(f"No CSV files found in {data_dir}")
        return None
    
    # Try to find closest date
    closest_file = None
    min_diff = timedelta(days=999)
    
    for csv_file in csv_files:
        try:
            # Extract date from filename (unfiltered_YYYY-MM-DD.csv)
            filename = csv_file.stem  # Remove .csv extension
            file_date_str = filename.replace('unfiltered_', '')
            file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
            
            diff = abs((target_date.date() - file_date.date()))
            if diff < min_diff:
                min_diff = diff
                closest_file = csv_file
        except ValueError:
            continue
    
    if closest_file and min_diff <= timedelta(days=7):
        logger.info(f"Using closest CSV (diff: {min_diff.days} days): {closest_file.name}")
        return closest_file
    
    logger.error(f"No suitable CSV found for date {date_str} (within 7 days)")
    return None


def load_data(target_date: Optional[datetime] = None, 
              data_dir: str = 'full_unfiltered_historicals') -> Optional[pd.DataFrame]:
    """
    Load data from CSV file for specified date or most recent.
    
    Args:
        target_date: Target date to load data for (None = most recent)
        data_dir: Directory containing CSV files (default: 'full_unfiltered_historicals')
    
    Returns:
        DataFrame with loaded data, or None if loading failed
    """
    # Find appropriate CSV file
    if target_date:
        csv_path = find_csv_by_date(target_date, data_dir)
    else:
        csv_path = find_most_recent_csv(data_dir)
    
    if csv_path is None:
        logger.error("Could not find CSV file to load")
        return None
    
    # Load CSV
    try:
        logger.info(f"Loading data from {csv_path.name}...")
        df = pd.read_csv(csv_path, low_memory=False)
        logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
        return df
    except Exception as e:
        logger.error(f"Error loading CSV file {csv_path}: {e}", exc_info=True)
        return None


def prepare_data(df: pd.DataFrame, 
                 target_date: Optional[datetime] = None) -> pd.DataFrame:
    """
    Prepare and validate loaded data for signal generation.
    
    Args:
        df: Raw DataFrame from CSV
        target_date: Target date for analysis (None = use most recent date in data)
    
    Returns:
        Prepared DataFrame ready for signal generation
    """
    if df is None or len(df) == 0:
        logger.error("Cannot prepare empty DataFrame")
        return df
    
    # Make a copy to avoid modifying original
    prepared_df = df.copy()
    
    # Normalize column names: convert to lowercase for common OHLC and indicator columns
    # This handles case differences between CSV headers and code expectations
    # Create a comprehensive mapping for all common columns
    column_mapping = {}
    
    # OHLC columns - normalize to lowercase (check both cases)
    # Priority: exact matches first, then specific patterns
    # First, try exact matches for standard OHLC (Close, Open, High, Low)
    for col_upper in ['Close', 'Open', 'High', 'Low']:
        col_lower = col_upper.lower()
        # If uppercase version exists and lowercase doesn't, rename it
        if col_upper in prepared_df.columns and col_lower not in prepared_df.columns:
            column_mapping[col_upper] = col_lower
    
    # Then handle _price suffix columns (close_price -> close, etc.)
    # But be careful - only rename if target doesn't already exist
    price_suffix_cols = {
        'close_price': 'close',
        'open_price': 'open', 
        'high_price': 'high',
        'low_price': 'low'
    }
    for col, target in price_suffix_cols.items():
        if col in prepared_df.columns and target not in prepared_df.columns and col not in column_mapping:
            column_mapping[col] = target
    
    # IMPORTANT: Don't rename columns like percentile_close, bb_lower, etc.
    # These contain 'close' or 'low' but are different columns!
    # Only rename the specific patterns above
    
    # EMA columns - normalize to lowercase (EMA_20 -> ema_20)
    ema_cols = [col for col in prepared_df.columns if col.startswith('EMA_')]
    for col_upper in ema_cols:
        col_lower = col_upper.lower()
        if col_upper in prepared_df.columns and col_lower not in prepared_df.columns:
            column_mapping[col_upper] = col_lower
    
    # Keep Date capitalized for consistency
    # Date column stays as 'Date'
    
    # Rename columns if they exist
    if column_mapping:
        prepared_df.rename(columns=column_mapping, inplace=True)
        logger.info(f"Renamed columns: {column_mapping}")
    
    # Convert Date column to datetime if it exists
    if 'Date' in prepared_df.columns:
        prepared_df['Date'] = pd.to_datetime(prepared_df['Date'], errors='coerce')
        logger.debug(f"Converted Date column to datetime")
    else:
        logger.warning("Date column not found in DataFrame")
    
    # If target_date specified, keep that date AND previous week(s) for cross detection
    # We need at least 2 weeks of data to detect crosses
    if target_date and 'Date' in prepared_df.columns:
        target_date_only = target_date.date()
        # Keep target date and up to 2 previous weeks for cross detection
        from datetime import timedelta
        min_date = target_date_only - timedelta(days=14)  # 2 weeks back
        before_filter = len(prepared_df)
        prepared_df = prepared_df[
            (prepared_df['Date'].dt.date >= min_date) & 
            (prepared_df['Date'].dt.date <= target_date_only)
        ]
        after_filter = len(prepared_df)
        logger.info(f"Filtered to target date {target_date_only} and previous weeks (min: {min_date}): {before_filter} -> {after_filter} rows")
        
        if after_filter == 0:
            logger.warning(f"No data found for target date {target_date_only} and previous weeks")
    
    # Sort by Date (most recent first) if Date column exists
    if 'Date' in prepared_df.columns:
        prepared_df = prepared_df.sort_values('Date', ascending=False)
    
    # Validate required columns for signal generation (after normalization)
    required_columns = [
        'ice_connect_symbol',
        'close',
        'atr',
        'macd_line',
        'macd_signal',
        'rsi',
        'percentile_close'
    ]
    
    missing_columns = [col for col in required_columns if col not in prepared_df.columns]
    if missing_columns:
        logger.warning(f"Missing recommended columns: {missing_columns}")
        # Log what columns we actually have that might be relevant
        available_ohlc = [c for c in prepared_df.columns if any(x in c.lower() for x in ['close', 'open', 'high', 'low'])]
        if available_ohlc:
            logger.warning(f"Available OHLC-like columns: {available_ohlc[:5]}")
        logger.warning("Signal generation may fail or produce incomplete results")
    
    # Log data summary
    logger.info(f"Prepared data: {len(prepared_df)} rows")
    if 'Date' in prepared_df.columns:
        date_range = prepared_df['Date'].min(), prepared_df['Date'].max()
        logger.info(f"Date range: {date_range[0]} to {date_range[1]}")
    
    # Count symbols
    if 'ice_connect_symbol' in prepared_df.columns:
        unique_symbols = prepared_df['ice_connect_symbol'].nunique()
        logger.info(f"Unique symbols: {unique_symbols}")
    
    return prepared_df


def validate_data(df: pd.DataFrame) -> Tuple[bool, list]:
    """
    Validate that data has required columns and sufficient data quality.
    
    Args:
        df: DataFrame to validate
    
    Returns:
        Tuple of (is_valid, list_of_warnings)
    """
    warnings = []
    
    if df is None or len(df) == 0:
        return False, ["DataFrame is empty"]
    
    # Check for required columns
    required_columns = ['ice_connect_symbol', 'close']
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        return False, [f"Missing required columns: {missing}"]
    
    # Check for missing values in critical columns
    critical_columns = ['close', 'ice_connect_symbol']
    for col in critical_columns:
        if col in df.columns:
            missing_count = df[col].isna().sum()
            if missing_count > 0:
                warnings.append(f"{col} has {missing_count} missing values")
    
    # Check indicator columns (warn if missing, but don't fail)
    indicator_columns = ['atr', 'macd_line', 'macd_signal', 'rsi', 'percentile_close']
    for col in indicator_columns:
        if col not in df.columns:
            warnings.append(f"Indicator column missing: {col}")
        elif df[col].isna().sum() == len(df):
            warnings.append(f"Indicator column {col} has no valid values")
    
    return True, warnings


