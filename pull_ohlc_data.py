"""
Pull OHLC data from ICE for all symbols and export to CSV files
Each file is named with the date of the weekly data (e.g., unfiltered_2025-01-10.csv)
"""
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import logging
import icepython as ice
import sys

# Setup detailed logging
def setup_logging(log_dir='logs/ice_data_pull'):
    """
    Setup detailed logging to file and console
    
    Args:
        log_dir: Directory for log files
    """
    # Create log directory
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    
    # Create timestamped log file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_path / f"ice_data_pull_{timestamp}.log"
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.DEBUG)
    
    # Formatter with detailed information
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler (INFO level for readability)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # File handler (DEBUG level for maximum detail)
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized - Log file: {log_file}")
    logger.debug(f"Debug logging enabled - All details will be logged to file")
    
    return logger, log_file

logger, log_file = setup_logging('logs/ice_data_pull')

# OHLC fields for weekly candles
OHLC_FIELDS = ['Open', 'High', 'Low', 'Close']


def get_friday_date(date=None):
    """
    Get the Friday date for the current week
    For weekly data, we use Friday as the week end date
    
    Logic:
    - If today is Friday: Use today (this week's Friday)
    - If today is Monday-Thursday: Use this week's Friday (upcoming)
    - If today is Saturday-Sunday: Use last Friday (most recent completed week)
    """
    if date is None:
        date = datetime.now()
    
    current_weekday = date.weekday()  # 0=Monday, 4=Friday, 6=Sunday
    
    if current_weekday == 4:  # Today is Friday
        friday = date
    elif current_weekday < 4:  # Monday-Thursday
        # Use this week's Friday (upcoming)
        days_to_friday = 4 - current_weekday
        friday = date + timedelta(days=days_to_friday)
    else:  # Saturday (5) or Sunday (6)
        # Use last Friday (most recent completed week)
        days_since_friday = current_weekday - 4
        friday = date - timedelta(days=days_since_friday)
    
    return friday


def fetch_symbol_ohlc(symbol, start_date, end_date):
    """
    Fetch OHLC data for a single symbol from ICE API
    
    Args:
        symbol: ICE symbol (e.g., '%PRL F!-IEU')
        start_date: Start date (datetime)
        end_date: End date (datetime)
    
    Returns:
        DataFrame with Date index and columns: open, high, low, close
    """
    # Format dates for ICE API (YYYY-MM-DD)
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    logger.debug(f"Fetching {symbol}: {start_str} to {end_str}")
    
    try:
        fetch_start_time = datetime.now()
        
        # Request time series data with weekly granularity
        logger.debug(f"ICE API call: get_timeseries([{symbol}], {OHLC_FIELDS}, 'W', '{start_str}', '{end_str}')")
        result = ice.get_timeseries(
            [symbol],
            OHLC_FIELDS,
            'W',  # Weekly granularity
            start_str,
            end_str
        )
        
        fetch_duration = (datetime.now() - fetch_start_time).total_seconds()
        logger.debug(f"ICE API response received in {fetch_duration:.2f}s for {symbol}")
        
        if result is None:
            logger.warning(f"No data returned (None) for {symbol}")
            return None
        
        if len(result) == 0:
            logger.warning(f"Empty result for {symbol}")
            return None
        
        logger.debug(f"ICE API returned {len(result)} rows for {symbol}")
        
        rows = []
        valid_rows = 0
        invalid_rows = 0
        
        for idx, row in enumerate(result):
            if row is None:
                logger.debug(f"  Row {idx}: None")
                invalid_rows += 1
                continue
            
            if len(row) == 0:
                logger.debug(f"  Row {idx}: Empty")
                invalid_rows += 1
                continue
            
            # First element is date
            date = row[0]
            if date is None:
                logger.debug(f"  Row {idx}: Date is None")
                invalid_rows += 1
                continue
            
            logger.debug(f"  Row {idx}: Date={date}")
            
            # Extract OHLC values (starting from index 1)
            row_data = {}
            for i, field in enumerate(OHLC_FIELDS):
                field_idx = i + 1  # Skip date at index 0
                if field_idx < len(row):
                    value = row[field_idx]
                    # Convert to float if not None
                    if value is not None and str(value).strip() != '':
                        try:
                            row_data[field.lower()] = float(value)
                            logger.debug(f"    {field}: {value}")
                        except (ValueError, TypeError) as e:
                            row_data[field.lower()] = None
                            logger.debug(f"    {field}: {value} (invalid: {e})")
                    else:
                        row_data[field.lower()] = None
                        logger.debug(f"    {field}: None/empty")
                else:
                    row_data[field.lower()] = None
                    logger.debug(f"    {field}: Missing (index {field_idx} >= {len(row)})")
            
            # Only add row if we have at least Close value
            if row_data.get('close') is not None:
                row_dict = {
                    'Date': pd.to_datetime(date),
                    'open': row_data.get('open'),
                    'high': row_data.get('high'),
                    'low': row_data.get('low'),
                    'close': row_data.get('close')
                }
                rows.append(row_dict)
                valid_rows += 1
                logger.debug(f"  Row {idx}: Valid OHLC added")
            else:
                invalid_rows += 1
                logger.debug(f"  Row {idx}: Invalid (no close price)")
        
        logger.debug(f"Parsed {valid_rows} valid rows, {invalid_rows} invalid rows for {symbol}")
        
        if len(rows) == 0:
            logger.warning(f"No valid OHLC data for {symbol} (all {len(result)} rows were invalid)")
            return None
        
        # Create DataFrame
        df = pd.DataFrame(rows)
        df = df.set_index('Date')
        df = df.sort_index()
        
        # Calculate data completeness
        ohlc_complete = df[['open', 'high', 'low', 'close']].notna().all(axis=1).sum()
        ohlc_partial = len(df) - ohlc_complete
        
        logger.info(f"✓ {symbol}: {len(df)} data points ({ohlc_complete} complete OHLC, {ohlc_partial} partial) - {start_date.date()} to {end_date.date()}")
        logger.debug(f"  Date range: {df.index.min().date()} to {df.index.max().date()}")
        logger.debug(f"  OHLC completeness: {ohlc_complete}/{len(df)} rows have all OHLC values")
        
        return df
        
    except Exception as e:
        logger.error(f"✗ Error fetching data for {symbol}: {e}", exc_info=True)
        return None


def calculate_quarterly_ohlc(component_symbols, component_data_dict, conversion_factor=None):
    """
    Calculate quarterly OHLC from component symbols' OHLC data
    
    Args:
        component_symbols: List of component symbols (e.g., ['%AFE F!-IEU', '%AFE G!-IEU', '%AFE H!-IEU'])
        component_data_dict: Dictionary mapping symbol -> DataFrame with OHLC data
        conversion_factor: Conversion factor string (e.g., '/521' or '/42') or None
    
    Returns:
        DataFrame with quarterly OHLC data
    """
    # Get data for all component symbols
    component_dfs = []
    for symbol in component_symbols:
        if symbol in component_data_dict:
            component_dfs.append(component_data_dict[symbol])
    
    if len(component_dfs) == 0:
        return None
    
    # Find common dates (intersection)
    common_dates = component_dfs[0].index
    for df in component_dfs[1:]:
        common_dates = common_dates.intersection(df.index)
    
    if len(common_dates) == 0:
        return None
    
    # Calculate quarterly OHLC for each date
    quarterly_rows = []
    for date in common_dates:
        opens = []
        highs = []
        lows = []
        closes = []
        
        for df in component_dfs:
            if date in df.index:
                row = df.loc[date]
                if pd.notna(row.get('open')):
                    opens.append(row['open'])
                if pd.notna(row.get('high')):
                    highs.append(row['high'])
                if pd.notna(row.get('low')):
                    lows.append(row['low'])
                if pd.notna(row.get('close')):
                    closes.append(row['close'])
        
        if len(opens) > 0 and len(closes) > 0:
            # Calculate quarterly values
            q_open = np.mean(opens) if opens else None
            q_high = np.max(highs) if highs else None
            q_low = np.min(lows) if lows else None
            q_close = np.mean(closes) if closes else None
            
            # Apply conversion factor if needed
            if conversion_factor:
                try:
                    # Parse conversion factor (e.g., '/521' -> divide by 521)
                    if conversion_factor.startswith('/'):
                        divisor = float(conversion_factor[1:])
                        if q_open is not None:
                            q_open = q_open / divisor
                        if q_high is not None:
                            q_high = q_high / divisor
                        if q_low is not None:
                            q_low = q_low / divisor
                        if q_close is not None:
                            q_close = q_close / divisor
                except (ValueError, TypeError):
                    logger.warning(f"Invalid conversion factor: {conversion_factor}")
            
            quarterly_rows.append({
                'Date': date,
                'open': q_open,
                'high': q_high,
                'low': q_low,
                'close': q_close
            })
    
    if len(quarterly_rows) == 0:
        return None
    
    df = pd.DataFrame(quarterly_rows)
    df = df.set_index('Date')
    return df


def pull_all_ohlc_data(
    symbols_file='lists_and_matrix/symbol_matrix.csv',
    weeks_back=1,
    output_dir='full_unfiltered_historicals',
    snapshot_date=None
):
    """
    Pull OHLC data for all symbols and export to CSV files
    
    Args:
        symbols_file: Path to CSV file with symbols (default: lists_and_matrix/symbol_matrix.csv)
        weeks_back: Number of weeks of history to fetch (default: 1 = current week only)
        output_dir: Directory to save CSV files (default: 'full_unfiltered_historicals')
        snapshot_date: Specific date to pull data for (default: None = current date)
    
    Returns:
        Path to the created CSV file
    """
    logger.info("=" * 80)
    logger.info("ICE OHLC DATA PULL - STARTING")
    logger.info("=" * 80)
    logger.info(f"Symbols file: {symbols_file}")
    logger.info(f"Weeks back: {weeks_back}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Log file: {log_file}")
    
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Output directory created/verified: {output_path}")
    
    # Load symbols
    logger.info(f"Loading symbols from {symbols_file}...")
    df_symbols = pd.read_csv(symbols_file, keep_default_na=False)
    
    # Get ALL symbols (outrights + spreads) - ICE can handle the formulas
    symbols = df_symbols['ice_symbol'].unique().tolist()
    
    # Count breakdown if available
    if 'spread_type' in df_symbols.columns:
        outright_count = len(df_symbols[df_symbols['spread_type'] == 'outright'])
        spread_count = len(df_symbols[df_symbols['spread_type'] == 'spread'])
        logger.info(f"Found {len(symbols)} total symbols: {outright_count} outrights + {spread_count} spreads")
    else:
        logger.info(f"Found {len(symbols)} unique symbols to fetch")
    
    # Determine snapshot date (Friday of the current week)
    if snapshot_date is None:
        snapshot_date = get_friday_date()
    else:
        if isinstance(snapshot_date, str):
            snapshot_date = datetime.strptime(snapshot_date, '%Y-%m-%d')
        snapshot_date = get_friday_date(snapshot_date)
    
    # Calculate start date (for current week, go back to previous Friday)
    # For weekly data, we fetch the week ending on the Friday date
    if weeks_back == 1:
        # Current week only: fetch the week ending on Friday
        # For weekly granularity, we need to go back to the previous Friday to get the current week
        start_date = snapshot_date - timedelta(weeks=1)
    else:
        # Multiple weeks: go back the specified number of weeks
        start_date = snapshot_date - timedelta(weeks=weeks_back)
    
    logger.info(f"Fetching current week data (week ending {snapshot_date.date()})")
    logger.info(f"Date range: {start_date.date()} to {snapshot_date.date()}")
    logger.info(f"Snapshot date (Friday): {snapshot_date.date()}")
    
    # Fetch OHLC data for all symbols (both monthly and quarterly)
    # ICE can handle quarterly formulas directly, so we fetch them like regular symbols
    logger.info(f"\nStarting data fetch for {len(symbols)} symbols...")
    logger.info(f"Date range: {start_date.date()} to {snapshot_date.date()}")
    
    all_data = []
    successful = 0
    failed = 0
    total_rows_fetched = 0
    
    overall_start_time = datetime.now()
    
    for i, symbol in enumerate(symbols, 1):
        symbol_start_time = datetime.now()
        logger.info(f"[{i}/{len(symbols)}] Processing {symbol}...")
        logger.debug(f"  Symbol type: {df_symbols[df_symbols['ice_symbol'] == symbol]['spread_type'].iloc[0] if len(df_symbols[df_symbols['ice_symbol'] == symbol]) > 0 else 'unknown'}")
        
        df = fetch_symbol_ohlc(symbol, start_date, snapshot_date)
        
        symbol_duration = (datetime.now() - symbol_start_time).total_seconds()
        
        if df is not None and len(df) > 0:
            # Add symbol column
            df['symbol'] = symbol
            all_data.append(df)
            successful += 1
            total_rows_fetched += len(df)
            logger.debug(f"  ✓ Success in {symbol_duration:.2f}s - {len(df)} rows added")
        else:
            failed += 1
            logger.warning(f"  ✗ Failed in {symbol_duration:.2f}s - No data returned")
        
        # Progress update every 100 symbols
        if i % 100 == 0:
            elapsed = (datetime.now() - overall_start_time).total_seconds()
            rate = i / elapsed if elapsed > 0 else 0
            remaining = (len(symbols) - i) / rate if rate > 0 else 0
            logger.info(f"  Progress: {i}/{len(symbols)} ({i/len(symbols)*100:.1f}%) - "
                       f"Success: {successful}, Failed: {failed} - "
                       f"Elapsed: {elapsed/60:.1f}min, Est. remaining: {remaining/60:.1f}min")
    
    overall_duration = (datetime.now() - overall_start_time).total_seconds()
    logger.info(f"\nData fetch complete in {overall_duration/60:.2f} minutes")
    logger.info(f"  Successful: {successful}/{len(symbols)}")
    logger.info(f"  Failed: {failed}/{len(symbols)}")
    logger.info(f"  Total rows fetched: {total_rows_fetched:,}")
    
    if len(all_data) == 0:
        logger.error("No data fetched for any symbols!")
        return None
    
    # Combine all data
    logger.info("Combining all symbol data...")
    combined_df = pd.concat(all_data, ignore_index=False)
    
    # Reset index to have Date as a column
    combined_df = combined_df.reset_index()
    
    # Reorder columns: Date, symbol, open, high, low, close
    column_order = ['Date', 'symbol', 'open', 'high', 'low', 'close']
    combined_df = combined_df[column_order]
    
    # Sort by Date and symbol
    combined_df = combined_df.sort_values(['Date', 'symbol'])
    
    # Determine actual most recent date in the data
    actual_latest_date = combined_df['Date'].max()
    actual_earliest_date = combined_df['Date'].min()
    
    logger.info(f"\nDate Analysis:")
    logger.info(f"  Requested snapshot date: {snapshot_date.date()}")
    logger.info(f"  Actual data date range: {actual_earliest_date.date()} to {actual_latest_date.date()}")
    logger.info(f"  Most recent data date: {actual_latest_date.date()}")
    
    # Check if actual date differs significantly from requested
    date_diff = (snapshot_date.date() - actual_latest_date.date()).days
    if abs(date_diff) > 7:
        logger.warning(f"  ⚠️  Actual data date differs from requested by {date_diff} days")
    else:
        logger.info(f"  ✓ Actual data date matches requested (difference: {date_diff} days)")
    
    # Generate output filename with ACTUAL data date (not requested date)
    actual_date_str = actual_latest_date.strftime('%Y-%m-%d')
    output_file = output_path / f"unfiltered_{actual_date_str}.csv"
    
    logger.info(f"  Using actual data date for filename: {actual_date_str}")
    logger.info(f"\nCombining and saving data...")
    logger.debug(f"  Total DataFrames to combine: {len(all_data)}")
    logger.debug(f"  Output file: {output_file}")
    
    # Save to CSV
    save_start_time = datetime.now()
    combined_df.to_csv(output_file, index=False)
    save_duration = (datetime.now() - save_start_time).total_seconds()
    
    file_size_mb = output_file.stat().st_size / (1024 * 1024)
    logger.info(f"✓ Saved to {output_file} ({file_size_mb:.2f} MB) in {save_duration:.2f}s")
    logger.debug(f"  File path: {output_file.absolute()}")
    
    logger.info("=" * 80)
    logger.info("OHLC DATA PULL COMPLETE")
    logger.info("=" * 80)
    logger.info(f"  Total symbols: {len(symbols)}")
    logger.info(f"  Successful: {successful}")
    logger.info(f"  Failed: {failed}")
    logger.info(f"  Total rows: {len(combined_df):,}")
    logger.info(f"  Date range: {actual_earliest_date.date()} to {actual_latest_date.date()}")
    logger.info(f"  Output file: {output_file}")
    logger.info(f"  Filename uses actual data date: {actual_date_str}")
    
    print("=" * 80)
    print("OHLC DATA PULL COMPLETE")
    print("=" * 80)
    print(f"  Total symbols: {len(symbols)}")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    print(f"  Total rows: {len(combined_df):,}")
    print(f"  Date range: {actual_earliest_date.date()} to {actual_latest_date.date()}")
    print(f"  Most recent data date: {actual_latest_date.date()}")
    print(f"  Output file: {output_file}")
    print(f"  (Filename uses actual data date: {actual_date_str})")
    
    return output_file


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Pull OHLC data from ICE for all symbols and export to CSV'
    )
    parser.add_argument(
        '--symbols',
        type=str,
        default='lists_and_matrix/symbol_matrix.csv',
        help='Path to symbols CSV file (default: lists_and_matrix/symbol_matrix.csv)'
    )
    parser.add_argument(
        '--weeks',
        type=int,
        default=1,
        help='Number of weeks of history to fetch (default: 1 = current week only)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='full_unfiltered_historicals',
        help='Output directory for CSV files (default: full_unfiltered_historicals)'
    )
    parser.add_argument(
        '--date',
        type=str,
        default=None,
        help='Specific date to pull data for (YYYY-MM-DD, default: current date)'
    )
    
    args = parser.parse_args()
    
    pull_all_ohlc_data(
        symbols_file=args.symbols,
        weeks_back=args.weeks,
        output_dir=args.output_dir,
        snapshot_date=args.date
    )

