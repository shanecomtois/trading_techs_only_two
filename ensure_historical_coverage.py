"""
Ensure historical coverage of weekly data files
- Maintains minimum 104 weeks (2 years) of historical files
- Allows growth to 156 weeks (3 years)
- Deletes files older than 156 weeks
- Validates all files and regenerates invalid ones
- Processes missing weeks sequentially (1 worker)
"""
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import logging
import sys
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import traceback

# Add error handling for imports
try:
    print("Starting script initialization...", file=sys.stderr, flush=True)
except:
    pass

def load_email_config(email_config_file='email_settings/Email.env'):
    """Load email configuration from .env file"""
    config_path = Path(email_config_file)
    
    if not config_path.exists():
        logger.warning(f"Email config file not found: {email_config_file}. Email notifications disabled.")
        return None
    
    email_config = {}
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    email_config[key.strip()] = value.strip()
        
        # Validate required fields
        required_fields = ['SMTP_HOST', 'SMTP_PORT', 'SMTP_USER', 'SMTP_PASS', 'SMTP_SENDER', 'SMTP_TO']
        missing_fields = [field for field in required_fields if field not in email_config]
        
        if missing_fields:
            logger.warning(f"Missing required email config fields: {missing_fields}. Email notifications disabled.")
            return None
        
        logger.info(f"Loaded email configuration from: {email_config_file}")
        return email_config
    except Exception as e:
        logger.error(f"Error loading email config file {email_config_file}: {e}")
        return None

# Setup logging
def setup_logging(log_dir='logs/historical_coverage'):
    """Setup logging for historical coverage script"""
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_path / f"historical_coverage_{timestamp}.log"
    
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized - Log file: {log_file}")
    
    return logger, log_file

# Setup logging first
try:
    logger, log_file = setup_logging()
    print(f"Logging initialized: {log_file}", file=sys.stderr, flush=True)
except Exception as e:
    print(f"CRITICAL: Failed to setup logging: {e}", file=sys.stderr, flush=True)
    print(traceback.format_exc(), file=sys.stderr, flush=True)
    sys.exit(1)

# Import the main data pull function (after logging is set up)
try:
    logger.info("Importing pull_all_ohlc_data...")
    from pull_ohlc_data import pull_all_ohlc_data
    logger.info("Successfully imported pull_all_ohlc_data")
except Exception as e:
    error_msg = f"Failed to import pull_all_ohlc_data: {e}"
    print(error_msg, file=sys.stderr, flush=True)
    print(traceback.format_exc(), file=sys.stderr, flush=True)
    if 'logger' in globals():
        logger.error(error_msg)
        logger.error(traceback.format_exc())
    sys.exit(1)

def get_friday_date(date=None):
    """
    Get the Friday date for a given date
    For weekly data, we use Friday as the week end date
    """
    if date is None:
        date = datetime.now()
    
    current_weekday = date.weekday()  # 0=Monday, 4=Friday, 6=Sunday
    
    if current_weekday == 4:  # Today is Friday
        friday = date
    elif current_weekday < 4:  # Monday-Thursday
        days_to_friday = 4 - current_weekday
        friday = date + timedelta(days=days_to_friday)
    else:  # Saturday (5) or Sunday (6)
        days_since_friday = current_weekday - 4
        friday = date - timedelta(days=days_since_friday)
    
    return friday

def load_config(config_file='study_settings/indicator_config.json'):
    """Load configuration from JSON file"""
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except Exception as e:
        logger.error(f"Error loading config file {config_file}: {e}")
        raise

def get_expected_symbol_count(symbols_file='lists_and_matrix/symbol_matrix.csv'):
    """
    Calculate expected symbol count dynamically from symbol matrix file.
    This ensures validation uses the correct count even as symbols are added/removed.
    
    Args:
        symbols_file: Path to symbol_matrix.csv file
    
    Returns:
        int: Expected number of symbols (rows) in historical files
    """
    try:
        symbol_path = Path(symbols_file)
        if not symbol_path.exists():
            logger.warning(f"Symbol matrix file not found: {symbols_file}. Using default count: 14124")
            return 14124  # Fallback to current known count
        
        df = pd.read_csv(symbol_path)
        count = len(df)
        logger.debug(f"Calculated expected symbol count from {symbols_file}: {count}")
        return count
    except Exception as e:
        logger.warning(f"Error calculating symbol count from {symbols_file}: {e}. Using default count: 14124")
        return 14124  # Fallback to current known count (176 outrights + 13,948 spreads = 14,124)

def validate_historical_file(file_path, expected_date, expected_symbol_count=None):
    """
    Validate a historical file
    
    Args:
        file_path: Path to the CSV file
        expected_date: Expected Friday date (datetime object)
        expected_symbol_count: Expected number of symbols (rows). If None, will be calculated dynamically.
    
    Returns:
        (is_valid: bool, error_message: str)
    """
    # Calculate expected count dynamically if not provided
    if expected_symbol_count is None:
        expected_symbol_count = get_expected_symbol_count()
    try:
        file_path = Path(file_path)
        
        # Check file exists
        if not file_path.exists():
            return False, "File does not exist"
        
        # Check file size (not empty)
        file_size = file_path.stat().st_size
        if file_size < 1000:  # Less than 1KB is suspicious
            return False, f"File too small ({file_size} bytes)"
        
        # Try to read the file
        try:
            df = pd.read_csv(file_path, nrows=5)  # Read first 5 rows to check structure
            if 'Date' not in df.columns:
                return False, "Missing 'Date' column"
        except Exception as e:
            return False, f"Error reading file: {str(e)}"
        
        # Read full file to validate
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            return False, f"Error reading full file: {str(e)}"
        
        # Check row count (allow some variance for symbol count changes)
        row_count = len(df)
        if row_count < expected_symbol_count * 0.9:  # Allow 10% variance
            return False, f"Row count too low: {row_count} (expected ~{expected_symbol_count})"
        
        # Check date column exists and contains expected date
        if 'Date' not in df.columns:
            return False, "Missing 'Date' column"
        
        # Convert Date column to datetime
        try:
            df['Date'] = pd.to_datetime(df['Date'])
        except Exception as e:
            return False, f"Error parsing Date column: {str(e)}"
        
        # Check that all dates match expected date
        unique_dates = df['Date'].unique()
        expected_date_str = expected_date.strftime('%Y-%m-%d')
        
        if len(unique_dates) != 1:
            return False, f"Multiple dates found: {unique_dates[:5]} (expected single date: {expected_date_str})"
        
        actual_date = unique_dates[0]
        if actual_date.date() != expected_date.date():
            return False, f"Date mismatch: {actual_date.date()} (expected: {expected_date.date()})"
        
        # All checks passed
        return True, "File is valid"
        
    except Exception as e:
        return False, f"Validation error: {str(e)}"

def scan_existing_files(output_dir='full_unfiltered_historicals'):
    """
    Scan directory for existing historical files and extract dates
    
    Returns:
        dict: {date: file_path} mapping
    """
    output_path = Path(output_dir)
    if not output_path.exists():
        logger.warning(f"Output directory does not exist: {output_dir}")
        return {}
    
    existing_files = {}
    pattern = re.compile(r'unfiltered_(\d{4}-\d{2}-\d{2})\.csv')
    
    for file_path in output_path.glob('unfiltered_*.csv'):
        match = pattern.match(file_path.name)
        if match:
            try:
                date_str = match.group(1)
                file_date = datetime.strptime(date_str, '%Y-%m-%d')
                existing_files[file_date] = file_path
            except Exception as e:
                logger.warning(f"Error parsing date from filename {file_path.name}: {e}")
    
    logger.info(f"Found {len(existing_files)} existing historical files")
    return existing_files

def calculate_required_dates(min_weeks=104, max_weeks=156):
    """
    Calculate required date ranges
    
    Returns:
        (required_dates: list, max_dates: list)
        Both are lists of Friday datetime objects
    """
    today = datetime.now()
    current_friday = get_friday_date(today)
    
    # Calculate date ranges
    required_dates = []
    max_dates = []
    
    for i in range(max_weeks):
        friday_date = current_friday - timedelta(weeks=i)
        max_dates.append(friday_date)
        if i < min_weeks:
            required_dates.append(friday_date)
    
    # Sort dates (oldest first)
    required_dates.sort()
    max_dates.sort()
    
    logger.info(f"Required dates: {len(required_dates)} weeks (from {required_dates[0].date()} to {required_dates[-1].date()})")
    logger.info(f"Maximum dates: {len(max_dates)} weeks (from {max_dates[0].date()} to {max_dates[-1].date()})")
    
    return required_dates, max_dates

def process_missing_week(target_date, output_dir, symbols_file, config_file, stats_lock, stats):
    """
    Process a single missing week
    
    Args:
        target_date: Friday date to process (datetime)
        output_dir: Output directory
        symbols_file: Path to symbols CSV
        config_file: Path to config JSON
        stats_lock: Thread lock for stats
        stats: Statistics dictionary
    
    Returns:
        (target_date, success: bool, error_message: str)
    """
    date_str = target_date.strftime('%Y-%m-%d')
    logger.info(f"Processing missing week: {date_str}")
    
    try:
        # Call the main data pull function with specific date
        output_file = pull_all_ohlc_data(
            symbols_file=symbols_file,
            weeks_back=None,  # Use config default (5 years for indicators)
            output_dir=output_dir,
            snapshot_date=date_str,  # Target specific date
            max_workers_outrights=10,
            max_workers_spreads=20,
            config_file=config_file
        )
        
        # Validate the created file
        is_valid, error_msg = validate_historical_file(
            output_file,
            target_date,
            expected_symbol_count=None  # Will be calculated dynamically from symbol matrix
        )
        
        if is_valid:
            with stats_lock:
                stats['weeks_filled'] += 1
            logger.info(f"✓ Successfully processed and validated: {date_str}")
            return (target_date, True, None)
        else:
            # Delete invalid file and retry once
            logger.warning(f"✗ File validation failed for {date_str}: {error_msg}")
            try:
                Path(output_file).unlink()
                logger.info(f"Deleted invalid file: {output_file}")
            except Exception as e:
                logger.error(f"Error deleting invalid file: {e}")
            
            # Retry once
            logger.info(f"Retrying {date_str}...")
            try:
                output_file = pull_all_ohlc_data(
                    symbols_file=symbols_file,
                    weeks_back=None,
                    output_dir=output_dir,
                    snapshot_date=date_str,
                    max_workers_outrights=10,
                    max_workers_spreads=20,
                    config_file=config_file
                )
                
                is_valid, error_msg = validate_historical_file(
                    output_file,
                    target_date,
                    expected_symbol_count=None  # Will be calculated dynamically from symbol matrix
                )
                
                if is_valid:
                    with stats_lock:
                        stats['weeks_filled'] += 1
                    logger.info(f"✓ Successfully processed on retry: {date_str}")
                    return (target_date, True, None)
                else:
                    with stats_lock:
                        stats['weeks_failed'] += 1
                        stats['failed_weeks'].append((date_str, error_msg))
                    logger.error(f"✗ Failed on retry for {date_str}: {error_msg}")
                    return (target_date, False, error_msg)
            except Exception as e:
                with stats_lock:
                    stats['weeks_failed'] += 1
                    stats['failed_weeks'].append((date_str, str(e)))
                logger.error(f"✗ Exception on retry for {date_str}: {e}")
                return (target_date, False, str(e))
        
    except Exception as e:
        with stats_lock:
            stats['weeks_failed'] += 1
            stats['failed_weeks'].append((date_str, str(e)))
        logger.error(f"✗ Exception processing {date_str}: {e}")
        logger.error(traceback.format_exc())
        return (target_date, False, str(e))

def ensure_historical_coverage(
    output_dir='full_unfiltered_historicals',
    symbols_file='lists_and_matrix/symbol_matrix.csv',
    config_file='study_settings/indicator_config.json',
    min_weeks=104,
    max_weeks=156,
    parallel_workers=2
):
    """
    Main function to ensure historical coverage
    
    Returns:
        stats: Dictionary with execution statistics
    """
    start_time = datetime.now()
    
    # Initialize statistics
    stats = {
        'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
        'status': 'UNKNOWN',
        'files_found': 0,
        'files_validated': 0,
        'files_invalid': 0,
        'weeks_missing': 0,
        'weeks_filled': 0,
        'weeks_failed': 0,
        'files_deleted_old': 0,
        'files_regenerated': 0,
        'failed_weeks': [],
        'deleted_files': [],
        'regenerated_files': []
    }
    
    logger.info("=" * 80)
    logger.info("ENSURING HISTORICAL COVERAGE")
    logger.info("=" * 80)
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Minimum weeks: {min_weeks}")
    logger.info(f"Maximum weeks: {max_weeks}")
    logger.info(f"Parallel workers: {parallel_workers}")
    
    try:
        # Step 1: Scan existing files
        logger.info("\n[Step 1] Scanning existing files...")
        existing_files = scan_existing_files(output_dir)
        stats['files_found'] = len(existing_files)
        logger.info(f"Found {len(existing_files)} existing files")
        
        # Step 2: Calculate required dates
        logger.info("\n[Step 2] Calculating required date ranges...")
        required_dates, max_dates = calculate_required_dates(min_weeks, max_weeks)
        
        # Step 3: Identify missing weeks
        logger.info("\n[Step 3] Identifying missing weeks...")
        missing_weeks = [date for date in required_dates if date not in existing_files]
        stats['weeks_missing'] = len(missing_weeks)
        logger.info(f"Missing weeks: {len(missing_weeks)}")
        if missing_weeks:
            logger.info(f"Missing dates: {[d.strftime('%Y-%m-%d') for d in missing_weeks[:10]]}...")
        
        # Step 4: Identify files to delete (older than max_weeks)
        logger.info("\n[Step 4] Identifying files to delete...")
        max_date = max_dates[0]  # Oldest date in max range
        files_to_delete = [(date, path) for date, path in existing_files.items() if date < max_date]
        stats['files_deleted_old'] = len(files_to_delete)
        logger.info(f"Files to delete (older than {max_weeks} weeks): {len(files_to_delete)}")
        if files_to_delete:
            logger.info(f"Files to delete: {[d.strftime('%Y-%m-%d') for d, _ in files_to_delete[:10]]}...")
        
        # Step 5: Delete old files
        if files_to_delete:
            logger.info("\n[Step 5] Deleting old files...")
            for date, file_path in files_to_delete:
                try:
                    file_path.unlink()
                    stats['deleted_files'].append(date.strftime('%Y-%m-%d'))
                    logger.info(f"Deleted: {file_path.name}")
                except Exception as e:
                    logger.error(f"Error deleting {file_path.name}: {e}")
        
        # Step 6: Process missing weeks in parallel
        if missing_weeks:
            logger.info(f"\n[Step 6] Processing {len(missing_weeks)} missing weeks (parallel workers: {parallel_workers})...")
            stats_lock = Lock()
            
            with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
                futures = {
                    executor.submit(
                        process_missing_week,
                        target_date,
                        output_dir,
                        symbols_file,
                        config_file,
                        stats_lock,
                        stats
                    ): target_date
                    for target_date in missing_weeks
                }
                
                # Process completed tasks
                for future in as_completed(futures):
                    target_date = futures[future]
                    try:
                        date, success, error = future.result()
                        if not success:
                            logger.error(f"Failed to process {date.strftime('%Y-%m-%d')}: {error}")
                    except Exception as e:
                        logger.error(f"Exception processing {target_date.strftime('%Y-%m-%d')}: {e}")
        
        # Step 7: Validate all existing files (in required range)
        logger.info("\n[Step 7] Validating all existing files...")
        existing_files = scan_existing_files(output_dir)  # Re-scan after processing
        files_to_validate = [
            (date, path) for date, path in existing_files.items()
            if date in required_dates or date in max_dates
        ]
        
        for date, file_path in files_to_validate:
            is_valid, error_msg = validate_historical_file(
                file_path,
                date,
                expected_symbol_count=None  # Will be calculated dynamically from symbol matrix
            )
            
            if is_valid:
                stats['files_validated'] += 1
            else:
                stats['files_invalid'] += 1
                logger.warning(f"Invalid file detected: {file_path.name} - {error_msg}")
                
                # Delete and regenerate
                try:
                    file_path.unlink()
                    logger.info(f"Deleted invalid file: {file_path.name}")
                    
                    # Regenerate
                    date_str = date.strftime('%Y-%m-%d')
                    logger.info(f"Regenerating: {date_str}")
                    output_file = pull_all_ohlc_data(
                        symbols_file=symbols_file,
                        weeks_back=None,
                        output_dir=output_dir,
                        snapshot_date=date_str,
                        max_workers_outrights=10,
                        max_workers_spreads=20,
                        config_file=config_file
                    )
                    
                    # Validate regenerated file
                    is_valid, error_msg = validate_historical_file(
                        output_file,
                        date,
                        expected_symbol_count=None  # Will be calculated dynamically from symbol matrix
                    )
                    
                    if is_valid:
                        stats['files_regenerated'] += 1
                        stats['regenerated_files'].append(date_str)
                        logger.info(f"✓ Successfully regenerated: {date_str}")
                    else:
                        logger.error(f"✗ Regeneration failed for {date_str}: {error_msg}")
                        stats['failed_weeks'].append((date_str, f"Regeneration failed: {error_msg}"))
                except Exception as e:
                    logger.error(f"Error regenerating {file_path.name}: {e}")
                    stats['failed_weeks'].append((date_str, str(e)))
        
        # Step 8: Final summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        stats['end_time'] = end_time.strftime('%Y-%m-%d %H:%M:%S')
        stats['duration'] = f"{duration/60:.1f} minutes"
        stats['duration_minutes'] = duration / 60
        
        # Determine status
        if stats['weeks_failed'] > 0 or stats['files_invalid'] > 0:
            stats['status'] = 'WARNINGS'
        elif stats['weeks_missing'] == 0 and stats['files_invalid'] == 0:
            stats['status'] = 'SUCCESS'
        else:
            stats['status'] = 'SUCCESS'
        
        logger.info("\n" + "=" * 80)
        logger.info("HISTORICAL COVERAGE SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Files found: {stats['files_found']}")
        logger.info(f"Files validated: {stats['files_validated']}")
        logger.info(f"Files invalid: {stats['files_invalid']}")
        logger.info(f"Weeks missing: {stats['weeks_missing']}")
        logger.info(f"Weeks filled: {stats['weeks_filled']}")
        logger.info(f"Weeks failed: {stats['weeks_failed']}")
        logger.info(f"Files deleted (old): {stats['files_deleted_old']}")
        logger.info(f"Files regenerated: {stats['files_regenerated']}")
        logger.info(f"Duration: {stats['duration']}")
        logger.info(f"Status: {stats['status']}")
        
        return stats
        
    except Exception as e:
        logger.error(f"Error in ensure_historical_coverage: {e}")
        logger.error(traceback.format_exc())
        stats['status'] = 'FAILED'
        stats['error'] = str(e)
        return stats

def generate_historical_coverage_html(stats):
    """Generate HTML email content for historical coverage report"""
    status = stats.get('status', 'UNKNOWN')
    if status == 'SUCCESS':
        status_symbol = '[OK]'
        status_color = '#28a745'
    elif status == 'WARNINGS':
        status_symbol = '[WARN]'
        status_color = '#ffc107'
    else:
        status_symbol = '[FAIL]'
        status_color = '#dc3545'
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; background-color: #f5f5f5; margin: 0; padding: 20px; }}
            .container {{ max-width: 900px; margin: 0 auto; background-color: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            h1 {{ color: #333; border-bottom: 3px solid #007bff; padding-bottom: 10px; }}
            h2 {{ color: #555; margin-top: 25px; border-bottom: 2px solid #e0e0e0; padding-bottom: 5px; }}
            table {{ width: 100%; border-collapse: collapse; margin: 15px 0; }}
            th {{ background-color: #007bff; color: white; padding: 10px; text-align: left; }}
            td {{ padding: 8px; border-bottom: 1px solid #e0e0e0; }}
            tr:nth-child(even) {{ background-color: #f9f9f9; }}
            .status {{ color: {status_color}; font-weight: bold; font-size: 18px; }}
            .metric {{ font-weight: bold; color: #333; }}
            .success-list {{ background-color: #d4edda; padding: 10px; border-left: 4px solid #28a745; margin: 10px 0; }}
            .warning-list {{ background-color: #fff3cd; padding: 10px; border-left: 4px solid #ffc107; margin: 10px 0; }}
            .error-list {{ background-color: #f8d7da; padding: 10px; border-left: 4px solid #dc3545; margin: 10px 0; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>HISTORICAL COVERAGE - EXECUTION SUMMARY</h1>
            
            <h2>EXECUTION DETAILS</h2>
            <table>
                <tr><td class="metric">Start Time:</td><td>{stats.get('start_time', 'N/A')}</td></tr>
                <tr><td class="metric">End Time:</td><td>{stats.get('end_time', 'N/A')}</td></tr>
                <tr><td class="metric">Duration:</td><td>{stats.get('duration', 'N/A')}</td></tr>
                <tr><td class="metric">Status:</td><td class="status">{status_symbol} {status}</td></tr>
            </table>
            
            <h2>COVERAGE STATISTICS</h2>
            <table>
                <tr><td class="metric">Files Found:</td><td>{stats.get('files_found', 0):,}</td></tr>
                <tr><td class="metric">Files Validated:</td><td>{stats.get('files_validated', 0):,}</td></tr>
                <tr><td class="metric">Files Invalid:</td><td>{stats.get('files_invalid', 0):,}</td></tr>
                <tr><td class="metric">Weeks Missing:</td><td>{stats.get('weeks_missing', 0):,}</td></tr>
                <tr><td class="metric">Weeks Filled:</td><td>{stats.get('weeks_filled', 0):,}</td></tr>
                <tr><td class="metric">Weeks Failed:</td><td>{stats.get('weeks_failed', 0):,}</td></tr>
                <tr><td class="metric">Files Deleted (Old):</td><td>{stats.get('files_deleted_old', 0):,}</td></tr>
                <tr><td class="metric">Files Regenerated:</td><td>{stats.get('files_regenerated', 0):,}</td></tr>
            </table>
    """
    
    # Add details if there are failures
    if stats.get('weeks_failed', 0) > 0 or stats.get('failed_weeks', []):
        html += """
            <h2>FAILED WEEKS</h2>
            <div class="error-list">
        """
        for date_str, error in stats.get('failed_weeks', [])[:20]:
            html += f"<strong>{date_str}:</strong> {error}<br>"
        if len(stats.get('failed_weeks', [])) > 20:
            html += f"<em>... and {len(stats.get('failed_weeks', [])) - 20} more (see log file)</em>"
        html += "</div>"
    
    # Add deleted files list
    if stats.get('deleted_files', []):
        html += """
            <h2>DELETED FILES (Older than max_weeks)</h2>
            <div class="warning-list">
        """
        for date_str in stats.get('deleted_files', [])[:20]:
            html += f"{date_str}<br>"
        if len(stats.get('deleted_files', [])) > 20:
            html += f"<em>... and {len(stats.get('deleted_files', [])) - 20} more</em>"
        html += "</div>"
    
    # Add regenerated files list
    if stats.get('regenerated_files', []):
        html += """
            <h2>REGENERATED FILES</h2>
            <div class="success-list">
        """
        for date_str in stats.get('regenerated_files', [])[:20]:
            html += f"{date_str}<br>"
        if len(stats.get('regenerated_files', [])) > 20:
            html += f"<em>... and {len(stats.get('regenerated_files', [])) - 20} more</em>"
        html += "</div>"
    
    html += """
            <div style="margin-top: 30px; padding-top: 20px; border-top: 2px solid #e0e0e0; color: #666; font-size: 12px; text-align: center;">
                <p>Generated automatically by Historical Coverage Script</p>
                <p>Report generated at """ + stats.get('end_time', 'N/A') + """</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html

def send_historical_coverage_email(stats, log_file_path=None, email_config=None):
    """Send email report for historical coverage"""
    if email_config is None:
        logger.warning("Email config not provided - skipping email send")
        return False
    
    try:
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.base import MIMEBase
        from email import encoders
        import smtplib
        
        msg = MIMEMultipart()
        msg['From'] = email_config['SMTP_SENDER']
        msg['To'] = email_config['SMTP_TO']
        
        status = stats.get('status', 'UNKNOWN')
        date_str = datetime.now().strftime('%Y-%m-%d %H:%M')
        env_label = email_config.get('ENV_LABEL', 'PROD')
        status_symbol = '[OK]' if status == 'SUCCESS' else '[WARN]'
        
        msg['Subject'] = f"[{env_label}] Historical Coverage - {status_symbol} - {date_str}"
        
        html_body = generate_historical_coverage_html(stats)
        msg.attach(MIMEText(html_body, 'html'))
        
        # Attach log file if provided
        if log_file_path and Path(log_file_path).exists():
            try:
                log_size_mb = Path(log_file_path).stat().st_size / (1024 * 1024)
                if log_size_mb < 10:
                    with open(log_file_path, 'rb') as f:
                        part = MIMEBase('application', 'octet-stream')
                        part.set_payload(f.read())
                        encoders.encode_base64(part)
                        part.add_header(
                            'Content-Disposition',
                            f'attachment; filename= {Path(log_file_path).name}'
                        )
                        msg.attach(part)
            except Exception as e:
                logger.warning(f"Failed to attach log file: {e}")
        
        # Send email
        smtp_host = email_config['SMTP_HOST']
        smtp_port = int(email_config['SMTP_PORT'])
        smtp_user = email_config['SMTP_USER']
        smtp_pass = email_config['SMTP_PASS']
        
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        
        logger.info("Email sent successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error sending email: {e}")
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    try:
        print("Starting historical coverage script...", flush=True)
        sys.stdout.flush()
        
        # Load configuration
        print("Loading configuration...", flush=True)
        config = load_config()
        historical_config = config.get('historical_coverage', {})
        
        min_weeks = historical_config.get('min_weeks', 104)
        max_weeks = historical_config.get('max_weeks', 156)
        parallel_workers = historical_config.get('parallel_workers', 2)
        
        print(f"Configuration loaded: min_weeks={min_weeks}, max_weeks={max_weeks}, parallel_workers={parallel_workers}", flush=True)
        
        # Run the coverage check
        print("Running ensure_historical_coverage...", flush=True)
        stats = ensure_historical_coverage(
            output_dir='full_unfiltered_historicals',
            symbols_file='lists_and_matrix/symbol_matrix.csv',
            config_file='study_settings/indicator_config.json',
            min_weeks=min_weeks,
            max_weeks=max_weeks,
            parallel_workers=parallel_workers
        )
        
        print(f"Coverage check completed. Status: {stats.get('status', 'UNKNOWN')}", flush=True)
        
        # Send email report
        email_config = load_email_config()
        if email_config:
            print("Sending email report...", flush=True)
            send_historical_coverage_email(stats, log_file, email_config)
        else:
            logger.info("Email notifications disabled (no email config found)")
        
        print("Script completed successfully.", flush=True)
        sys.exit(0)
        
    except Exception as e:
        error_msg = f"Fatal error in main execution: {e}"
        print(error_msg, flush=True)
        print(traceback.format_exc(), flush=True)
        if 'logger' in globals():
            logger.error(error_msg)
            logger.error(traceback.format_exc())
        sys.exit(1)

