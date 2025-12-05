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
import json
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from threading import Lock, Thread, Event
import pythoncom  # For COM initialization in threads
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os

# Import pandas_ta for technical indicators
try:
    import pandas_ta as ta
    PANDAS_TA_AVAILABLE = True
except ImportError:
    PANDAS_TA_AVAILABLE = False
    # Logger not initialized yet, will log warning in function if needed

# Import statsmodels for cointegration tests
try:
    from statsmodels.tsa.stattools import coint
    STATSMODELS_AVAILABLE = True
except ImportError:
    STATSMODELS_AVAILABLE = False
    # Logger not initialized yet, will log warning in function if needed

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

# Only setup logging if running as standalone script
# When imported by ensure_historical_coverage, use the external logger instead
# DO NOT clear handlers or setup logging when imported - let the importing module handle it
if __name__ == "__main__":
    logger, log_file = setup_logging('logs/ice_data_pull')
else:
    # When imported, create a logger that uses the root logger's handlers
    # This way it will use whatever logging setup the importing module created
    logger = logging.getLogger(__name__)
    log_file = None
    # Don't add any handlers - use the root logger's handlers from the importing module

# OHLC fields for weekly candles
# Include 'Recent Settlement' for incomplete weeks where Close might be None
OHLC_FIELDS = ['Open', 'High', 'Low', 'Close', 'Recent Settlement']


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


def detect_most_recent_ice_date(symbol='%PRL 1!-IEU', cache_hours=12, stale_warning_days=7):
    """
    Detect the most recent date available in ICE data by querying a test symbol.
    Uses caching to avoid unnecessary API calls.
    
    Args:
        symbol: ICE symbol to use for detection (default: '%PRL 1!-IEU' - NGL, more stable)
        cache_hours: How long to trust cached date before re-detecting (default: 12)
        stale_warning_days: Warn if detected date is older than this many days (default: 7)
    
    Returns:
        datetime object representing the most recent Friday date available in ICE data
    """
    from pathlib import Path
    import json
    
    cache_dir = Path('cache')
    cache_dir.mkdir(exist_ok=True)
    cache_file = cache_dir / 'latest_date_cache.json'
    
    # Check cache first
    if cache_file.exists():
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            cached_date_str = cache_data.get('date')
            detected_at_str = cache_data.get('detected_at')
            
            if cached_date_str and detected_at_str:
                cached_date = datetime.strptime(cached_date_str, '%Y-%m-%d')
                detected_at = datetime.strptime(detected_at_str, '%Y-%m-%d %H:%M:%S')
                
                # Check if cache is still fresh
                hours_since_detection = (datetime.now() - detected_at).total_seconds() / 3600
                
                if hours_since_detection < cache_hours:
                    logger.info(f"Using cached most recent date: {cached_date.date()} (detected {hours_since_detection:.1f} hours ago)")
                    return cached_date
                else:
                    logger.debug(f"Cache expired ({hours_since_detection:.1f} hours old, threshold: {cache_hours} hours). Re-detecting...")
        except Exception as e:
            logger.warning(f"Error reading date cache: {e}. Will re-detect.")
    
    # Cache miss or expired - detect from ICE API
    logger.info(f"Detecting most recent ICE data date using symbol: {symbol}")
    
    try:
        # Fetch last 6 weeks of data to find the most recent date
        end_date = datetime.now()
        start_date = end_date - timedelta(weeks=6)
        
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        logger.debug(f"Fetching detection data: {start_str} to {end_str}")
        
        result = ice.get_timeseries(
            [symbol],
            ['Close'],  # Only need one field for date detection
            'W',  # Weekly
            start_str,
            end_str
        )
        
        if result is None or len(result) == 0:
            logger.warning(f"No data returned for detection symbol {symbol}. Using calculated Friday date.")
            # Fallback to calculated Friday
            detected_date = get_friday_date()
        else:
            # Find the most recent date in the results
            dates = []
            for row in result:
                if row and len(row) > 0:
                    date_val = row[0]  # First element is date
                    if date_val:
                        try:
                            date_obj = pd.to_datetime(date_val)
                            dates.append(date_obj)
                        except:
                            pass
            
            if dates:
                detected_date = max(dates)
                logger.info(f"✓ Detected most recent ICE data date: {detected_date.date()}")
            else:
                logger.warning(f"Could not parse dates from detection result. Using calculated Friday date.")
                detected_date = get_friday_date()
        
        # Check if date is stale and warn
        days_old = (datetime.now().date() - detected_date.date()).days
        if days_old > stale_warning_days:
            logger.warning(f"⚠️  WARNING: Detected date ({detected_date.date()}) is {days_old} days old.")
            logger.warning(f"    This may indicate data availability issues. Expected date should be within {stale_warning_days} days.")
        else:
            logger.debug(f"Detected date is {days_old} days old (within {stale_warning_days} day threshold)")
        
        # Save to cache
        try:
            cache_data = {
                'date': detected_date.strftime('%Y-%m-%d'),
                'detected_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'symbol_used': symbol
            }
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2)
            logger.debug(f"Cached detected date: {cache_file}")
        except Exception as e:
            logger.warning(f"Failed to save date cache: {e}")
        
        return detected_date
        
    except Exception as e:
        logger.error(f"Error detecting most recent ICE date: {e}", exc_info=True)
        logger.warning("Falling back to calculated Friday date")
        return get_friday_date()


def load_email_config(email_config_file='email_settings/Email.env'):
    """
    Load email configuration from .env file
    
    Args:
        email_config_file: Path to email configuration file
    
    Returns:
        Dictionary with email settings, or None if file not found
    """
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


def generate_email_html(stats):
    """
    Generate HTML email content from statistics
    
    Args:
        stats: Dictionary with all execution statistics
    
    Returns:
        HTML string for email body
    """
    # Determine status symbol and color
    if stats.get('status') == 'SUCCESS':
        status_symbol = '[OK]'
        status_color = '#28a745'
    elif stats.get('status') == 'WARNINGS':
        status_symbol = '[WARN]'
        status_color = '#ffc107'
    else:
        status_symbol = '[FAIL]'
        status_color = '#dc3545'
    
    # Check for errors/warnings to display at top
    error_count = stats.get('error_count', 0)
    warning_count = stats.get('warning_count', 0)
    failed_symbols = stats.get('failed_symbols', [])
    has_errors = error_count > 0 or warning_count > 0 or len(failed_symbols) > 0
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
            .container {{ max-width: 900px; margin: 0 auto; background-color: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            h1 {{ color: #333; border-bottom: 3px solid #007bff; padding-bottom: 10px; }}
            h2 {{ color: #555; margin-top: 25px; border-bottom: 2px solid #e0e0e0; padding-bottom: 5px; }}
            table {{ width: 100%; border-collapse: collapse; margin: 15px 0; }}
            th {{ background-color: #007bff; color: white; padding: 10px; text-align: left; }}
            td {{ padding: 8px; border-bottom: 1px solid #e0e0e0; }}
            tr:nth-child(even) {{ background-color: #f9f9f9; }}
            .status {{ color: {status_color}; font-weight: bold; font-size: 18px; }}
            .metric {{ font-weight: bold; color: #333; }}
            .error-banner {{ background: linear-gradient(135deg, #dc3545 0%, #c82333 100%); color: white; padding: 20px; border-radius: 8px; margin: 20px 0; box-shadow: 0 4px 6px rgba(220, 53, 69, 0.3); }}
            .error-banner h2 {{ color: white; border-bottom: 2px solid rgba(255,255,255,0.3); margin-top: 0; }}
            .error-banner strong {{ font-size: 18px; display: block; margin-bottom: 10px; }}
            .error-list {{ background-color: #fff3cd; padding: 15px; border-left: 5px solid #ffc107; margin: 10px 0; border-radius: 4px; }}
            .warning-banner {{ background: linear-gradient(135deg, #ffc107 0%, #e0a800 100%); color: #856404; padding: 20px; border-radius: 8px; margin: 20px 0; box-shadow: 0 4px 6px rgba(255, 193, 7, 0.3); }}
            .warning-banner h2 {{ color: #856404; border-bottom: 2px solid rgba(133, 100, 4, 0.3); margin-top: 0; }}
            .success-list {{ background-color: #d4edda; padding: 10px; border-left: 4px solid #28a745; margin: 10px 0; }}
            .footer {{ margin-top: 30px; padding-top: 20px; border-top: 2px solid #e0e0e0; color: #666; font-size: 12px; text-align: center; }}
            .failed-symbol {{ background-color: rgba(255,255,255,0.2); padding: 5px 10px; margin: 3px 0; border-radius: 3px; display: inline-block; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>ICE OHLC DATA PULL - EXECUTION SUMMARY</h1>
            
            <h2>EXECUTION DETAILS</h2>
            <table>
                <tr><td class="metric">Start Time:</td><td>{stats.get('start_time', 'N/A')}</td></tr>
                <tr><td class="metric">End Time:</td><td>{stats.get('end_time', 'N/A')}</td></tr>
                <tr><td class="metric">Total Duration:</td><td>{stats.get('duration', 'N/A')} ({stats.get('duration_minutes', 0):.1f} minutes)</td></tr>
                <tr><td class="metric">Status:</td><td class="status">{status_symbol} {stats.get('status', 'UNKNOWN')}</td></tr>
            </table>
    """
    
    # Add prominent error/warning banner at the top if there are any issues
    if has_errors:
        if error_count > 0:
            html += f"""
            <div class="error-banner">
                <h2>ERRORS DETECTED - ACTION REQUIRED</h2>
                <strong>WARNING: {error_count} errors found during execution</strong>
                <p style="margin: 10px 0 0 0;">
                    • {len(failed_symbols)} symbols failed to fetch<br>
                    • {warning_count} warnings generated<br>
                    Please review the details below and check the attached log file for more information.
                </p>
            </div>
            """
        elif warning_count > 0 or len(failed_symbols) > 0:
            html += f"""
            <div class="warning-banner">
                <h2>WARNINGS DETECTED</h2>
                <strong>WARNING: {len(failed_symbols)} symbols failed to fetch</strong>
                <p style="margin: 10px 0 0 0;">
                    • {warning_count} warnings generated<br>
                    Please review the details below.
                </p>
            </div>
            """
    
    # Continue building HTML with main content sections
    html += f"""
            <h2>DATA PULL STATISTICS</h2>
            <table>
                <tr><td class="metric">Total Symbols:</td><td>{stats.get('total_symbols', 0):,}</td></tr>
                <tr><td class="metric">Outrights:</td><td>{stats.get('outrights_total', 0):,}</td></tr>
                <tr><td class="metric">  ├─ Success:</td><td>{stats.get('outrights_success', 0):,} ({stats.get('outrights_success_pct', 0):.1f}%)</td></tr>
                <tr><td class="metric">  └─ Failed:</td><td>{stats.get('outrights_failed', 0):,} ({stats.get('outrights_failed_pct', 0):.1f}%)</td></tr>
                <tr><td class="metric">Spreads:</td><td>{stats.get('spreads_total', 0):,}</td></tr>
                <tr><td class="metric">  ├─ Success:</td><td>{stats.get('spreads_success', 0):,} ({stats.get('spreads_success_pct', 0):.1f}%)</td></tr>
                <tr><td class="metric">  └─ Failed:</td><td>{stats.get('spreads_failed', 0):,} ({stats.get('spreads_failed_pct', 0):.1f}%)</td></tr>
            </table>
            
            <h2>DATA QUALITY METRICS</h2>
            <table>
                <tr><td class="metric">Date Range:</td><td>{stats.get('earliest_date', 'N/A')} to {stats.get('latest_date', 'N/A')}</td></tr>
                <tr><td class="metric">Total Rows:</td><td>{stats.get('total_rows', 0):,}</td></tr>
                <tr><td class="metric">Unique Symbols:</td><td>{stats.get('unique_symbols', 0):,}</td></tr>
                <tr><td class="metric">Data Points Avg:</td><td>{stats.get('avg_data_points', 0):.1f} weeks per symbol</td></tr>
                <tr><td class="metric">Missing Data:</td><td>{stats.get('missing_data_pct', 0):.1f}% (NaN values)</td></tr>
            </table>
            
            <h2>INDICATOR CALCULATION SUMMARY</h2>
            <div class="success-list">
                <strong>All 8 phases completed:</strong><br>
                ✓ Moving Averages (EMAs: 10, 20, 50, 100, 200)<br>
                ✓ Trend Indicators (ADX, SuperTrend)<br>
                ✓ Momentum (MACD, RSI, Stochastic, ROC)<br>
                ✓ Oscillators (CCI, Williams %R, Aroon)<br>
                ✓ Volatility (Bollinger Bands, ATR, Historical Vol)<br>
                ✓ Statistical (Z-score, CV, Percentiles)<br>
                ✓ 4-Factor Markov Model
            </div>
            
            <h2>OUTPUT FILE INFORMATION</h2>
            <table>
                <tr><td class="metric">Output File:</td><td>{stats.get('output_file', 'N/A')}</td></tr>
                <tr><td class="metric">File Size:</td><td>{stats.get('file_size_mb', 0):.2f} MB</td></tr>
                <tr><td class="metric">Rows Written:</td><td>{stats.get('rows_written', 0):,}</td></tr>
                <tr><td class="metric">Total Columns:</td><td>{stats.get('total_columns', 0)} (including all indicators)</td></tr>
            </table>
            
            <h2>PERFORMANCE METRICS</h2>
            <table>
                <tr><td class="metric">Outright Fetch:</td><td>{stats.get('outright_duration', 0):.1f}s ({stats.get('outright_rate', 0):.1f} symbols/sec)</td></tr>
                <tr><td class="metric">Spread Calculation:</td><td>{stats.get('spread_duration', 0):.1f}s ({stats.get('spread_rate', 0):.1f} spreads/sec)</td></tr>
                <tr><td class="metric">Indicator Calc:</td><td>{stats.get('indicator_duration', 0):.1f}s</td></tr>
                <tr><td class="metric">File Write:</td><td>{stats.get('file_write_duration', 0):.1f}s</td></tr>
                <tr><td class="metric">Total Processing:</td><td>{stats.get('total_duration', 0):.1f}s</td></tr>
            </table>
    """
    
    # Add detailed errors and warnings section (after banner at top)
    if has_errors:
        html += """
            <h2>ERROR & WARNING DETAILS</h2>
            <div class="error-list">
        """
        html += f"<strong>Summary:</strong><br>"
        html += f"• {error_count} errors<br>"
        html += f"• {warning_count} warnings<br>"
        html += f"• {len(failed_symbols)} symbols failed to fetch<br><br>"
        
        if len(failed_symbols) > 0:
            html += "<strong>Failed Symbols (first 30):</strong><br>"
            html += "<div style='max-height: 200px; overflow-y: auto; background-color: rgba(255,255,255,0.5); padding: 10px; border-radius: 4px;'>"
            for symbol in failed_symbols[:30]:
                html += f"<span class='failed-symbol'>{symbol}</span> "
            if len(failed_symbols) > 30:
                html += f"<br><br><em>... and {len(failed_symbols) - 30} more (see log file for complete list)</em>"
            html += "</div>"
        
        html += "</div>"
    
    # Add configuration section
    html += f"""
            <h2>CONFIGURATION USED</h2>
            <table>
                <tr><td class="metric">Config File:</td><td>{stats.get('config_file', 'N/A')}</td></tr>
                <tr><td class="metric">Years Back:</td><td>{stats.get('years_back', 0)} ({stats.get('weeks_back', 0)} weeks)</td></tr>
                <tr><td class="metric">Workers (Outrights):</td><td>{stats.get('max_workers_outrights', 0)}</td></tr>
                <tr><td class="metric">Workers (Spreads):</td><td>{stats.get('max_workers_spreads', 0)}</td></tr>
                <tr><td class="metric">Symbols File:</td><td>{stats.get('symbols_file', 'N/A')}</td></tr>
            </table>
            
            <div class="footer">
                <p>Generated automatically by ICE OHLC Data Pull Script</p>
                <p>Report generated at {stats.get('end_time', 'N/A')}</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html


def send_summary_email(stats, log_file_path=None, output_file_path=None, email_config=None):
    """
    Send HTML email summary with attachments
    
    Args:
        stats: Dictionary with execution statistics
        log_file_path: Path to log file to attach
        output_file_path: Path to output CSV file to attach
        email_config: Email configuration dictionary
    
    Returns:
        True if email sent successfully, False otherwise
    """
    if email_config is None:
        logger.warning("Email config not provided - skipping email send")
        return False
    
    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = email_config['SMTP_SENDER']
        msg['To'] = email_config['SMTP_TO']
        
        # Determine subject line
        status = stats.get('status', 'UNKNOWN')
        total_symbols = stats.get('total_symbols', 0)
        date_str = datetime.now().strftime('%Y-%m-%d %H:%M')
        env_label = email_config.get('ENV_LABEL', '')
        if env_label:
            subject = f"[{env_label}] ICE Data Pull - {status} - {date_str} - {total_symbols:,} symbols"
        else:
            subject = f"ICE Data Pull - {status} - {date_str} - {total_symbols:,} symbols"
        
        msg['Subject'] = subject
        
        # Generate HTML body
        html_body = generate_email_html(stats)
        msg.attach(MIMEText(html_body, 'html'))
        
        # Attach log file if provided (only if < 10MB to avoid email size issues)
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
                    logger.debug(f"Attached log file: {log_file_path} ({log_size_mb:.2f} MB)")
                else:
                    logger.info(f"Log file too large to attach ({log_size_mb:.2f} MB > 10 MB limit). Log location: {log_file_path}")
            except Exception as e:
                logger.warning(f"Failed to attach log file: {e}")
        
        # Attach output CSV file if provided
        if output_file_path and Path(output_file_path).exists():
            try:
                # Check file size - only attach if < 25MB (email size limit)
                file_size_mb = Path(output_file_path).stat().st_size / (1024 * 1024)
                if file_size_mb < 25:
                    with open(output_file_path, 'rb') as f:
                        part = MIMEBase('application', 'octet-stream')
                        part.set_payload(f.read())
                        encoders.encode_base64(part)
                        part.add_header(
                            'Content-Disposition',
                            f'attachment; filename= {Path(output_file_path).name}'
                        )
                        msg.attach(part)
                    logger.debug(f"Attached output file: {output_file_path} ({file_size_mb:.2f} MB)")
                else:
                    logger.info(f"Output file too large to attach ({file_size_mb:.2f} MB > 25 MB limit)")
            except Exception as e:
                logger.warning(f"Failed to attach output file: {e}")
        
        # Send email
        smtp_host = email_config['SMTP_HOST']
        smtp_port = int(email_config['SMTP_PORT'])
        smtp_user = email_config['SMTP_USER']
        smtp_pass = email_config['SMTP_PASS']
        smtp_to = email_config['SMTP_TO']
        smtp_from = email_config['SMTP_SENDER']
        
        logger.info(f"Sending email summary to {smtp_to}...")
        logger.debug(f"SMTP settings: Host={smtp_host}, Port={smtp_port}, User={smtp_user}, From={smtp_from}, To={smtp_to}")
        
        try:
            server = smtplib.SMTP(smtp_host, smtp_port, timeout=30)
            logger.debug("SMTP connection established")
            
            server.starttls()
            logger.debug("TLS started")
            
            server.login(smtp_user, smtp_pass)
            logger.debug("SMTP login successful")
            
            text = msg.as_string()
            
            # Log message size for debugging
            msg_size = len(text.encode('utf-8'))
            logger.debug(f"Email message size: {msg_size:,} bytes ({msg_size/1024:.2f} KB)")
            
            # Send email and capture response
            logger.debug(f"Sending email via SMTP sendmail...")
            send_result = server.sendmail(smtp_from, [smtp_to], text)
            
            # sendmail returns a dictionary of failed recipients (empty dict = success)
            # For Mimecast, empty dict means the server accepted the message for delivery
            if send_result:
                logger.warning(f"SMTP sendmail returned non-empty result (some recipients may have failed): {send_result}")
                for failed_recipient, error_info in send_result.items():
                    logger.error(f"  Failed recipient: {failed_recipient}, Error: {error_info}")
                # If there are failures, return False
                server.quit()
                return False
            else:
                logger.info("SMTP server accepted email for delivery (empty result dict = success)")
                logger.debug("All recipients accepted by SMTP server")
            
            # Get final server response before quitting
            try:
                server.quit()
                logger.debug("SMTP connection closed gracefully")
            except Exception as e:
                logger.warning(f"Error closing SMTP connection: {e}")
            
            logger.info(f"✓ Email summary sent successfully to {smtp_to}")
            logger.info(f"  Subject: {subject}")
            logger.info(f"  From: {smtp_from}")
            logger.info(f"  To: {smtp_to}")
            logger.info(f"  Message size: {msg_size/1024:.2f} KB")
            logger.info(f"")
            logger.info(f"  IMPORTANT: If you don't see the email:")
            logger.info(f"    1. Check your spam/junk folder")
            logger.info(f"    2. Check Mimecast quarantine (if enabled)")
            logger.info(f"    3. Verify {smtp_to} is correct")
            logger.info(f"    4. Check Mimecast email logs for delivery status")
            return True
        except smtplib.SMTPAuthenticationError as e:
            logger.error(f"SMTP Authentication failed: {e}")
            logger.error(f"  Check username/password for {smtp_user}")
            return False
        except smtplib.SMTPRecipientsRefused as e:
            logger.error(f"SMTP Recipient refused: {e}")
            logger.error(f"  Check recipient email address: {smtp_to}")
            return False
        except smtplib.SMTPServerDisconnected as e:
            logger.error(f"SMTP Server disconnected: {e}")
            logger.error(f"  Check SMTP host/port: {smtp_host}:{smtp_port}")
            return False
        except Exception as e:
            logger.error(f"Unexpected SMTP error: {e}")
            raise
        
    except Exception as e:
        logger.error(f"Failed to send email: {e}", exc_info=True)
        return False


def load_indicator_config(config_file='study_settings/indicator_config.json'):
    """
    Load indicator configuration from JSON file
    
    Args:
        config_file: Path to configuration JSON file
    
    Returns:
        Dictionary with all indicator settings
    """
    config_path = Path(config_file)
    
    if not config_path.exists():
        logger.warning(f"Config file not found: {config_file}. Using defaults.")
        # Return default config if file doesn't exist
        return {
            "data_settings": {"years_back": 5, "min_weeks_for_indicators": 200},
            "moving_averages": {"ema_periods": [10, 20, 50, 100, 200]},
            "trend_indicators": {
                "adx": {"period": 14},
                "supertrend": {"atr_period": 10, "multiplier": 3.0},
                "parabolic_sar": {"step": 0.02, "max_step": 0.2}
            },
            "momentum_indicators": {
                "macd": {"fast": 12, "slow": 26, "signal": 9},
                "rsi": {"period": 14, "overbought": 70, "oversold": 30},
                "stochastic": {"k_period": 14, "d_period": 3, "smooth_k": 3, "overbought": 80, "oversold": 20},
                "roc": {"periods": [1, 5, 10, 20, 50]}
            },
            "oscillators": {
                "cci": {"period": 20, "overbought": 100, "oversold": -100},
                "williams_r": {"period": 14, "overbought": -20, "oversold": -80}
            },
            "aroon": {"period": 14, "strong_uptrend_threshold": 70, "strong_downtrend_threshold": 70},
            "volatility": {
                "bollinger_bands": {"period": 20, "std_dev": 2.0},
                "atr": {"period": 14},
                "historical_volatility": {"period": 20}
            },
            "statistical": {
                "zscore": {"period": 52},
                "coefficient_of_variation": {"period": 52},
                "percentiles": {"lookback_years": 5, "lookback_weeks": 260, "indicators": ["close", "rsi", "macd_line", "macd_signal", "macd_histogram"]}
            },
            "markov_model": {
                "state_classification": {"adx_strong_threshold": 25, "rsi_overbought": 70, "rsi_oversold": 30, "ema_short": 50, "ema_medium": 100, "ema_long": 200},
                "transition_matrix": {"lookback_weeks": 52}
            }
        }
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        logger.info(f"Loaded indicator configuration from: {config_file}")
        logger.debug(f"Config keys: {list(config.keys())}")
        return config
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON config file {config_file}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error loading config file {config_file}: {e}")
        raise


def fetch_symbol_ohlc(symbol, start_date, end_date, timeout_seconds=60):
    """
    Fetch OHLC data for a single symbol from ICE API
    
    Args:
        symbol: ICE symbol (e.g., '%PRL F!-IEU' or '=('PRL F!-IEU')-('PRN F!-IEU')')
        start_date: Start date (datetime)
        end_date: End date (datetime)
        timeout_seconds: Maximum time to wait for API call (default: 60 seconds)
    
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
        # Note: This function is only called for outrights now (spreads are calculated)
        # COM is already initialized in the main thread (in pull_all_ohlc_data)
        # Call ICE API directly - no threading wrapper to avoid ICE library threading issues
        logger.debug(f"ICE API call: get_timeseries([{symbol}], {OHLC_FIELDS}, 'W', '{start_str}', '{end_str}')")
        logger.info(f"Calling ICE API for {symbol}...")
        
        try:
            # Direct call to ICE API - COM already initialized in main thread
            # NOTE: ICE API has a maximum record limit of 2500 data points per request
            # We're requesting ~104 weeks (104 data points) per symbol, which is well under the limit
            result = ice.get_timeseries(
                [symbol],
                OHLC_FIELDS,
                'W',  # Weekly granularity
                start_str,
                end_str
            )
        except (SystemError, RuntimeError) as sys_error:
            # Catch .NET/COM exceptions (System.NullReferenceException, etc.)
            fetch_duration = (datetime.now() - fetch_start_time).total_seconds()
            logger.error(f"✗ ICE API System/Runtime Error for {symbol} after {fetch_duration:.2f}s: {sys_error}")
            logger.error(f"  Error type: {type(sys_error).__name__}")
            logger.error(f"  This may indicate ICE Connect/ICE XL is not running or there's a COM issue")
            return None
        except Exception as api_error:
            fetch_duration = (datetime.now() - fetch_start_time).total_seconds()
            logger.error(f"✗ ICE API call FAILED for {symbol} after {fetch_duration:.2f}s: {api_error}")
            logger.error(f"  Error type: {type(api_error).__name__}")
            logger.error(traceback.format_exc())
            return None
        
        fetch_duration = (datetime.now() - fetch_start_time).total_seconds()
        logger.debug(f"ICE API response received in {fetch_duration:.2f}s for {symbol}")
        if fetch_duration > 30:
            logger.warning(f"⚠️  Slow API response for {symbol}: {fetch_duration:.2f}s")
        
        if result is None:
            logger.warning(f"No data returned (None) for {symbol}")
            return None
        
        if len(result) == 0:
            logger.warning(f"Empty result for {symbol}")
            return None
        
        logger.debug(f"ICE API returned {len(result)} rows for {symbol}")
        
        # DIAGNOSTIC: Log the last few rows to see what dates we're getting
        if len(result) > 0:
            logger.debug(f"  Last 3 rows from API (raw): {result[-3:] if len(result) >= 3 else result[-len(result):]}")
        
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
                            # Store field name with spaces replaced by underscores for easier access
                            field_key = field.lower().replace(' ', '_')
                            row_data[field_key] = float(value)
                            logger.debug(f"    {field}: {value}")
                        except (ValueError, TypeError) as e:
                            field_key = field.lower().replace(' ', '_')
                            row_data[field_key] = None
                            logger.debug(f"    {field}: {value} (invalid: {e})")
                    else:
                        field_key = field.lower().replace(' ', '_')
                        row_data[field_key] = None
                        logger.debug(f"    {field}: None/empty")
                else:
                    field_key = field.lower().replace(' ', '_')
                    row_data[field_key] = None
                    logger.debug(f"    {field}: Missing (index {field_idx} >= {len(row)})")
            
            # For incomplete weeks, use Recent Settlement as Close if Close is None
            if row_data.get('close') is None and row_data.get('recent_settlement') is not None:
                row_data['close'] = row_data['recent_settlement']
                logger.debug(f"  Using Recent Settlement ({row_data['recent_settlement']}) as Close for incomplete week")
            
            # Add row if we have at least one OHLC value (don't require Close)
            # This allows us to capture 12/5 data even if Close is None (week not finalized)
            has_any_ohlc = any(row_data.get(field) is not None for field in ['open', 'high', 'low', 'close'])
            
            if has_any_ohlc:
                row_dict = {
                    'Date': pd.to_datetime(date),
                    'open': row_data.get('open'),
                    'high': row_data.get('high'),
                    'low': row_data.get('low'),
                    'close': row_data.get('close')
                }
                rows.append(row_dict)
                valid_rows += 1
                if row_data.get('close') is None:
                    logger.debug(f"  Row {idx}: Valid OHLC added (Close=None, but has other values)")
                else:
                    logger.debug(f"  Row {idx}: Valid OHLC added")
            else:
                invalid_rows += 1
                logger.debug(f"  Row {idx}: Invalid (no OHLC values)")
        
        logger.debug(f"Parsed {valid_rows} valid rows, {invalid_rows} invalid rows for {symbol}")
        
        if len(rows) == 0:
            logger.warning(f"No valid OHLC data for {symbol} (all {len(result)} rows were invalid)")
            return None
        
        # Create DataFrame
        df = pd.DataFrame(rows)
        df = df.set_index('Date')
        df = df.sort_index()
        
        # For incomplete weeks (Close=None), forward fill from last known Close
        # Note: Recent Settlement should have been used above, but this is a fallback
        if 'close' in df.columns:
            df['close'] = df['close'].ffill()  # Forward fill from last known Close
        
        # Calculate data completeness
        ohlc_complete = df[['open', 'high', 'low', 'close']].notna().all(axis=1).sum()
        ohlc_partial = len(df) - ohlc_complete
        
        logger.info(f"✓ {symbol}: {len(df)} data points ({ohlc_complete} complete OHLC, {ohlc_partial} partial) - {start_date.date()} to {end_date.date()}")
        logger.debug(f"  Date range in returned data: {df.index.min().date()} to {df.index.max().date()}")
        logger.debug(f"  OHLC completeness: {ohlc_complete}/{len(df)} rows have all OHLC values")
        
        # DIAGNOSTIC: Log the last few dates to see what we actually got
        if len(df) > 0:
            last_dates = df.index[-3:].tolist() if len(df) >= 3 else df.index.tolist()
            logger.debug(f"  Last 3 dates in returned data: {[d.date() for d in last_dates]}")
        
        return df
        
    except Exception as e:
        fetch_duration = (datetime.now() - fetch_start_time).total_seconds() if 'fetch_start_time' in locals() else 0
        logger.error(f"✗ Error fetching data for {symbol} after {fetch_duration:.2f}s: {e}", exc_info=True)
        logger.error(f"  Error type: {type(e).__name__}")
        logger.error(traceback.format_exc())
        return None


def apply_conversion_factor(df, conversion_factor):
    """
    Apply conversion factor to OHLC DataFrame to convert to $/usg
    
    Args:
        df: DataFrame with OHLC columns (open, high, low, close)
        conversion_factor: Conversion factor string (e.g., '/521', '/42') or 'n/a' for no conversion
    
    Returns:
        DataFrame with converted OHLC values
    """
    if df is None or len(df) == 0:
        return df
    
    if conversion_factor == 'n/a' or conversion_factor == '' or conversion_factor is None:
        # No conversion needed - already in $/usg
        return df
    
    # Parse conversion factor (e.g., '/521' -> divide by 521)
    if conversion_factor.startswith('/'):
        try:
            divisor = float(conversion_factor[1:])
            df_converted = df.copy()
            for col in ['open', 'high', 'low', 'close']:
                if col in df_converted.columns:
                    df_converted[col] = df_converted[col] / divisor
            return df_converted
        except (ValueError, TypeError) as e:
            logger.warning(f"Invalid conversion factor '{conversion_factor}': {e}")
            return df
    else:
        logger.warning(f"Unknown conversion factor format: '{conversion_factor}'")
        return df


def find_symbol_in_outright_dict(symbol, outright_data_dict, spread_row_meta=None, is_symbol_a=True):
    """
    Helper function to find a symbol (including quarterlies) in outright_data_dict
    Handles quarterlies stored with conversion factors appended
    
    Args:
        symbol: Symbol to look up (may be quarterly formula starting with '=')
        outright_data_dict: Dictionary of outright DataFrames {symbol: DataFrame}
        spread_row_meta: Optional spread row metadata (for conversion factors)
        is_symbol_a: True if this is symbol_a, False if symbol_b
    
    Returns:
        Symbol key found in outright_data_dict, or original symbol if not found
    """
    if not symbol:
        return symbol
    
    # If not a quarterly formula, try direct lookup
    if not symbol.startswith('='):
        if symbol in outright_data_dict:
            return symbol
        return symbol
    
    # For quarterly formulas, try multiple lookup strategies
    # First try direct lookup (quarterly might be stored without conversion)
    if symbol in outright_data_dict:
        return symbol
    
    # Try with conversion from spread row metadata if available
    if spread_row_meta is not None:
        conversion_key = 'convert_to_$usg' if is_symbol_a else 'convert_to_$usg_2'
        conversion = spread_row_meta.get(conversion_key, '')
        
        if conversion and conversion != 'n/a' and conversion != '':
            try_symbol = f"{symbol}{conversion}"
            if try_symbol in outright_data_dict:
                logger.debug(f"  Found quarterly for correlation with conversion: {symbol} -> {try_symbol}")
                return try_symbol
    
    # Try common conversions as fallback
    for conv in ['/521', '/42']:
        if not symbol.endswith(conv):
            try_symbol = f"{symbol}{conv}"
            if try_symbol in outright_data_dict:
                logger.debug(f"  Found quarterly for correlation with common conversion: {symbol} -> {try_symbol}")
                return try_symbol
    
    # Not found, return original (will cause lookup to fail downstream)
    logger.debug(f"  Could not find quarterly for correlation: {symbol}")
    return symbol


def calculate_correlation_and_cointegration(df, symbol_a, symbol_b, outright_data_dict, config, spread_row_meta=None):
    """
    Calculate correlation and cointegration for spread components
    
    Args:
        df: DataFrame with Date index and close prices (for the spread)
        symbol_a: First component symbol (e.g., '%PRL F!-IEU' or '=((('%PRL N!-IEU')...')
        symbol_b: Second component symbol (e.g., '%PRN F!-IEU' or '=((('%PRL N!-IEU')...')
        outright_data_dict: Dictionary of outright DataFrames {symbol: DataFrame}
        config: Configuration dictionary with spread_analysis settings
        spread_row_meta: Optional spread row metadata (for conversion factor lookup)
    
    Returns:
        Dictionary with correlation and cointegration values, or None if calculation fails
    """
    if not symbol_a or not symbol_b:
        return None
    
    try:
        spread_analysis_config = config.get('spread_analysis', {})
        correlation_config = spread_analysis_config.get('correlation', {})
        cointegration_config = spread_analysis_config.get('cointegration', {})
        
        lookback_weeks = correlation_config.get('lookback_weeks', 52)
        significance_level = cointegration_config.get('significance_level', 0.05)
        
        # Look up symbols in outright_data_dict (handles quarterlies with conversion factors)
        lookup_symbol_a = find_symbol_in_outright_dict(symbol_a, outright_data_dict, spread_row_meta, is_symbol_a=True)
        lookup_symbol_b = find_symbol_in_outright_dict(symbol_b, outright_data_dict, spread_row_meta, is_symbol_a=False)
        
        # Get component symbol data
        if lookup_symbol_a not in outright_data_dict or lookup_symbol_b not in outright_data_dict:
            logger.debug(f"Component symbols not found in outright_data_dict: {lookup_symbol_a} (original: {symbol_a}), {lookup_symbol_b} (original: {symbol_b})")
            return None
        
        df_a = outright_data_dict[lookup_symbol_a].copy()
        df_b = outright_data_dict[lookup_symbol_b].copy()
        
        if df_a is None or len(df_a) == 0 or df_b is None or len(df_b) == 0:
            logger.debug(f"Insufficient data for correlation/cointegration: {lookup_symbol_a}, {lookup_symbol_b}")
            return None
        
        # Ensure both have 'close' column
        if 'close' not in df_a.columns or 'close' not in df_b.columns:
            logger.debug(f"Missing 'close' column in component data")
            return None
        
        # Outright DataFrames have 'Date' as a column, not index
        # Set Date as index for alignment
        if 'Date' in df_a.columns:
            df_a = df_a.set_index('Date')
            df_a.index = pd.to_datetime(df_a.index)
        elif not isinstance(df_a.index, pd.DatetimeIndex):
            df_a.index = pd.to_datetime(df_a.index)
        
        if 'Date' in df_b.columns:
            df_b = df_b.set_index('Date')
            df_b.index = pd.to_datetime(df_b.index)
        elif not isinstance(df_b.index, pd.DatetimeIndex):
            df_b.index = pd.to_datetime(df_b.index)
        
        # Get close price series
        close_a = df_a['close']
        close_b = df_b['close']
        
        # Align series by date
        aligned = pd.DataFrame({'a': close_a, 'b': close_b}).dropna()
        
        if len(aligned) < lookback_weeks:
            logger.debug(f"Insufficient data for correlation: {len(aligned)} rows, need {lookback_weeks}")
            return None
        
        # Calculate correlation using rolling window
        # Use the most recent lookback_weeks of data
        recent_data = aligned.tail(lookback_weeks)
        correlation = recent_data['a'].corr(recent_data['b'])
        
        # Calculate cointegration using Engle-Granger test
        cointegration_pvalue = np.nan
        cointegration_statistic = np.nan
        is_cointegrated = False
        
        if STATSMODELS_AVAILABLE:
            try:
                # Need at least 52 weeks for reliable cointegration test
                if len(aligned) >= 52:
                    # Run Engle-Granger cointegration test
                    test_result = coint(aligned['a'], aligned['b'])
                    cointegration_statistic = test_result[0]
                    cointegration_pvalue = test_result[1]
                    critical_values = test_result[2]
                    
                    # Determine if cointegrated (p-value < significance level)
                    is_cointegrated = cointegration_pvalue < significance_level
                else:
                    logger.debug(f"Insufficient data for cointegration test: {len(aligned)} rows, need at least 52")
            except Exception as e:
                logger.debug(f"Error calculating cointegration: {e}")
        else:
            if not STATSMODELS_AVAILABLE:
                logger.warning("statsmodels not available - skipping cointegration calculation. Install with: conda install statsmodels")
        
        return {
            'correlation': correlation if not pd.isna(correlation) else np.nan,
            'cointegration_pvalue': cointegration_pvalue,
            'cointegration_statistic': cointegration_statistic,
            'is_cointegrated': is_cointegrated
        }
        
    except Exception as e:
        logger.warning(f"Error calculating correlation/cointegration for {symbol_a}/{symbol_b}: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return None


def calculate_technical_indicators(df, symbol_info, config):
    """
    Calculate all technical indicators for a symbol's OHLC data
    
    This function is implemented incrementally across multiple phases.
    Phase 2: EMAs and ADX implemented
    Phase 3: SuperTrend implemented
    Phase 4: MACD, RSI, Stochastic, ROC implemented
    Phase 5: CCI, Williams %R, Aroon implemented
    Phase 6: Bollinger Bands, ATR, Historical Volatility implemented
    Phase 7: Z-score, Coefficient of Variation, Percentiles implemented
    Phase 8: 4-Factor Markov Model implemented
    
    Args:
        df: DataFrame with Date index and columns: open, high, low, close
        symbol_info: Dictionary with symbol metadata (from symbol matrix)
        config: Indicator configuration dictionary
    
    Returns:
        DataFrame with all technical indicators added
    """
    if df is None or len(df) == 0:
        return df
    
    if not PANDAS_TA_AVAILABLE:
        logger.warning("pandas_ta not available - skipping indicator calculations")
        return df
    
    # Make a copy to avoid modifying original
    result_df = df.copy()
    
    # Ensure Date is the index
    if 'Date' in result_df.columns:
        result_df = result_df.set_index('Date')
    
    # Sort by date
    result_df = result_df.sort_index()
    
    # Ensure we have the required columns
    required_cols = ['open', 'high', 'low', 'close']
    if not all(col in result_df.columns for col in required_cols):
        logger.warning(f"Missing required OHLC columns. Available: {result_df.columns.tolist()}")
        return result_df
    
    # PHASE 2: Moving Averages and Basic Trend Indicators
    
    # Step 2.1: Exponential Moving Averages (EMA)
    ema_periods = config['moving_averages']['ema_periods']
    for period in ema_periods:
        if len(result_df) >= period:
            ema = ta.ema(result_df['close'], length=period)
            result_df[f'ema_{period}'] = ema
        else:
            # Not enough data - fill with NaN
            result_df[f'ema_{period}'] = np.nan
            logger.debug(f"Insufficient data for EMA_{period}: {len(result_df)} rows available")
    
    # Step 2.2: ADX and Directional Indicators
    adx_config = config['trend_indicators']['adx']
    adx_period = adx_config['period']
    
    if len(result_df) >= adx_period:
        # Calculate ADX, DI+, DI-
        adx_result = ta.adx(
            high=result_df['high'],
            low=result_df['low'],
            close=result_df['close'],
            length=adx_period
        )
        
        if adx_result is not None and len(adx_result) > 0:
            # pandas_ta returns DataFrame with columns: ADX_14, DMP_14, DMN_14
            # We need to map these to our column names
            adx_col = f'ADX_{adx_period}'
            di_plus_col = f'DMP_{adx_period}'  # DI+ (Directional Movement Plus)
            di_minus_col = f'DMN_{adx_period}'  # DI- (Directional Movement Minus)
            
            if adx_col in adx_result.columns:
                result_df['adx'] = adx_result[adx_col]
            if di_plus_col in adx_result.columns:
                result_df['di_plus'] = adx_result[di_plus_col]
            if di_minus_col in adx_result.columns:
                result_df['di_minus'] = adx_result[di_minus_col]
        else:
            result_df['adx'] = np.nan
            result_df['di_plus'] = np.nan
            result_df['di_minus'] = np.nan
    else:
        result_df['adx'] = np.nan
        result_df['di_plus'] = np.nan
        result_df['di_minus'] = np.nan
        logger.debug(f"Insufficient data for ADX: {len(result_df)} rows available, need {adx_period}")
    
    logger.debug(f"Phase 2 indicators calculated: EMAs and ADX")
    
    # PHASE 3: Additional Trend Indicators
    
    # Step 3.1: SuperTrend
    supertrend_config = config['trend_indicators']['supertrend']
    atr_period = supertrend_config['atr_period']
    multiplier = supertrend_config['multiplier']
    
    if len(result_df) >= atr_period:
        # Calculate SuperTrend using pandas_ta
        supertrend_result = ta.supertrend(
            high=result_df['high'],
            low=result_df['low'],
            close=result_df['close'],
            length=atr_period,
            multiplier=multiplier
        )
        
        if supertrend_result is not None and len(supertrend_result) > 0:
            # pandas_ta returns DataFrame with columns: SUPERT_10_3.0, SUPERTd_10_3.0
            # SUPERT_10_3.0 = SuperTrend value
            # SUPERTd_10_3.0 = SuperTrend direction (1 for up, -1 for down)
            supert_col = f'SUPERT_{atr_period}_{multiplier}'
            supertd_col = f'SUPERTd_{atr_period}_{multiplier}'
            
            if supert_col in supertrend_result.columns:
                result_df['supertrend_value'] = supertrend_result[supert_col]
            if supertd_col in supertrend_result.columns:
                result_df['supertrend_direction'] = supertrend_result[supertd_col]
        else:
            result_df['supertrend_value'] = np.nan
            result_df['supertrend_direction'] = np.nan
    else:
        result_df['supertrend_value'] = np.nan
        result_df['supertrend_direction'] = np.nan
        logger.debug(f"Insufficient data for SuperTrend: {len(result_df)} rows available, need {atr_period}")
    
    logger.debug(f"Phase 3 indicators calculated: SuperTrend")
    
    # PHASE 4: Momentum Indicators
    
    # Step 4.1: MACD
    macd_config = config['momentum_indicators']['macd']
    macd_fast = macd_config['fast']
    macd_slow = macd_config['slow']
    macd_signal = macd_config['signal']
    
    if len(result_df) >= macd_slow:
        # Calculate MACD using pandas_ta
        macd_result = ta.macd(
            close=result_df['close'],
            fast=macd_fast,
            slow=macd_slow,
            signal=macd_signal
        )
        
        if macd_result is not None and len(macd_result) > 0:
            # pandas_ta returns DataFrame with columns: MACD_12_26_9, MACDs_12_26_9, MACDh_12_26_9
            # MACD_12_26_9 = MACD line, MACDs_12_26_9 = Signal line, MACDh_12_26_9 = Histogram
            macd_col = f'MACD_{macd_fast}_{macd_slow}_{macd_signal}'
            macds_col = f'MACDs_{macd_fast}_{macd_slow}_{macd_signal}'
            macdh_col = f'MACDh_{macd_fast}_{macd_slow}_{macd_signal}'
            
            if macd_col in macd_result.columns:
                result_df['macd_line'] = macd_result[macd_col]
            if macds_col in macd_result.columns:
                result_df['macd_signal'] = macd_result[macds_col]
            if macdh_col in macd_result.columns:
                result_df['macd_histogram'] = macd_result[macdh_col]
        else:
            result_df['macd_line'] = np.nan
            result_df['macd_signal'] = np.nan
            result_df['macd_histogram'] = np.nan
    else:
        result_df['macd_line'] = np.nan
        result_df['macd_signal'] = np.nan
        result_df['macd_histogram'] = np.nan
        logger.debug(f"Insufficient data for MACD: {len(result_df)} rows available, need {macd_slow}")
    
    # Step 4.2: RSI
    rsi_config = config['momentum_indicators']['rsi']
    rsi_period = rsi_config['period']
    rsi_overbought_threshold = rsi_config['overbought']
    rsi_oversold_threshold = rsi_config['oversold']
    
    if len(result_df) >= rsi_period:
        # Calculate RSI using pandas_ta
        rsi = ta.rsi(close=result_df['close'], length=rsi_period)
        
        if rsi is not None and len(rsi) > 0:
            result_df['rsi'] = rsi
            
            # Add overbought/oversold flags
            result_df['rsi_overbought'] = (rsi > rsi_overbought_threshold).astype(int)
            result_df['rsi_oversold'] = (rsi < rsi_oversold_threshold).astype(int)
        else:
            result_df['rsi'] = np.nan
            result_df['rsi_overbought'] = np.nan
            result_df['rsi_oversold'] = np.nan
    else:
        result_df['rsi'] = np.nan
        result_df['rsi_overbought'] = np.nan
        result_df['rsi_oversold'] = np.nan
        logger.debug(f"Insufficient data for RSI: {len(result_df)} rows available, need {rsi_period}")
    
    # Step 4.3: Slow Stochastic (14,3,3 smoothed - matching ICE Connect SIMPLE, SIMPLE)
    stoch_config = config['momentum_indicators']['stochastic']
    stoch_k_period = stoch_config['k_period']  # 14
    stoch_d_period = stoch_config['d_period']  # 3
    stoch_smooth_k = stoch_config['smooth_k']  # 3
    stoch_overbought_threshold = stoch_config['overbought']  # 80
    stoch_oversold_threshold = stoch_config['oversold']  # 20
    
    # Calculate slow stochastics manually to match ICE Connect (SIMPLE, SIMPLE)
    # ICE Connect uses: SSTOC(..., 14, 3, 3, SIMPLE, SIMPLE)
    # This means:
    # 1. Calculate raw %K (14-period): %K_raw = 100 * (Close - Lowest Low) / (Highest High - Lowest Low)
    # 2. Smooth %K by 3 periods (SIMPLE moving average): %K_slow = SMA(%K_raw, 3)
    # 3. Calculate %D as 3-period SMA of slow %K: %D = SMA(%K_slow, 3)
    min_periods_needed = stoch_k_period + stoch_smooth_k + stoch_d_period - 2  # 14 + 3 + 3 - 2 = 18
    
    if len(result_df) >= min_periods_needed:
        try:
            # Step 1: Calculate raw %K (14-period)
            # %K_raw = 100 * (Close - Lowest Low in 14 periods) / (Highest High in 14 periods - Lowest Low in 14 periods)
            high_rolling = result_df['high'].rolling(window=stoch_k_period, min_periods=stoch_k_period)
            low_rolling = result_df['low'].rolling(window=stoch_k_period, min_periods=stoch_k_period)
            
            highest_high = high_rolling.max()
            lowest_low = low_rolling.min()
            
            # Calculate raw %K
            denominator = highest_high - lowest_low
            # Avoid division by zero
            denominator = denominator.replace(0, np.nan)
            stoch_k_raw = 100 * (result_df['close'] - lowest_low) / denominator
            
            # Step 2: Smooth %K by 3 periods (SIMPLE moving average)
            stoch_k_slow = stoch_k_raw.rolling(window=stoch_smooth_k, min_periods=stoch_smooth_k).mean()
            
            # Step 3: Calculate %D as 3-period SMA of slow %K
            stoch_d = stoch_k_slow.rolling(window=stoch_d_period, min_periods=stoch_d_period).mean()
            
            # Assign to result DataFrame
            result_df['stoch_k'] = stoch_k_slow
            result_df['stoch_d'] = stoch_d
            
            # Add overbought/oversold flags (1 if condition met, 0 otherwise)
            result_df['stoch_overbought'] = (stoch_k_slow > stoch_overbought_threshold).astype(int)
            result_df['stoch_oversold'] = (stoch_k_slow < stoch_oversold_threshold).astype(int)
            
        except Exception as e:
            logger.warning(f"Error calculating slow stochastics: {e}")
            result_df['stoch_k'] = np.nan
            result_df['stoch_d'] = np.nan
            result_df['stoch_overbought'] = np.nan
            result_df['stoch_oversold'] = np.nan
    else:
        result_df['stoch_k'] = np.nan
        result_df['stoch_d'] = np.nan
        result_df['stoch_overbought'] = np.nan
        result_df['stoch_oversold'] = np.nan
        logger.debug(f"Insufficient data for Slow Stochastic: {len(result_df)} rows available, need {min_periods_needed}")
    
    # Step 4.4: Rate of Change (ROC)
    roc_config = config['momentum_indicators']['roc']
    roc_periods = roc_config['periods']
    
    for roc_period in roc_periods:
        if len(result_df) >= roc_period:
            # Calculate ROC using pandas_ta
            roc = ta.roc(close=result_df['close'], length=roc_period)
            
            if roc is not None and len(roc) > 0:
                if roc_period == 1:
                    result_df['roc'] = roc
                else:
                    result_df[f'roc_{roc_period}w'] = roc
            else:
                if roc_period == 1:
                    result_df['roc'] = np.nan
                else:
                    result_df[f'roc_{roc_period}w'] = np.nan
        else:
            if roc_period == 1:
                result_df['roc'] = np.nan
            else:
                result_df[f'roc_{roc_period}w'] = np.nan
            logger.debug(f"Insufficient data for ROC_{roc_period}w: {len(result_df)} rows available, need {roc_period}")
    
    logger.debug(f"Phase 4 indicators calculated: MACD, RSI, Stochastic, ROC")
    
    # PHASE 5: Oscillators and Aroon
    
    # Step 5.1: CCI (Commodity Channel Index)
    cci_config = config['oscillators']['cci']
    cci_period = cci_config['period']
    cci_overbought_threshold = cci_config['overbought']
    cci_oversold_threshold = cci_config['oversold']
    
    if len(result_df) >= cci_period:
        # Calculate CCI using pandas_ta
        cci = ta.cci(
            high=result_df['high'],
            low=result_df['low'],
            close=result_df['close'],
            length=cci_period
        )
        
        if cci is not None and len(cci) > 0:
            result_df['cci'] = cci
            
            # Add overbought/oversold flags
            result_df['cci_overbought'] = (cci > cci_overbought_threshold).astype(int)
            result_df['cci_oversold'] = (cci < cci_oversold_threshold).astype(int)
        else:
            result_df['cci'] = np.nan
            result_df['cci_overbought'] = np.nan
            result_df['cci_oversold'] = np.nan
    else:
        result_df['cci'] = np.nan
        result_df['cci_overbought'] = np.nan
        result_df['cci_oversold'] = np.nan
        logger.debug(f"Insufficient data for CCI: {len(result_df)} rows available, need {cci_period}")
    
    # Step 5.2: Williams %R
    williams_r_config = config['oscillators']['williams_r']
    williams_r_period = williams_r_config['period']
    williams_r_overbought_threshold = williams_r_config['overbought']
    williams_r_oversold_threshold = williams_r_config['oversold']
    
    if len(result_df) >= williams_r_period:
        # Calculate Williams %R using pandas_ta
        williams_r = ta.willr(
            high=result_df['high'],
            low=result_df['low'],
            close=result_df['close'],
            length=williams_r_period
        )
        
        if williams_r is not None and len(williams_r) > 0:
            result_df['williams_r'] = williams_r
            
            # Add overbought/oversold flags (note: Williams %R is inverted, so thresholds are negative)
            result_df['williams_r_overbought'] = (williams_r > williams_r_overbought_threshold).astype(int)
            result_df['williams_r_oversold'] = (williams_r < williams_r_oversold_threshold).astype(int)
        else:
            result_df['williams_r'] = np.nan
            result_df['williams_r_overbought'] = np.nan
            result_df['williams_r_oversold'] = np.nan
    else:
        result_df['williams_r'] = np.nan
        result_df['williams_r_overbought'] = np.nan
        result_df['williams_r_oversold'] = np.nan
        logger.debug(f"Insufficient data for Williams %R: {len(result_df)} rows available, need {williams_r_period}")
    
    # Step 5.3: Aroon
    aroon_config = config['aroon']
    aroon_period = aroon_config['period']
    aroon_strong_uptrend_threshold = aroon_config['strong_uptrend_threshold']
    aroon_strong_downtrend_threshold = aroon_config['strong_downtrend_threshold']
    
    if len(result_df) >= aroon_period:
        # Calculate Aroon using pandas_ta
        aroon_result = ta.aroon(
            high=result_df['high'],
            low=result_df['low'],
            length=aroon_period
        )
        
        if aroon_result is not None and len(aroon_result) > 0:
            # pandas_ta returns DataFrame with columns: AROONU_14, AROOND_14
            aroonu_col = f'AROONU_{aroon_period}'
            aroond_col = f'AROOND_{aroon_period}'
            
            if aroonu_col in aroon_result.columns and aroond_col in aroon_result.columns:
                result_df['aroon_up'] = aroon_result[aroonu_col]
                result_df['aroon_down'] = aroon_result[aroond_col]
                
                # Calculate Aroon Oscillator (Aroon Up - Aroon Down)
                result_df['aroon_oscillator'] = aroon_result[aroonu_col] - aroon_result[aroond_col]
                
                # Add trend flags
                # Strong uptrend: Aroon Up > threshold AND Aroon Down < (100 - threshold)
                # Strong downtrend: Aroon Down > threshold AND Aroon Up < (100 - threshold)
                result_df['aroon_strong_uptrend'] = (
                    (aroon_result[aroonu_col] > aroon_strong_uptrend_threshold) & 
                    (aroon_result[aroond_col] < (100 - aroon_strong_uptrend_threshold))
                ).astype(int)
                
                result_df['aroon_strong_downtrend'] = (
                    (aroon_result[aroond_col] > aroon_strong_downtrend_threshold) & 
                    (aroon_result[aroonu_col] < (100 - aroon_strong_downtrend_threshold))
                ).astype(int)
            else:
                result_df['aroon_up'] = np.nan
                result_df['aroon_down'] = np.nan
                result_df['aroon_oscillator'] = np.nan
                result_df['aroon_strong_uptrend'] = np.nan
                result_df['aroon_strong_downtrend'] = np.nan
        else:
            result_df['aroon_up'] = np.nan
            result_df['aroon_down'] = np.nan
            result_df['aroon_oscillator'] = np.nan
            result_df['aroon_strong_uptrend'] = np.nan
            result_df['aroon_strong_downtrend'] = np.nan
    else:
        result_df['aroon_up'] = np.nan
        result_df['aroon_down'] = np.nan
        result_df['aroon_oscillator'] = np.nan
        result_df['aroon_strong_uptrend'] = np.nan
        result_df['aroon_strong_downtrend'] = np.nan
        logger.debug(f"Insufficient data for Aroon: {len(result_df)} rows available, need {aroon_period}")
    
    logger.debug(f"Phase 5 indicators calculated: CCI, Williams %R, Aroon")
    
    # PHASE 6: Volatility Indicators
    
    # Step 6.1: Bollinger Bands
    bb_config = config['volatility']['bollinger_bands']
    bb_period = bb_config['period']
    bb_std_dev = bb_config['std_dev']
    
    if len(result_df) >= bb_period:
        # Calculate Bollinger Bands using pandas_ta
        bb_result = ta.bbands(
            close=result_df['close'],
            length=bb_period,
            std=bb_std_dev
        )
        
        if bb_result is not None and len(bb_result) > 0:
            # pandas_ta returns DataFrame with columns: BBU_20_2.0, BBM_20_2.0, BBL_20_2.0
            bbu_col = f'BBU_{bb_period}_{bb_std_dev}'
            bbm_col = f'BBM_{bb_period}_{bb_std_dev}'
            bbl_col = f'BBL_{bb_period}_{bb_std_dev}'
            
            if bbu_col in bb_result.columns and bbm_col in bb_result.columns and bbl_col in bb_result.columns:
                result_df['bb_upper'] = bb_result[bbu_col]
                result_df['bb_middle'] = bb_result[bbm_col]
                result_df['bb_lower'] = bb_result[bbl_col]
                
                # Calculate Bollinger Band Width % = ((Upper - Lower) / Middle) * 100
                result_df['bb_width_pct'] = ((bb_result[bbu_col] - bb_result[bbl_col]) / bb_result[bbm_col]) * 100
            else:
                result_df['bb_upper'] = np.nan
                result_df['bb_middle'] = np.nan
                result_df['bb_lower'] = np.nan
                result_df['bb_width_pct'] = np.nan
        else:
            result_df['bb_upper'] = np.nan
            result_df['bb_middle'] = np.nan
            result_df['bb_lower'] = np.nan
            result_df['bb_width_pct'] = np.nan
    else:
        result_df['bb_upper'] = np.nan
        result_df['bb_middle'] = np.nan
        result_df['bb_lower'] = np.nan
        result_df['bb_width_pct'] = np.nan
        logger.debug(f"Insufficient data for Bollinger Bands: {len(result_df)} rows available, need {bb_period}")
    
    # Step 6.2: ATR (Average True Range)
    atr_config = config['volatility']['atr']
    atr_period = atr_config['period']
    
    if len(result_df) >= atr_period:
        # Calculate ATR using pandas_ta
        atr = ta.atr(
            high=result_df['high'],
            low=result_df['low'],
            close=result_df['close'],
            length=atr_period
        )
        
        if atr is not None and len(atr) > 0:
            result_df['atr'] = atr
            
            # Calculate ATR % of price = (ATR / Close) * 100
            result_df['atr_pct_of_price'] = (atr / result_df['close']) * 100
        else:
            result_df['atr'] = np.nan
            result_df['atr_pct_of_price'] = np.nan
    else:
        result_df['atr'] = np.nan
        result_df['atr_pct_of_price'] = np.nan
        logger.debug(f"Insufficient data for ATR: {len(result_df)} rows available, need {atr_period}")
    
    # Step 6.3: Historical Volatility
    hv_config = config['volatility']['historical_volatility']
    hv_period = hv_config['period']
    
    if len(result_df) >= hv_period + 1:  # Need at least period+1 for returns calculation
        # Calculate historical volatility as rolling standard deviation of returns, annualized
        # First calculate weekly returns
        returns = result_df['close'].pct_change()
        
        # Calculate rolling standard deviation of returns
        rolling_std = returns.rolling(window=hv_period).std()
        
        # Annualize: multiply by sqrt(52) for weekly data (52 weeks per year)
        # Historical volatility = rolling_std * sqrt(52) * 100 (to convert to percentage)
        result_df['historical_volatility_20w'] = rolling_std * np.sqrt(52) * 100
    else:
        result_df['historical_volatility_20w'] = np.nan
        logger.debug(f"Insufficient data for Historical Volatility: {len(result_df)} rows available, need {hv_period + 1}")
    
    logger.debug(f"Phase 6 indicators calculated: Bollinger Bands, ATR, Historical Volatility")
    
    # PHASE 7: Statistical Measures
    
    # Step 7.1: Z-score
    zscore_config = config['statistical']['zscore']
    zscore_period = zscore_config['period']
    
    if len(result_df) >= zscore_period:
        # Calculate rolling mean and standard deviation
        rolling_mean = result_df['close'].rolling(window=zscore_period).mean()
        rolling_std = result_df['close'].rolling(window=zscore_period).std()
        
        # Calculate Z-score = (value - mean) / std_dev
        result_df['zscore'] = (result_df['close'] - rolling_mean) / rolling_std
    else:
        result_df['zscore'] = np.nan
        logger.debug(f"Insufficient data for Z-score: {len(result_df)} rows available, need {zscore_period}")
    
    # Step 7.2: Coefficient of Variation
    cv_config = config['statistical']['coefficient_of_variation']
    cv_period = cv_config['period']
    
    if len(result_df) >= cv_period:
        # Calculate rolling mean and standard deviation
        rolling_mean = result_df['close'].rolling(window=cv_period).mean()
        rolling_std = result_df['close'].rolling(window=cv_period).std()
        
        # Coefficient of Variation = (std_dev / mean) * 100
        # Handle division by zero
        result_df['coefficient_of_variation'] = np.where(
            rolling_mean != 0,
            (rolling_std / rolling_mean) * 100,
            np.nan
        )
    else:
        result_df['coefficient_of_variation'] = np.nan
        logger.debug(f"Insufficient data for Coefficient of Variation: {len(result_df)} rows available, need {cv_period}")
    
    # Step 7.3: Percentiles (5-year rolling lookback)
    percentiles_config = config['statistical']['percentiles']
    lookback_weeks = percentiles_config['lookback_weeks']
    percentile_indicators = percentiles_config['indicators']
    
    # Calculate percentiles for each indicator using rolling window
    for indicator in percentile_indicators:
        if indicator == 'close':
            # Use close price
            data_series = result_df['close']
            col_name = 'percentile_close'
        elif indicator == 'rsi':
            # Use RSI (must be calculated first)
            if 'rsi' not in result_df.columns:
                logger.debug(f"RSI not available for percentile calculation")
                result_df['rsi_percentile'] = np.nan
                continue
            data_series = result_df['rsi']
            col_name = 'rsi_percentile'
        elif indicator == 'macd_line':
            # Use MACD line (must be calculated first)
            if 'macd_line' not in result_df.columns:
                logger.debug(f"MACD line not available for percentile calculation")
                result_df['macd_line_percentile'] = np.nan
                continue
            data_series = result_df['macd_line']
            col_name = 'macd_line_percentile'
        elif indicator == 'macd_signal':
            # Use MACD signal (must be calculated first)
            if 'macd_signal' not in result_df.columns:
                logger.debug(f"MACD signal not available for percentile calculation")
                result_df['macd_signal_percentile'] = np.nan
                continue
            data_series = result_df['macd_signal']
            col_name = 'macd_signal_percentile'
        elif indicator == 'macd_histogram':
            # Use MACD histogram (must be calculated first)
            if 'macd_histogram' not in result_df.columns:
                logger.debug(f"MACD histogram not available for percentile calculation")
                result_df['macd_histogram_percentile'] = np.nan
                continue
            data_series = result_df['macd_histogram']
            col_name = 'macd_histogram_percentile'
        else:
            logger.warning(f"Unknown indicator for percentile: {indicator}")
            continue
        
        # Calculate percentile for each row using rolling window
        # IMPORTANT: Percentiles are based on 5-year (260-week) rolling lookback
        # For the most recent week in output, percentiles use all 5 years of historical data
        if len(result_df) >= lookback_weeks:
            # For each row, calculate percentile of current value within rolling window
            percentile_values = []
            for i in range(len(result_df)):
                if i < lookback_weeks - 1:
                    # Not enough data for full 5-year window - use available data
                    window_data = data_series.iloc[:i+1]
                    if len(window_data) > 0 and not window_data.isna().all():
                        current_value = data_series.iloc[i]
                        if pd.notna(current_value):
                            percentile = (window_data <= current_value).sum() / len(window_data) * 100
                            percentile_values.append(percentile)
                        else:
                            percentile_values.append(np.nan)
                    else:
                        percentile_values.append(np.nan)
                else:
                    # Full 5-year window available - use 260 weeks of historical data
                    # For most recent week, this uses all 5 years leading up to that date
                    window_data = data_series.iloc[i - lookback_weeks + 1:i + 1]
                    current_value = data_series.iloc[i]
                    if pd.notna(current_value) and not window_data.isna().all():
                        # Calculate percentile: what percentage of values in 5-year window are <= current value
                        percentile = (window_data <= current_value).sum() / len(window_data) * 100
                        percentile_values.append(percentile)
                    else:
                        percentile_values.append(np.nan)
            
            result_df[col_name] = percentile_values
        else:
            # Not enough data for full lookback - calculate with available data
            if len(result_df) > 0:
                percentile_values = []
                for i in range(len(result_df)):
                    window_data = data_series.iloc[:i+1]
                    current_value = data_series.iloc[i]
                    if pd.notna(current_value) and len(window_data) > 0 and not window_data.isna().all():
                        percentile = (window_data <= current_value).sum() / len(window_data) * 100
                        percentile_values.append(percentile)
                    else:
                        percentile_values.append(np.nan)
                result_df[col_name] = percentile_values
            else:
                result_df[col_name] = np.nan
            logger.debug(f"Insufficient data for {col_name}: {len(result_df)} rows available, using available data")
    
    logger.debug(f"Phase 7 indicators calculated: Z-score, Coefficient of Variation, Percentiles")
    
    # PHASE 8: 4-Factor Markov Model
    
    # Step 8.1: State Classification
    markov_config = config['markov_model']
    state_config = markov_config['state_classification']
    transition_config = markov_config['transition_matrix']
    
    adx_strong_threshold = state_config['adx_strong_threshold']
    rsi_overbought = state_config['rsi_overbought']
    rsi_oversold = state_config['rsi_oversold']
    ema_short = state_config['ema_short']
    ema_medium = state_config['ema_medium']
    ema_long = state_config['ema_long']
    lookback_weeks = transition_config['lookback_weeks']
    
    # Check if required indicators are available
    required_cols = ['close', 'adx', 'macd_line', 'rsi']
    ema_cols = [f'ema_{ema_short}', f'ema_{ema_medium}', f'ema_{ema_long}']
    
    if all(col in result_df.columns for col in required_cols) and all(col in result_df.columns for col in ema_cols):
        # Classify each week into one of 4 states
        # State 1: Strong Bullish - High momentum, price above MAs, strong trend
        # State 2: Weak Bullish - Low momentum, price above MAs but weakening
        # State 3: Weak Bearish - Low momentum, price below MAs but weakening
        # State 4: Strong Bearish - High momentum, price below MAs, strong downtrend
        
        markov_states = []
        
        for i in range(len(result_df)):
            close_val = result_df['close'].iloc[i]
            adx_val = result_df['adx'].iloc[i]
            macd_val = result_df['macd_line'].iloc[i]
            rsi_val = result_df['rsi'].iloc[i]
            ema_short_val = result_df[f'ema_{ema_short}'].iloc[i]
            ema_medium_val = result_df[f'ema_{ema_medium}'].iloc[i]
            ema_long_val = result_df[f'ema_{ema_long}'].iloc[i]
            
            # Check if all values are valid
            if pd.isna(close_val) or pd.isna(adx_val) or pd.isna(macd_val) or pd.isna(rsi_val) or \
               pd.isna(ema_short_val) or pd.isna(ema_medium_val) or pd.isna(ema_long_val):
                markov_states.append(np.nan)
                continue
            
            # Determine price position relative to EMAs
            price_above_short = close_val > ema_short_val
            price_above_medium = close_val > ema_medium_val
            price_above_long = close_val > ema_long_val
            
            # Count how many EMAs price is above
            emas_above = sum([price_above_short, price_above_medium, price_above_long])
            
            # Determine trend strength
            strong_trend = adx_val > adx_strong_threshold
            
            # Determine momentum direction
            bullish_momentum = macd_val > 0
            
            # Determine RSI condition
            rsi_overbought_cond = rsi_val > rsi_overbought
            rsi_oversold_cond = rsi_val < rsi_oversold
            rsi_neutral = not rsi_overbought_cond and not rsi_oversold_cond
            
            # Classify state
            if emas_above >= 2 and strong_trend and bullish_momentum:
                # Strong Bullish: Price above most MAs, strong trend, bullish momentum
                state = 1
            elif emas_above >= 2 and (not strong_trend or not bullish_momentum):
                # Weak Bullish: Price above MAs but weak trend or momentum
                state = 2
            elif emas_above < 2 and (not strong_trend or bullish_momentum):
                # Weak Bearish: Price below MAs but weak trend or still some bullish momentum
                state = 3
            elif emas_above < 2 and strong_trend and not bullish_momentum:
                # Strong Bearish: Price below MAs, strong trend, bearish momentum
                state = 4
            else:
                # Default classification based on price position
                if emas_above >= 2:
                    state = 2  # Weak Bullish
                else:
                    state = 3  # Weak Bearish
            
            markov_states.append(state)
        
        result_df['markov_state'] = markov_states
        
        # Step 8.2: Transition Probabilities
        # Calculate 4x4 transition matrix using rolling window
        transition_probabilities = {
            'markov_prob_state_1': [],
            'markov_prob_state_2': [],
            'markov_prob_state_3': [],
            'markov_prob_state_4': []
        }
        
        for i in range(len(result_df)):
            if i < lookback_weeks:
                # Not enough data for full window - use available data
                window_states = result_df['markov_state'].iloc[:i+1]
            else:
                # Full window available
                window_states = result_df['markov_state'].iloc[i - lookback_weeks + 1:i + 1]
            
            # Get current state
            current_state = result_df['markov_state'].iloc[i]
            
            if pd.isna(current_state) or len(window_states) < 2:
                # Not enough data or invalid state
                transition_probabilities['markov_prob_state_1'].append(np.nan)
                transition_probabilities['markov_prob_state_2'].append(np.nan)
                transition_probabilities['markov_prob_state_3'].append(np.nan)
                transition_probabilities['markov_prob_state_4'].append(np.nan)
                continue
            
            # Calculate transition probabilities
            # Count transitions from current state to each next state
            transitions = {1: 0, 2: 0, 3: 0, 4: 0}
            total_transitions = 0
            
            # Find all occurrences of current state in window and count next states
            window_states_list = window_states.tolist()
            for j in range(len(window_states_list) - 1):
                if pd.notna(window_states_list[j]) and window_states_list[j] == current_state:
                    next_state = window_states_list[j + 1]
                    if pd.notna(next_state) and next_state in [1, 2, 3, 4]:
                        transitions[int(next_state)] += 1
                        total_transitions += 1
            
            # Calculate probabilities
            if total_transitions > 0:
                transition_probabilities['markov_prob_state_1'].append(transitions[1] / total_transitions)
                transition_probabilities['markov_prob_state_2'].append(transitions[2] / total_transitions)
                transition_probabilities['markov_prob_state_3'].append(transitions[3] / total_transitions)
                transition_probabilities['markov_prob_state_4'].append(transitions[4] / total_transitions)
            else:
                # No transitions found - use equal probabilities or NaN
                transition_probabilities['markov_prob_state_1'].append(np.nan)
                transition_probabilities['markov_prob_state_2'].append(np.nan)
                transition_probabilities['markov_prob_state_3'].append(np.nan)
                transition_probabilities['markov_prob_state_4'].append(np.nan)
        
        # Add transition probability columns
        result_df['markov_prob_state_1'] = transition_probabilities['markov_prob_state_1']
        result_df['markov_prob_state_2'] = transition_probabilities['markov_prob_state_2']
        result_df['markov_prob_state_3'] = transition_probabilities['markov_prob_state_3']
        result_df['markov_prob_state_4'] = transition_probabilities['markov_prob_state_4']
        
    else:
        # Missing required indicators
        result_df['markov_state'] = np.nan
        result_df['markov_prob_state_1'] = np.nan
        result_df['markov_prob_state_2'] = np.nan
        result_df['markov_prob_state_3'] = np.nan
        result_df['markov_prob_state_4'] = np.nan
        logger.debug(f"Missing required indicators for Markov model: need {required_cols + ema_cols}")
    
    logger.debug(f"Phase 8 indicators calculated: 4-Factor Markov Model")
    
    return result_df


def calculate_spread_ohlc(symbol_1, symbol_2, component_data_dict):
    """
    Calculate spread OHLC from two component symbols' OHLC data
    
    Spread calculation logic:
    - Open = symbol_1_open - symbol_2_open
    - High = symbol_1_high - symbol_2_low (widest spread)
    - Low = symbol_1_low - symbol_2_high (narrowest spread)
    - Close = symbol_1_close - symbol_2_close
    
    Args:
        symbol_1: First component symbol (e.g., '%PRL F!-IEU')
        symbol_2: Second component symbol (e.g., '%PRN F!-IEU')
        component_data_dict: Dictionary mapping symbol -> DataFrame with OHLC data
    
    Returns:
        DataFrame with spread OHLC data, or None if insufficient data
    """
    # Get data for both component symbols
    if symbol_1 not in component_data_dict or symbol_2 not in component_data_dict:
        logger.debug(f"Missing component data for spread: {symbol_1} - {symbol_2}")
        return None
    
    df_1 = component_data_dict[symbol_1]
    df_2 = component_data_dict[symbol_2]
    
    if df_1 is None or len(df_1) == 0 or df_2 is None or len(df_2) == 0:
        logger.debug(f"Insufficient data for spread: {symbol_1} - {symbol_2}")
        return None
    
    # Find common dates (intersection)
    common_dates = df_1.index.intersection(df_2.index)
    
    if len(common_dates) == 0:
        logger.debug(f"No common dates for spread: {symbol_1} - {symbol_2}")
        return None
    
    # Calculate spread OHLC for each date
    spread_rows = []
    for date in common_dates:
        row_1 = df_1.loc[date]
        row_2 = df_2.loc[date]
        
        # Calculate spread values (only if both components have the required data)
        open_1 = row_1.get('open')
        high_1 = row_1.get('high')
        low_1 = row_1.get('low')
        close_1 = row_1.get('close')
        
        open_2 = row_2.get('open')
        high_2 = row_2.get('high')
        low_2 = row_2.get('low')
        close_2 = row_2.get('close')
        
        # Calculate spread OHLC
        spread_open = None
        spread_high = None
        spread_low = None
        spread_close = None
        
        if pd.notna(open_1) and pd.notna(open_2):
            spread_open = open_1 - open_2
        
        if pd.notna(high_1) and pd.notna(low_2):
            spread_high = high_1 - low_2  # Widest spread
        
        if pd.notna(low_1) and pd.notna(high_2):
            spread_low = low_1 - high_2  # Narrowest spread
        
        if pd.notna(close_1) and pd.notna(close_2):
            spread_close = close_1 - close_2
        
        # Only add row if we have at least Close value
        if spread_close is not None:
            spread_rows.append({
                'Date': date,
                'open': spread_open,
                'high': spread_high,
                'low': spread_low,
                'close': spread_close
            })
    
    if len(spread_rows) == 0:
        logger.debug(f"No valid spread data calculated for: {symbol_1} - {symbol_2}")
        return None
    
    df = pd.DataFrame(spread_rows)
    df = df.set_index('Date')
    df = df.sort_index()
    
    logger.debug(f"Calculated spread OHLC: {symbol_1} - {symbol_2} ({len(df)} data points)")
    return df


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
    weeks_back=None,
    output_dir='full_unfiltered_historicals',
    snapshot_date=None,
    max_workers_outrights=10,
    max_workers_spreads=20,
    config_file='study_settings/indicator_config.json',
    external_logger=None
):
    """
    Pull OHLC data for all symbols and export to CSV files with technical indicators
    
    Uses parallel processing to speed up data fetching:
    - Fetches OHLC for outrights in parallel (I/O bound - API calls)
    - Calculates spread OHLC in parallel (CPU bound - dataframe operations)
    - Always fetches 5 years of history for indicator calculations
    
    Args:
        symbols_file: Path to CSV file with symbols (default: lists_and_matrix/symbol_matrix.csv)
        weeks_back: Number of weeks of history to fetch (default: None = use config, always 5 years)
        output_dir: Directory to save CSV files (default: 'full_unfiltered_historicals')
        snapshot_date: Specific date to pull data for (default: None = current date)
        max_workers_outrights: Number of parallel workers for fetching outrights (default: 10)
        max_workers_spreads: Number of parallel workers for calculating spreads (default: 20)
        config_file: Path to indicator configuration JSON file
        external_logger: Optional logger to use instead of module-level logger (for unified logging)
    
    Returns:
        Path to the created CSV file
    """
    # Use external logger if provided, otherwise use module-level logger
    # This allows unified logging when called from ensure_historical_coverage
    data_logger = external_logger if external_logger is not None else logger
    
    # Initialize COM for main thread (required for ICE API)
    # This is needed when using serial processing (no threading)
    pythoncom.CoInitialize()
    
    # Initialize ICE XL Publisher (required per ICE Python documentation)
    # ICE XL Publisher can hibernate when not in use - we need to wake it up
    # Per ICE docs: ICE XL must be installed and authenticated on the same machine
    data_logger.info("Initializing ICE XL Publisher...")
    data_logger.info("  NOTE: ICE XL must be installed and running on this machine")
    data_logger.info("  NOTE: You must be authenticated in ICE XL for API calls to work")
    
    publisher_initialized = False
    try:
        # Try to start publisher with a simple call first
        # If this hangs, ICE XL is likely not running or not accessible
        data_logger.info("  Attempting to start ICE XL Publisher...")
        ice.start_publisher()
        data_logger.info("  ✓ ICE XL Publisher start command sent")
        publisher_initialized = True
        
        # Try to check hibernation status (this may hang if publisher isn't responding)
        try:
            hibernation_status = ice.get_hibernation()
            data_logger.info(f"  ✓ ICE XL Publisher hibernation status: {hibernation_status}")
        except Exception as hiber_error:
            data_logger.warning(f"  ⚠️  Could not check hibernation status: {hiber_error}")
            data_logger.warning(f"  This may indicate ICE XL is not fully started yet")
        
        # Set timeout to prevent hibernation during processing (set to 1 hour = 3600 seconds)
        try:
            ice.set_timeout(3600)
            data_logger.info("  ✓ ICE XL Publisher timeout set to 3600 seconds (1 hour)")
        except Exception as timeout_error:
            data_logger.warning(f"  ⚠️  Could not set timeout: {timeout_error}")
            
    except Exception as pub_error:
        data_logger.error(f"  ✗ ERROR initializing ICE XL Publisher: {pub_error}")
        data_logger.error(f"  Error type: {type(pub_error).__name__}")
        data_logger.error(f"  This usually means:")
        data_logger.error(f"    1. ICE XL is not installed on this machine")
        data_logger.error(f"    2. ICE XL is not running")
        data_logger.error(f"    3. You are not authenticated in ICE XL")
        data_logger.error(f"    4. ICE XL Publisher service is not responding")
        data_logger.error(f"  Please check ICE XL and try again")
        publisher_initialized = False
    
    if not publisher_initialized:
        data_logger.warning("  ⚠️  Continuing anyway - first API call may wake up the publisher")
        data_logger.warning("  ⚠️  If API calls hang, check that ICE XL is running and you are logged in")
    
    # Track execution start time
    execution_start_time = datetime.now()
    
    # Initialize statistics dictionary
    stats = {
        'start_time': execution_start_time.strftime('%Y-%m-%d %H:%M:%S'),
        'status': 'UNKNOWN',
        'error_count': 0,
        'warning_count': 0,
        'failed_symbols': [],
        'outright_duration': 0,
        'spread_duration': 0,
        'indicator_duration': 0,
        'file_write_duration': 0
    }
    
    # Load email configuration
    email_config = load_email_config()
    
    # Load indicator configuration
    config = load_indicator_config(config_file)
    years_back = config['data_settings']['years_back']
    
    # For current processing: use 5 years (approximately 260 weeks) for indicator calculations
    # For historical processing: use 2 years (104 weeks) to avoid API hangs
    if weeks_back is None:
        if snapshot_date is not None:
            # Historical processing - use 2 years to avoid hangs
            weeks_back = 104  # 2 years
            data_logger.info(f"Historical processing: Using 2 years (104 weeks) to avoid API hangs")
        else:
            # Current processing - use full 5 years
            weeks_back = int(years_back * 52.1775)  # Average weeks per year
            data_logger.info(f"Using config setting: {years_back} years = {weeks_back} weeks")
    else:
        # User specified weeks_back
        if snapshot_date is not None:
            # Historical - limit to 2 years
            weeks_back = min(weeks_back, 104)
            data_logger.info(f"Historical processing: Limited to {weeks_back} weeks (2 years max) to avoid API hangs")
        else:
            # Current - use full amount
            required_weeks = int(years_back * 52.1775)
            if weeks_back < required_weeks:
                data_logger.warning(f"Requested {weeks_back} weeks but need {required_weeks} weeks for indicators. Using {required_weeks} weeks.")
                weeks_back = required_weeks
    
    data_logger.info("=" * 80)
    data_logger.info("ICE OHLC DATA PULL - STARTING")
    data_logger.info("=" * 80)
    data_logger.info(f"Symbols file: {symbols_file}")
    data_logger.info(f"Weeks back: {weeks_back} (for {years_back} years of history)")
    data_logger.info(f"Output directory: {output_dir}")
    data_logger.info(f"Config file: {config_file}")
    if external_logger is None and log_file is not None:
        data_logger.info(f"Log file: {log_file}")
    
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    data_logger.debug(f"Output directory created/verified: {output_path}")
    
    # Load symbols
    data_logger.info(f"Loading symbols from {symbols_file}...")
    df_symbols = pd.read_csv(symbols_file, keep_default_na=False)
    
    # Separate outrights and spreads
    if 'spread_type' in df_symbols.columns:
        # Filter to only non-quarterly outrights (quarter_numb == 'N')
        true_outrights = df_symbols[
            (df_symbols['spread_type'] == 'outright') & 
            (df_symbols['quarter_numb'] == 'N')
        ].copy()
        
        spreads = df_symbols[df_symbols['spread_type'] == 'spread'].copy()
        
        outright_symbols = true_outrights['ice_symbol'].unique().tolist()
        spread_symbols = spreads['ice_symbol'].unique().tolist()
        
        data_logger.info(f"Symbol breakdown:")
        data_logger.info(f"  Outrights (N only): {len(outright_symbols)}")
        data_logger.info(f"  Spreads: {len(spread_symbols):,}")
        data_logger.info(f"  Total: {len(outright_symbols) + len(spread_symbols):,}")
    else:
        # Fallback: treat all as outrights if no spread_type column
        data_logger.warning("No 'spread_type' column found - treating all symbols as outrights")
        outright_symbols = df_symbols['ice_symbol'].unique().tolist()
        spread_symbols = []
        spreads = pd.DataFrame()
    
    # We will only pull OHLC for outrights, then calculate spreads
    symbols_to_fetch = outright_symbols
    
    # SIMPLIFIED APPROACH: Just request a wide date range and let ICE API return latest available data
    # This matches how ICE XL works - it just gives you the latest data automatically
    if snapshot_date is None:
        # No specific date requested - we'll fetch wide range and find the actual latest date after fetching
        data_logger.info("No date specified - will fetch wide range and use latest available date from returned data")
        snapshot_date = None  # Will be determined after fetching
    else:
        # Use explicitly provided date (for backfill or specific date requests)
        if isinstance(snapshot_date, str):
            snapshot_date = datetime.strptime(snapshot_date, '%Y-%m-%d')
        snapshot_date = get_friday_date(snapshot_date)
        data_logger.info(f"Using explicitly provided date: {snapshot_date.date()}")
    
    # Determine the reference date for calculating the date range
    # If snapshot_date is provided (historical processing), use that as reference
    # Otherwise, use today (current processing)
    if snapshot_date is not None:
        reference_date = snapshot_date
        data_logger.info(f"Historical processing: Using snapshot_date as reference: {snapshot_date.date()}")
        # For historical dates, we only need enough history for indicators
        # Reduce to 2 years (104 weeks) instead of 5 years to avoid API hangs
        # This is still enough for most indicators (EMAs up to 200, etc.)
        historical_weeks_back = min(weeks_back, 104)  # Limit to 2 years for historical
        if weeks_back > 104:
            data_logger.warning(f"⚠️  Reducing historical date range from {weeks_back} weeks to {historical_weeks_back} weeks to avoid API hangs")
            data_logger.warning(f"  This is still sufficient for indicator calculations")
        weeks_back = historical_weeks_back
    else:
        reference_date = datetime.now()
        data_logger.info(f"Current processing: Using today as reference")
    
    # Request weeks_back from reference date (for indicator calculations)
    # This ensures historical dates get proper history leading up to that date
    start_date = reference_date - timedelta(weeks=weeks_back)
    
    # End date: For historical dates, use snapshot_date + buffer
    # For current dates, use next Friday + buffer
    if snapshot_date is not None:
        # Historical: Request up to snapshot_date + 1 week buffer
        fetch_end_date = snapshot_date + timedelta(weeks=1)
        data_logger.info(f"Fetching weekly timeseries: 5 years back from snapshot date")
    else:
        # Current: Get next Friday (weekly data ends on Fridays)
        today = datetime.now()
        days_until_friday = (4 - today.weekday()) % 7  # 0=Mon, 4=Fri
        if days_until_friday == 0 and today.weekday() != 4:
            days_until_friday = 7  # If today is not Friday, go to next Friday
        next_friday = today + timedelta(days=days_until_friday)
        # Add 1 more week buffer to ensure we get latest data
        fetch_end_date = next_friday + timedelta(weeks=1)
        data_logger.info(f"Fetching weekly timeseries: 5 years back from today")
    
    data_logger.info(f"Date range: {start_date.date()} to {fetch_end_date.date()}")
    if snapshot_date is not None:
        data_logger.info(f"Will filter output to snapshot date: {snapshot_date.date()}")
        data_logger.info(f"Note: Using 2 years of history (instead of 5) for historical dates to avoid API hangs")
    else:
        data_logger.info(f"API will return latest available data (will determine actual date after fetch)")
    
    # Test ICE API connection with a quick, simple call
    # This helps diagnose issues before processing all symbols
    # Use a timeout to prevent hanging if ICE XL is not responding
    data_logger.info("Testing ICE API connection with a simple test call...")
    data_logger.info("  This will verify ICE XL is accessible before processing all symbols")
    
    connection_test_passed = False
    try:
        test_start = datetime.now()
        test_symbol = '%PRL 1!-IEU'  # Simple, commonly available symbol
        test_end_date = datetime.now()
        test_start_date = test_end_date - timedelta(weeks=2)  # Just 2 weeks of data
        
        data_logger.info(f"  Test symbol: {test_symbol}")
        data_logger.info(f"  Test date range: {test_start_date.date()} to {test_end_date.date()}")
        
        # Use ThreadPoolExecutor with short timeout (10 seconds) just for the test
        def test_ice_call():
            pythoncom.CoInitialize()
            try:
                return ice.get_timeseries(
                    [test_symbol],
                    ['Close'],  # Just one field for speed
                    'W',
                    test_start_date.strftime('%Y-%m-%d'),
                    test_end_date.strftime('%Y-%m-%d')
                )
            finally:
                pythoncom.CoUninitialize()
        
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(test_ice_call)
            test_result = future.result(timeout=10)  # 10 second timeout for test
        
        test_duration = (datetime.now() - test_start).total_seconds()
        
        if test_result is not None and len(test_result) > 0:
            data_logger.info(f"  ✓ ICE API connection test PASSED ({test_duration:.2f}s)")
            data_logger.info(f"    Test returned {len(test_result)} rows of data")
            connection_test_passed = True
        else:
            data_logger.warning(f"  ⚠️  ICE API connection test returned no data ({test_duration:.2f}s)")
            data_logger.warning(f"    API call succeeded but returned None or empty result")
            data_logger.warning(f"    This may indicate the symbol has no data, but API is working")
            connection_test_passed = True  # Still consider it passed if we got a response
            
    except FutureTimeoutError:
        data_logger.error(f"  ✗ ICE API connection test TIMEOUT (10 seconds)")
        data_logger.error(f"    The API call took longer than 10 seconds")
        data_logger.error(f"    This indicates ICE XL Publisher is not responding")
        data_logger.error(f"    Please check:")
        data_logger.error(f"      1. ICE XL is installed and running")
        data_logger.error(f"      2. You are logged into ICE XL")
        data_logger.error(f"      3. ICE XL Publisher service is running")
        data_logger.error(f"    Continuing anyway - will attempt actual data fetch...")
        connection_test_passed = False
    except (SystemError, RuntimeError) as sys_error:
        data_logger.error(f"  ✗ ICE API connection test FAILED with System/Runtime error")
        data_logger.error(f"    Error: {sys_error}")
        data_logger.error(f"    Error type: {type(sys_error).__name__}")
        data_logger.error(f"    This may indicate COM/.NET interop issues")
        data_logger.error(f"    Continuing anyway - will attempt actual data fetch...")
        connection_test_passed = False
    except Exception as test_error:
        data_logger.error(f"  ✗ ICE API connection test FAILED")
        data_logger.error(f"    Error: {test_error}")
        data_logger.error(f"    Error type: {type(test_error).__name__}")
        data_logger.error(f"    Continuing anyway - will attempt actual data fetch...")
        connection_test_passed = False
    
    if connection_test_passed:
        data_logger.info("  Proceeding with data fetch - ICE API appears to be working")
    else:
        data_logger.warning("  ⚠️  Connection test failed, but proceeding anyway")
        data_logger.warning("  ⚠️  If data fetch hangs, ICE XL is likely not accessible")
    
    # STEP 1: Fetch OHLC data for OUTRIGHTS ONLY (PARALLEL)
    data_logger.info(f"\n{'='*80}")
    data_logger.info("STEP 1: Fetching OHLC data for OUTRIGHTS (PARALLEL)")
    data_logger.info(f"{'='*80}")
    outright_start_time = datetime.now()
    data_logger.info(f"Fetching data for {len(symbols_to_fetch)} outright symbols...")
    data_logger.info(f"Date range: {start_date.date()} to {fetch_end_date.date()} (extended to capture latest)")
    
    # Parallel processing configuration
    # NOTE: ICE Python library has threading issues - use 1 worker to avoid hangs and crashes
    # The library's internal tracking crashes/hangs when multiple threads call it simultaneously
    # Use serial processing (1 worker) for reliability
    max_workers = 1  # Force to 1 worker to avoid ICE library threading issues
    if max_workers_outrights > 1:
        data_logger.warning(f"⚠️  Using 1 worker (serial processing) to avoid ICE library threading issues")
        data_logger.warning(f"  Requested {max_workers_outrights} workers, but ICE library requires serial processing")
    data_logger.info(f"Using {max_workers} worker (serial processing) for reliable data fetching")
    
    outright_data_dict = {}  # Store outright data: {symbol: DataFrame}
    # Thread-safe counters
    counter_lock = Lock()
    successful = 0
    failed = 0
    total_rows_fetched = 0
    completed = 0
    
    overall_start_time = datetime.now()
    
    # Start periodic progress logging thread
    progress_stop_event = Event()
    def periodic_progress_logger():
        """Log progress every 20 minutes to show activity during long operations"""
        interval_seconds = 20 * 60  # 20 minutes
        while not progress_stop_event.wait(interval_seconds):
            elapsed = (datetime.now() - overall_start_time).total_seconds()
            with counter_lock:
                current_completed = completed
                current_successful = successful
                current_failed = failed
            if current_completed > 0:
                rate = current_completed / elapsed if elapsed > 0 else 0
                remaining = (len(symbols_to_fetch) - current_completed) / rate if rate > 0 else 0
                data_logger.info(f"⏱️  PERIODIC PROGRESS UPDATE (Outright Fetch):")
                data_logger.info(f"   Elapsed: {elapsed/3600:.2f} hours ({elapsed/60:.1f} minutes)")
                data_logger.info(f"   Progress: {current_completed}/{len(symbols_to_fetch)} ({current_completed/len(symbols_to_fetch)*100:.1f}%)")
                data_logger.info(f"   Success: {current_successful}, Failed: {current_failed}")
                if rate > 0:
                    data_logger.info(f"   Rate: {rate:.2f} symbols/min, Est. remaining: {remaining/60:.1f} minutes")
                data_logger.info("")
    
    progress_thread = Thread(target=periodic_progress_logger, daemon=True)
    progress_thread.start()
    
    def fetch_symbol_direct(symbol):
        """Direct fetch function for serial processing (no threading)"""
        # COM is already initialized in main thread, so we can call ICE API directly
        symbol_start_time = datetime.now()
        data_logger.info(f"Fetching outright: {symbol}...")
        
        try:
            df = fetch_symbol_ohlc(symbol, start_date, fetch_end_date)
        except SystemError as sys_error:
            # Catch .NET/COM exceptions from ICE library
            symbol_duration = (datetime.now() - symbol_start_time).total_seconds()
            data_logger.error(f"✗ SystemError in fetch_symbol_ohlc for {symbol} after {symbol_duration:.2f}s: {sys_error}")
            data_logger.error(f"  This is likely a threading issue with the ICE library")
            df = None
        except Exception as e:
            symbol_duration = (datetime.now() - symbol_start_time).total_seconds()
            data_logger.error(f"✗ Exception in fetch_symbol_ohlc for {symbol} after {symbol_duration:.2f}s: {e}")
            data_logger.error(f"  Error type: {type(e).__name__}")
            data_logger.error(traceback.format_exc())
            df = None
        
        symbol_duration = (datetime.now() - symbol_start_time).total_seconds()
        return symbol, df, symbol_duration
    
    def fetch_symbol_wrapper(symbol):
        """Wrapper function for parallel execution - initializes COM for each thread"""
        nonlocal successful, failed, total_rows_fetched, completed
        # Initialize COM for this thread (required for win32com/ICE API)
        pythoncom.CoInitialize()
        try:
            symbol_start_time = datetime.now()
            data_logger.info(f"Fetching outright: {symbol}...")
            
            try:
                df = fetch_symbol_ohlc(symbol, start_date, fetch_end_date)
            except SystemError as sys_error:
                # Catch .NET/COM exceptions from ICE library
                symbol_duration = (datetime.now() - symbol_start_time).total_seconds()
                data_logger.error(f"✗ SystemError in fetch_symbol_ohlc for {symbol} after {symbol_duration:.2f}s: {sys_error}")
                data_logger.error(f"  This is likely a threading issue with the ICE library")
                df = None
            except Exception as e:
                symbol_duration = (datetime.now() - symbol_start_time).total_seconds()
                data_logger.error(f"✗ Exception in fetch_symbol_ohlc for {symbol} after {symbol_duration:.2f}s: {e}")
                data_logger.error(f"  Error type: {type(e).__name__}")
                data_logger.error(traceback.format_exc())
                df = None
            
            symbol_duration = (datetime.now() - symbol_start_time).total_seconds()
            
            with counter_lock:
                completed += 1
                if df is not None and len(df) > 0:
                    successful += 1
                    total_rows_fetched += len(df)
                    data_logger.debug(f"  ✓ {symbol}: Success in {symbol_duration:.2f}s - {len(df)} rows")
                else:
                    failed += 1
                    if symbol not in stats['failed_symbols']:
                        stats['failed_symbols'].append(symbol)
                    data_logger.warning(f"  ✗ {symbol}: Failed in {symbol_duration:.2f}s - No data returned")
                
                # Progress update every 10 completions
                if completed % 10 == 0 or completed == len(symbols_to_fetch):
                    elapsed = (datetime.now() - overall_start_time).total_seconds()
                    rate = completed / elapsed if elapsed > 0 else 0
                    remaining = (len(symbols_to_fetch) - completed) / rate if rate > 0 else 0
                    data_logger.info(f"  Progress: {completed}/{len(symbols_to_fetch)} ({completed/len(symbols_to_fetch)*100:.1f}%) - "
                               f"Success: {successful}, Failed: {failed} - "
                               f"Elapsed: {elapsed/60:.1f}min, Est. remaining: {remaining/60:.1f}min")
            
            return symbol, df
        except Exception as e:
            symbol_duration = (datetime.now() - symbol_start_time).total_seconds() if 'symbol_start_time' in locals() else 0
            data_logger.error(f"✗ Fatal exception in fetch_symbol_wrapper for {symbol} after {symbol_duration:.2f}s: {e}")
            data_logger.error(traceback.format_exc())
            with counter_lock:
                completed += 1
                failed += 1
                if symbol not in stats['failed_symbols']:
                    stats['failed_symbols'].append(symbol)
            return symbol, None
        finally:
            # Uninitialize COM for this thread
            pythoncom.CoUninitialize()
    
    # Execute using ThreadPoolExecutor (or serial if max_workers=1)
    if max_workers == 1:
        # Serial processing - call directly to avoid threading issues with ICE library
        data_logger.info("Using serial processing (no threading) to avoid ICE library issues")
        for symbol in symbols_to_fetch:
            try:
                result_symbol, df, symbol_duration = fetch_symbol_direct(symbol)
                
                # Update counters
                with counter_lock:
                    completed += 1
                    if df is not None and len(df) > 0:
                        successful += 1
                        total_rows_fetched += len(df)
                        data_logger.debug(f"  ✓ {result_symbol}: Success in {symbol_duration:.2f}s - {len(df)} rows")
                    else:
                        failed += 1
                        if result_symbol not in stats['failed_symbols']:
                            stats['failed_symbols'].append(result_symbol)
                        data_logger.warning(f"  ✗ {result_symbol}: Failed in {symbol_duration:.2f}s - No data returned")
                    
                    # Progress update every 10 completions
                    if completed % 10 == 0 or completed == len(symbols_to_fetch):
                        elapsed = (datetime.now() - overall_start_time).total_seconds()
                        rate = completed / elapsed if elapsed > 0 else 0
                        remaining = (len(symbols_to_fetch) - completed) / rate if rate > 0 else 0
                        data_logger.info(f"  Progress: {completed}/{len(symbols_to_fetch)} ({completed/len(symbols_to_fetch)*100:.1f}%) - "
                                   f"Success: {successful}, Failed: {failed} - "
                                   f"Elapsed: {elapsed/60:.1f}min, Est. remaining: {remaining/60:.1f}min")
                
                # Update dictionary
                if df is not None and len(df) > 0:
                    # Apply conversion factor to convert to $/usg
                    symbol_row = true_outrights[true_outrights['ice_symbol'] == result_symbol]
                    if len(symbol_row) > 0:
                        conversion_factor = symbol_row.iloc[0]['convert_to_$usg']
                        df_converted = apply_conversion_factor(df, conversion_factor)
                        if conversion_factor != 'n/a' and conversion_factor != '':
                            data_logger.debug(f"  Applied conversion {conversion_factor} to {result_symbol}")
                        outright_data_dict[result_symbol] = df_converted
                    else:
                        # Fallback if symbol not found in matrix
                        data_logger.warning(f"Symbol {result_symbol} not found in matrix, storing without conversion")
                        outright_data_dict[result_symbol] = df
            except Exception as e:
                with counter_lock:
                    completed += 1
                    failed += 1
                    if symbol not in stats['failed_symbols']:
                        stats['failed_symbols'].append(symbol)
                    stats['error_count'] += 1
                data_logger.error(f"Exception processing {symbol}: {e}", exc_info=True)
    else:
        # Parallel processing
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_symbol = {executor.submit(fetch_symbol_wrapper, symbol): symbol 
                               for symbol in symbols_to_fetch}
            
            # Process completed tasks as they finish and collect results
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    result_symbol, df = future.result()
                    # Update dictionary (each thread writes to different key, so thread-safe)
                    if df is not None and len(df) > 0:
                        # Apply conversion factor to convert to $/usg
                        symbol_row = true_outrights[true_outrights['ice_symbol'] == result_symbol]
                        if len(symbol_row) > 0:
                            conversion_factor = symbol_row.iloc[0]['convert_to_$usg']
                            df_converted = apply_conversion_factor(df, conversion_factor)
                            if conversion_factor != 'n/a' and conversion_factor != '':
                                data_logger.debug(f"  Applied conversion {conversion_factor} to {result_symbol}")
                            outright_data_dict[result_symbol] = df_converted
                        else:
                            # Fallback if symbol not found in matrix
                            data_logger.warning(f"Symbol {result_symbol} not found in matrix, storing without conversion")
                            outright_data_dict[result_symbol] = df
                except Exception as e:
                    with counter_lock:
                        failed += 1
                        if symbol not in stats['failed_symbols']:
                            stats['failed_symbols'].append(symbol)
                        stats['error_count'] += 1
                    data_logger.error(f"Exception fetching {symbol}: {e}", exc_info=True)
    
    # Stop periodic progress logging
    progress_stop_event.set()
    progress_thread.join(timeout=5)  # Wait up to 5 seconds for thread to finish
    
    overall_duration = (datetime.now() - overall_start_time).total_seconds()
    stats['outright_duration'] = overall_duration
    data_logger.info(f"\nOutright data fetch complete in {overall_duration/60:.2f} minutes")
    data_logger.info(f"  Successful: {successful}/{len(symbols_to_fetch)}")
    data_logger.info(f"  Failed: {failed}/{len(symbols_to_fetch)}")
    data_logger.info(f"  Total rows fetched: {total_rows_fetched:,}")
    data_logger.info(f"  Speed improvement: ~{max_workers}x faster with parallel processing")
    
    if len(outright_data_dict) == 0:
        data_logger.error("No outright data fetched! Cannot calculate spreads.")
        data_logger.error("This usually means all ICE API calls failed - check ICE connection")
        # Cleanup COM
        try:
            pythoncom.CoUninitialize()
        except:
            pass
        return None
    
    # STEP 1.5: Calculate OHLC for QUARTERLIES from monthly components
    data_logger.info(f"\n{'='*80}")
    data_logger.info("STEP 1.5: Calculating OHLC for QUARTERLIES from monthly components")
    data_logger.info(f"{'='*80}")
    
    # Get quarterly outrights
    quarterly_outrights = df_symbols[
        (df_symbols['spread_type'] == 'outright') & 
        (df_symbols['quarter_numb'] == 'Y')
    ].copy()
    
    if len(quarterly_outrights) > 0:
        data_logger.info(f"Calculating {len(quarterly_outrights)} quarterly symbols from monthly components...")
        quarterly_count = 0
        quarterly_failed = 0
        
        for idx, row in quarterly_outrights.iterrows():
            quarterly_symbol = row['ice_symbol']
            component_symbols_str = row.get('component_symbols', '')
            
            if not component_symbols_str or component_symbols_str == 'n/a' or component_symbols_str == '':
                data_logger.warning(f"  ✗ {quarterly_symbol}: No component_symbols found")
                quarterly_failed += 1
                continue
            
            # Parse component symbols (comma-separated)
            # Handle CSV parsing issues - component_symbols may have quotes or extra commas
            component_symbols_str = component_symbols_str.strip().strip('"').strip("'")
            component_symbols = [s.strip() for s in component_symbols_str.split(',')]
            
            # Filter out any entries that are just month codes (single letters) - we need full symbols
            component_symbols = [s for s in component_symbols if s.startswith('%')]
            
            if len(component_symbols) < 3:
                data_logger.warning(f"  ✗ {quarterly_symbol}: Invalid component_symbols (got {len(component_symbols)} symbols, need 3): {component_symbols_str}")
                quarterly_failed += 1
                continue
            
            # Check if all component symbols have data
            missing_components = [s for s in component_symbols if s not in outright_data_dict]
            if missing_components:
                data_logger.warning(f"  ✗ {quarterly_symbol}: Missing component data for {missing_components}")
                quarterly_failed += 1
                continue
            
            # Calculate quarterly OHLC
            # NOTE: Monthly data is already converted to $/usg, so pass conversion_factor=None
            # The quarterly formula in the matrix already includes the conversion if needed
            quarterly_df = calculate_quarterly_ohlc(
                component_symbols, 
                outright_data_dict, 
                conversion_factor=None  # Monthly data already converted
            )
            
            if quarterly_df is not None and len(quarterly_df) > 0:
                # Store quarterly data (already in correct units from monthly conversion)
                outright_data_dict[quarterly_symbol] = quarterly_df
                quarterly_count += 1
                data_logger.debug(f"  ✓ {quarterly_symbol}: Calculated from {len(component_symbols)} components - {len(quarterly_df)} rows")
            else:
                data_logger.warning(f"  ✗ {quarterly_symbol}: Failed to calculate quarterly OHLC")
                quarterly_failed += 1
        
        data_logger.info(f"Quarterly calculation complete:")
        data_logger.info(f"  Successful: {quarterly_count}/{len(quarterly_outrights)}")
        data_logger.info(f"  Failed: {quarterly_failed}/{len(quarterly_outrights)}")
        data_logger.info(f"  Total outright symbols now available: {len(outright_data_dict)}")
    else:
        data_logger.info("No quarterly symbols found in matrix")
    
    # STEP 2: Calculate OHLC for SPREADS (PARALLEL)
    data_logger.info(f"\n{'='*80}")
    data_logger.info("STEP 2: Calculating OHLC for SPREADS (PARALLEL)")
    data_logger.info(f"{'='*80}")
    data_logger.info(f"Calculating spreads from {len(outright_data_dict)} outright symbols...")
    data_logger.info(f"Total spreads to calculate: {len(spread_symbols):,}")
    
    # Parallel processing for spread calculations
    spread_max_workers = max_workers_spreads  # More workers for CPU-bound calculations
    data_logger.info(f"Using {spread_max_workers} parallel workers for spread calculations")
    
    spread_data_dict = {}  # Store calculated spread data: {spread_formula: DataFrame}
    spread_counter_lock = Lock()
    spread_successful = 0
    spread_failed = 0
    spread_completed = 0
    
    spread_start_time = datetime.now()
    
    # Start periodic progress logging for spread calculation
    spread_progress_stop_event = Event()
    def periodic_spread_progress_logger():
        """Log progress every 20 minutes during spread calculation"""
        interval_seconds = 20 * 60  # 20 minutes
        while not spread_progress_stop_event.wait(interval_seconds):
            elapsed = (datetime.now() - spread_start_time).total_seconds()
            with spread_counter_lock:
                current_completed = spread_completed
                current_successful = spread_successful
                current_failed = spread_failed
            if current_completed > 0:
                rate = current_completed / elapsed if elapsed > 0 else 0
                remaining = (len(spread_symbols) - current_completed) / rate if rate > 0 else 0
                data_logger.info(f"⏱️  PERIODIC PROGRESS UPDATE (Spread Calculation):")
                data_logger.info(f"   Elapsed: {elapsed/3600:.2f} hours ({elapsed/60:.1f} minutes)")
                data_logger.info(f"   Progress: {current_completed:,}/{len(spread_symbols):,} ({current_completed/len(spread_symbols)*100:.1f}%)")
                data_logger.info(f"   Success: {current_successful:,}, Failed: {current_failed:,}")
                if rate > 0:
                    data_logger.info(f"   Rate: {rate:.0f} spreads/min, Est. remaining: {remaining/60:.1f} minutes")
                data_logger.info("")
    
    spread_progress_thread = Thread(target=periodic_spread_progress_logger, daemon=True)
    spread_progress_thread.start()
    
    def calculate_spread_wrapper(spread_formula):
        """Wrapper function for parallel spread calculation"""
        nonlocal spread_successful, spread_failed, spread_completed
        
        # Get component symbols from the spreads dataframe
        spread_row = spreads[spreads['ice_symbol'] == spread_formula]
        
        if len(spread_row) == 0:
            with spread_counter_lock:
                spread_failed += 1
                spread_completed += 1
            return spread_formula, None
        
        symbol_1 = spread_row.iloc[0]['symbol_1']
        symbol_2 = spread_row.iloc[0]['symbol_2']
        
        if pd.isna(symbol_1) or pd.isna(symbol_2) or symbol_1 == '' or symbol_2 == '':
            with spread_counter_lock:
                spread_failed += 1
                spread_completed += 1
            return spread_formula, None
        
        # For quarterly formulas (starting with '='), try to find matching quarterly in outright_data_dict
        # The quarterly is stored with conversion factor appended in the matrix, but symbol_1/symbol_2 might not have it
        # So we need to try both: symbol as-is, and symbol with conversion
        def find_quarterly_in_dict(symbol, spread_row_meta, is_symbol_1=True):
            """Helper to find quarterly symbol in outright_data_dict, trying with/without conversion"""
            if not symbol.startswith('='):
                # Not a quarterly formula, return as-is
                return symbol
            
            # First try direct lookup (quarterly might be stored without conversion)
            if symbol in outright_data_dict:
                return symbol
            
            # Try with conversion appended (this is how quarterlies are stored in the matrix)
            conversion_key = 'convert_to_$usg' if is_symbol_1 else 'convert_to_$usg_2'
            conversion = spread_row_meta.get(conversion_key, '')
            
            if conversion and conversion != 'n/a' and conversion != '':
                try_symbol = f"{symbol}{conversion}"
                if try_symbol in outright_data_dict:
                    data_logger.debug(f"  Found quarterly with conversion from metadata: {symbol} -> {try_symbol}")
                    return try_symbol
            
            # Try common conversions as fallback (in case metadata doesn't have it)
            for conv in ['/521', '/42']:
                if not symbol.endswith(conv):
                    try_symbol = f"{symbol}{conv}"
                    if try_symbol in outright_data_dict:
                        data_logger.debug(f"  Found quarterly with common conversion fallback: {symbol} -> {try_symbol}")
                        return try_symbol
            
            # Not found, return original (will cause error downstream)
            data_logger.debug(f"  Could not find quarterly in dict: {symbol} (tried with conversions: {conversion}, /521, /42)")
            return symbol
        
        # Look up both symbols
        lookup_symbol_1 = find_quarterly_in_dict(symbol_1, spread_row.iloc[0], is_symbol_1=True)
        lookup_symbol_2 = find_quarterly_in_dict(symbol_2, spread_row.iloc[0], is_symbol_1=False)
        
        # Calculate spread OHLC
        spread_df = calculate_spread_ohlc(lookup_symbol_1, lookup_symbol_2, outright_data_dict)
        
        with spread_counter_lock:
            spread_completed += 1
            if spread_df is not None and len(spread_df) > 0:
                spread_successful += 1
                if spread_completed % 1000 == 0 or spread_completed == len(spread_symbols):
                    elapsed = (datetime.now() - spread_start_time).total_seconds()
                    rate = spread_completed / elapsed if elapsed > 0 else 0
                    remaining = (len(spread_symbols) - spread_completed) / rate if rate > 0 else 0
                    data_logger.info(f"  Progress: {spread_completed}/{len(spread_symbols):,} ({spread_completed/len(spread_symbols)*100:.1f}%) - "
                               f"Success: {spread_successful:,}, Failed: {spread_failed:,} - "
                               f"Elapsed: {elapsed/60:.1f}min, Est. remaining: {remaining/60:.1f}min")
            else:
                spread_failed += 1
                # Log failure reason for debugging (use lookup_symbols which are the actual keys we tried)
                if lookup_symbol_1 not in outright_data_dict:
                    data_logger.debug(f"  ✗ {spread_formula}: Missing symbol_1 '{lookup_symbol_1}' (original: '{symbol_1}') in outright_data_dict")
                elif lookup_symbol_2 not in outright_data_dict:
                    data_logger.debug(f"  ✗ {spread_formula}: Missing symbol_2 '{lookup_symbol_2}' (original: '{symbol_2}') in outright_data_dict")
                else:
                    data_logger.debug(f"  ✗ {spread_formula}: No common dates or insufficient data for {lookup_symbol_1} - {lookup_symbol_2}")
        
        return spread_formula, spread_df
    
    # Execute spread calculations in parallel
    with ThreadPoolExecutor(max_workers=spread_max_workers) as executor:
        # Submit all tasks
        future_to_spread = {executor.submit(calculate_spread_wrapper, spread_formula): spread_formula 
                            for spread_formula in spread_symbols}
        
        # Process completed tasks as they finish
        for future in as_completed(future_to_spread):
            spread_formula = future_to_spread[future]
            try:
                formula, spread_df = future.result()
                if spread_df is not None and len(spread_df) > 0:
                    spread_data_dict[formula] = spread_df
            except Exception as e:
                with spread_counter_lock:
                    spread_failed += 1
                data_logger.error(f"Exception calculating spread {spread_formula}: {e}", exc_info=True)
    
    # Stop periodic progress logging for spreads
    spread_progress_stop_event.set()
    spread_progress_thread.join(timeout=5)  # Wait up to 5 seconds for thread to finish
    
    spread_duration = (datetime.now() - spread_start_time).total_seconds()
    stats['spread_duration'] = spread_duration
    data_logger.info(f"\nSpread calculation complete in {spread_duration/60:.2f} minutes")
    data_logger.info(f"  Successful: {spread_successful}/{len(spread_symbols):,}")
    data_logger.info(f"  Failed: {spread_failed}/{len(spread_symbols):,}")
    data_logger.info(f"  Speed improvement: ~{spread_max_workers}x faster with parallel processing")
    
    # STEP 3: Calculate indicators and combine all data (outrights + calculated spreads)
    data_logger.info(f"\n{'='*80}")
    data_logger.info("STEP 3: Calculating indicators and combining all data")
    data_logger.info(f"{'='*80}")
    indicator_start_time = datetime.now()
    
    all_data = []
    
    # Create lookup dictionary for symbol metadata
    symbol_metadata = {}
    for _, row in df_symbols.iterrows():
        ice_symbol = row.get('ice_symbol', '')
        spread_type = row.get('spread_type', 'outright')
        is_outright = spread_type == 'outright'
        
        # For monthly outrights, format as formula: =('SYMBOL') or =('SYMBOL')/CONVERSION
        # (Quarterlies already have conversion in their formula, spreads have it in their formula)
        if is_outright and row.get('quarter_numb', 'N') == 'N':  # Monthly outright only
            # Format as formula: =('SYMBOL') or =('SYMBOL')/CONVERSION
            conversion = row.get('convert_to_$usg', '')
            if conversion and conversion != 'n/a' and conversion != '':
                # With conversion: =('%AFE F!-IEU')/521
                spread_name = f"=('{ice_symbol}'){conversion}"
                ice_connect_symbol = f"=('{ice_symbol}'){conversion}"
            else:
                # Without conversion: =('%PRL X!-IEU')
                spread_name = f"=('{ice_symbol}')"
                ice_connect_symbol = f"=('{ice_symbol}')"
        else:
            # Quarterlies and spreads already have formula format
            spread_name = ice_symbol
            ice_connect_symbol = ice_symbol
        
        symbol_metadata[ice_symbol] = {
            'spread_name': spread_name,
            'ice_connect_symbol': ice_connect_symbol,
            'symbol_a': row.get('symbol_1', ''),
            'symbol_b': row.get('symbol_2', ''),
            'is_outright': is_outright
        }
    
    # Process outright data with indicators and metadata
    data_logger.info("Processing outrights with indicators and metadata...")
    for symbol, df in outright_data_dict.items():
        if df is None or len(df) == 0:
            continue
        
        # Get symbol metadata
        symbol_info = symbol_metadata.get(symbol, {
            'spread_name': symbol,
            'ice_connect_symbol': symbol,
            'symbol_a': symbol,
            'symbol_b': '',
            'is_outright': True
        })
        
        # Calculate technical indicators (placeholder for now)
        df_with_indicators = calculate_technical_indicators(df, symbol_info, config)
        
        if df_with_indicators is None or len(df_with_indicators) == 0:
            continue
        
        # Reset index to have Date as column
        df_result = df_with_indicators.reset_index()
        
        # Add metadata columns
        df_result['ice_connect_symbol'] = symbol_info.get('ice_connect_symbol', symbol)
        df_result['spread_name'] = symbol_info['spread_name']
        df_result['symbol_a'] = symbol_info['symbol_a']
        df_result['symbol_b'] = symbol_info['symbol_b']
        df_result['is_outright'] = symbol_info['is_outright']
        
        # Rename OHLC columns
        df_result = df_result.rename(columns={
            'open': 'open_price',
            'high': 'high_price',
            'low': 'low_price',
            'close': 'close_price'
        })
        
        # Add data_points (count of weeks for this symbol up to each date)
        # Sort by date first to ensure correct cumulative count
        df_result = df_result.sort_values('Date')
        df_result['data_points'] = range(1, len(df_result) + 1)
        
        all_data.append(df_result)
    
    # Process spread data with indicators and metadata
    data_logger.info("Processing spreads with indicators and metadata...")
    for spread_formula, df in spread_data_dict.items():
        if df is None or len(df) == 0:
            continue
        
        # Get symbol metadata
        symbol_info = symbol_metadata.get(spread_formula, {
            'spread_name': spread_formula,
            'symbol_a': '',
            'symbol_b': '',
            'is_outright': False
        })
        
        # Calculate technical indicators
        df_with_indicators = calculate_technical_indicators(df, symbol_info, config)
        
        if df_with_indicators is None or len(df_with_indicators) == 0:
            continue
        
        # Reset index to have Date as column
        df_result = df_with_indicators.reset_index()
        
        # Add metadata columns
        df_result['ice_connect_symbol'] = spread_formula
        df_result['spread_name'] = symbol_info['spread_name']
        df_result['symbol_a'] = symbol_info['symbol_a']
        df_result['symbol_b'] = symbol_info['symbol_b']
        df_result['is_outright'] = symbol_info['is_outright']
        
        # Calculate correlation and cointegration for spreads
        symbol_a = symbol_info.get('symbol_a', '')
        symbol_b = symbol_info.get('symbol_b', '')
        
        if symbol_a and symbol_b:
            # Get spread row metadata for conversion factor lookup (if available)
            # Try to get from df_symbols if this is a spread
            spread_row_meta = None
            if not symbol_info.get('is_outright', True):
                # This is a spread - try to find the spread row for metadata
                # Note: df_symbols is available in the function scope
                try:
                    spread_formula = symbol_info.get('spread_name', '')
                    if spread_formula:
                        # Access df_symbols from the outer function scope
                        # We're inside pull_all_ohlc_data, so df_symbols should be available
                        # Use a try-except to handle if it's not in scope
                        try:
                            spread_row = df_symbols[df_symbols['ice_symbol'] == spread_formula]
                            if len(spread_row) > 0:
                                spread_row_meta = spread_row.iloc[0].to_dict()
                        except NameError:
                            # df_symbols not in scope, skip metadata lookup
                            pass
                except Exception as e:
                    data_logger.debug(f"Could not get spread row metadata for correlation: {e}")
            
            # Calculate correlation and cointegration
            spread_stats = calculate_correlation_and_cointegration(
                df, symbol_a, symbol_b, outright_data_dict, config, spread_row_meta=spread_row_meta
            )
            
            if spread_stats:
                # Add correlation and cointegration columns (same value for all rows)
                df_result['correlation_52w'] = spread_stats['correlation']
                df_result['cointegration_pvalue'] = spread_stats['cointegration_pvalue']
                df_result['cointegration_statistic'] = spread_stats['cointegration_statistic']
                df_result['is_cointegrated'] = spread_stats['is_cointegrated']
            else:
                # Set to NaN if calculation failed
                df_result['correlation_52w'] = np.nan
                df_result['cointegration_pvalue'] = np.nan
                df_result['cointegration_statistic'] = np.nan
                df_result['is_cointegrated'] = False
        else:
            # No component symbols - set to NaN
            df_result['correlation_52w'] = np.nan
            df_result['cointegration_pvalue'] = np.nan
            df_result['cointegration_statistic'] = np.nan
            df_result['is_cointegrated'] = False
        
        # Rename OHLC columns
        df_result = df_result.rename(columns={
            'open': 'open_price',
            'high': 'high_price',
            'low': 'low_price',
            'close': 'close_price'
        })
        
        # Add data_points (count of weeks for this symbol up to each date)
        # Sort by date first to ensure correct cumulative count
        df_result = df_result.sort_values('Date')
        df_result['data_points'] = range(1, len(df_result) + 1)
        
        all_data.append(df_result)
    
    data_logger.info(f"Processed {len(outright_data_dict)} outrights + {len(spread_data_dict)} spreads = {len(all_data)} total symbols")
    
    if len(all_data) == 0:
        data_logger.error("No data to combine!")
        return None
    
    # Combine all data
    data_logger.info("Combining all symbol data...")
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Sort by Date and symbol
    combined_df = combined_df.sort_values(['Date', 'ice_connect_symbol'])
    
    # Track indicator calculation duration
    indicator_duration = (datetime.now() - indicator_start_time).total_seconds()
    stats['indicator_duration'] = indicator_duration
    
    # Determine actual most recent date in the data (this is what ICE API actually returned)
    # Ensure Date is datetime type for proper comparison
    if combined_df['Date'].dtype == 'object':
        combined_df['Date'] = pd.to_datetime(combined_df['Date'])
    
    # Normalize dates to date-only (remove time component) for consistent comparison
    combined_df['Date'] = pd.to_datetime(combined_df['Date']).dt.normalize()
    
    actual_latest_date = combined_df['Date'].max()
    actual_earliest_date = combined_df['Date'].min()
    
    # DIAGNOSTIC: Check what dates are actually in the data
    unique_dates = sorted(combined_df['Date'].unique(), reverse=True)
    data_logger.info(f"\nDate Analysis - What API Actually Returned:")
    data_logger.info(f"  Earliest date: {actual_earliest_date.date()}")
    data_logger.info(f"  Latest date: {actual_latest_date.date()}")
    data_logger.info(f"  Most recent 5 dates in data: {[d.date() for d in unique_dates[:5]]}")
    
    # Count how many symbols have each of the recent dates
    recent_dates = unique_dates[:5]
    data_logger.info(f"\nSymbol count by recent dates:")
    for date in recent_dates:
        count = len(combined_df[combined_df['Date'] == date])
        data_logger.info(f"  {date.date()}: {count:,} symbols")
    
    # SIMPLIFIED: Use the actual latest date from returned data as the snapshot date
    # This matches ICE XL behavior - just use whatever latest date the API returned
    if snapshot_date is None:
        # No date was requested - use the actual latest date from API
        snapshot_date = actual_latest_date
        data_logger.info(f"\nNo date specified - using latest date from API: {actual_latest_date.date()}")
    else:
        # Date was explicitly requested - check if it exists, otherwise use actual latest
        snapshot_date_normalized = pd.to_datetime(snapshot_date).normalize()
        requested_date_exists = (combined_df['Date'] == snapshot_date_normalized).any()
        
        if not requested_date_exists:
            data_logger.warning(f"  ⚠️  Requested date {snapshot_date.date()} not found in returned data!")
            data_logger.warning(f"  Available dates range: {actual_earliest_date.date()} to {actual_latest_date.date()}")
            data_logger.warning(f"  Using actual latest date instead: {actual_latest_date.date()}")
            snapshot_date = actual_latest_date
        else:
            data_logger.info(f"  ✓ Requested date {snapshot_date.date()} found in data")
            snapshot_date = snapshot_date_normalized
    
    # IMPORTANT: Filter to only the snapshot date's data for output
    # We pulled 5 years for indicator calculations, but only output the latest week
    data_logger.info(f"\nFiltering output to snapshot date...")
    data_logger.info(f"  Total rows before filtering: {len(combined_df):,}")
    data_logger.info(f"  Snapshot date: {snapshot_date.date()}")
    data_logger.info(f"  Actual date range in data: {actual_earliest_date.date()} to {actual_latest_date.date()}")
    
    # Filter to only the snapshot date (normalized)
    snapshot_date_normalized = pd.to_datetime(snapshot_date).normalize()
    combined_df = combined_df[combined_df['Date'] == snapshot_date_normalized].copy()
    
    data_logger.info(f"  Rows after filtering: {len(combined_df):,}")
    data_logger.info(f"  Output will contain data for date: {snapshot_date.date()}")
    
    # Verify filtering worked
    unique_dates = combined_df['Date'].nunique()
    if unique_dates > 1:
        data_logger.warning(f"  ⚠️  WARNING: Filtering may have failed - {unique_dates} unique dates still in output!")
        data_logger.warning(f"  Dates found: {combined_df['Date'].unique()[:10]}")
    else:
        data_logger.info(f"  ✓ Filtering successful - only 1 date in output: {snapshot_date.date()}")
    
    # Update actual_latest_date for filename and stats
    actual_latest_date = snapshot_date
    
    data_logger.info(f"\nDate Analysis:")
    data_logger.info(f"  Historical data pulled: {actual_earliest_date.date()} to {actual_latest_date.date()} (for indicator calculations)")
    data_logger.info(f"  Snapshot date used: {actual_latest_date.date()}")
    data_logger.info(f"  Output contains: Most recent week only ({actual_latest_date.date()})")
    
    # Generate output filename with ACTUAL data date (not requested date)
    actual_date_str = actual_latest_date.strftime('%Y-%m-%d')
    output_file = output_path / f"unfiltered_{actual_date_str}.csv"
    
    data_logger.info(f"  Using actual data date for filename: {actual_date_str}")
    data_logger.info(f"\nCombining and saving data...")
    data_logger.debug(f"  Total DataFrames to combine: {len(all_data)}")
    data_logger.debug(f"  Output file: {output_file}")
    
    # Reorder columns: Date first (farthest left), then metadata columns, then OHLC, then indicators
    required_left_columns = ['Date', 'ice_connect_symbol', 'spread_name', 'symbol_a', 'symbol_b', 'is_outright', 'data_points']
    
    # Get all current columns
    all_columns = list(combined_df.columns)
    
    # Ensure required columns exist
    missing_columns = [col for col in required_left_columns if col not in all_columns]
    if missing_columns:
        data_logger.warning(f"  Warning: Missing required columns: {missing_columns}")
        # Remove missing columns from required list
        required_left_columns = [col for col in required_left_columns if col in all_columns]
    
    # Get remaining columns (OHLC, indicators, etc.) in their current order
    remaining_columns = [col for col in all_columns if col not in required_left_columns]
    
    # Create new column order: Date and metadata columns first, then the rest
    new_column_order = required_left_columns + remaining_columns
    
    data_logger.info(f"  Reordering columns: Date + {len(required_left_columns)-1} metadata columns first, then {len(remaining_columns)} data columns")
    combined_df = combined_df[new_column_order]
    
    # Save to CSV
    save_start_time = datetime.now()
    combined_df.to_csv(output_file, index=False)
    save_duration = (datetime.now() - save_start_time).total_seconds()
    stats['file_write_duration'] = save_duration
    
    file_size_mb = output_file.stat().st_size / (1024 * 1024)
    data_logger.info(f"✓ Saved to {output_file} ({file_size_mb:.2f} MB) in {save_duration:.2f}s")
    data_logger.debug(f"  File path: {output_file.absolute()}")
    
    # Calculate final statistics for email
    execution_end_time = datetime.now()
    total_duration = (execution_end_time - execution_start_time).total_seconds()
    
    # Calculate missing data percentage
    total_cells = len(combined_df) * len(combined_df.columns)
    missing_cells = combined_df.isna().sum().sum()
    missing_data_pct = (missing_cells / total_cells * 100) if total_cells > 0 else 0
    
    # Determine status
    if failed == 0 and spread_failed == 0:
        stats['status'] = 'SUCCESS'
    elif failed > 0 or spread_failed > 0:
        stats['status'] = 'WARNINGS'
    else:
        stats['status'] = 'FAILURE'
    
    # Compile all statistics
    stats.update({
        'end_time': execution_end_time.strftime('%Y-%m-%d %H:%M:%S'),
        'duration': f"{total_duration/60:.1f} minutes",
        'duration_minutes': total_duration / 60,
        'total_duration': total_duration,
        'total_symbols': len(outright_data_dict) + len(spread_data_dict),
        'outrights_total': len(symbols_to_fetch),
        'outrights_success': successful,
        'outrights_failed': failed,
        'outrights_success_pct': (successful / len(symbols_to_fetch) * 100) if len(symbols_to_fetch) > 0 else 0,
        'outrights_failed_pct': (failed / len(symbols_to_fetch) * 100) if len(symbols_to_fetch) > 0 else 0,
        'outright_rate': len(symbols_to_fetch) / overall_duration if overall_duration > 0 else 0,
        'spreads_total': len(spread_symbols),
        'spreads_success': spread_successful,
        'spreads_failed': spread_failed,
        'spreads_success_pct': (spread_successful / len(spread_symbols) * 100) if len(spread_symbols) > 0 else 0,
        'spreads_failed_pct': (spread_failed / len(spread_symbols) * 100) if len(spread_symbols) > 0 else 0,
        'spread_rate': len(spread_symbols) / spread_duration if spread_duration > 0 else 0,
        'earliest_date': actual_earliest_date.strftime('%Y-%m-%d'),
        'latest_date': actual_latest_date.strftime('%Y-%m-%d'),
        'total_rows': len(combined_df),
        'unique_symbols': combined_df['ice_connect_symbol'].nunique() if 'ice_connect_symbol' in combined_df.columns else 0,
        'avg_data_points': combined_df['data_points'].mean() if 'data_points' in combined_df.columns else 0,
        'missing_data_pct': missing_data_pct,
        'output_file': str(output_file),
        'file_size_mb': file_size_mb,
        'rows_written': len(combined_df),
        'total_columns': len(combined_df.columns),
        'config_file': config_file,
        'years_back': years_back,
        'weeks_back': weeks_back,
        'max_workers_outrights': max_workers_outrights,
        'max_workers_spreads': max_workers_spreads,
        'symbols_file': symbols_file
    })
    
    data_logger.info("=" * 80)
    data_logger.info("OHLC DATA PULL COMPLETE")
    data_logger.info("=" * 80)
    data_logger.info(f"  Outrights pulled: {len(outright_data_dict)}")
    data_logger.info(f"    Successful: {successful}/{len(symbols_to_fetch)}")
    data_logger.info(f"    Failed: {failed}/{len(symbols_to_fetch)}")
    data_logger.info(f"  Spreads calculated: {len(spread_data_dict):,}")
    data_logger.info(f"    Successful: {spread_successful}/{len(spread_symbols):,}")
    data_logger.info(f"    Failed: {spread_failed}/{len(spread_symbols):,}")
    data_logger.info(f"  Total symbols: {len(outright_data_dict) + len(spread_data_dict):,}")
    data_logger.info(f"  Total rows: {len(combined_df):,}")
    data_logger.info(f"  Date range: {actual_earliest_date.date()} to {actual_latest_date.date()}")
    data_logger.info(f"  Output file: {output_file}")
    data_logger.info(f"  Filename uses actual data date: {actual_date_str}")
    
    print("=" * 80)
    print("OHLC DATA PULL COMPLETE")
    print("=" * 80)
    print(f"  Outrights pulled: {len(outright_data_dict)}")
    print(f"    Successful: {successful}/{len(symbols_to_fetch)}")
    print(f"    Failed: {failed}/{len(symbols_to_fetch)}")
    print(f"  Spreads calculated: {len(spread_data_dict):,}")
    print(f"    Successful: {spread_successful}/{len(spread_symbols):,}")
    print(f"    Failed: {spread_failed}/{len(spread_symbols):,}")
    print(f"  Total symbols: {len(outright_data_dict) + len(spread_data_dict):,}")
    print(f"  Total rows: {len(combined_df):,}")
    print(f"  Date range: {actual_earliest_date.date()} to {actual_latest_date.date()}")
    print(f"  Most recent data date: {actual_latest_date.date()}")
    print(f"  Output file: {output_file}")
    print(f"  (Filename uses actual data date: {actual_date_str})")
    
    # Send email summary
    if email_config:
        data_logger.info("\nSending email summary...")
        # Only attach log file if we're running standalone (log_file exists)
        log_file_for_email = log_file if external_logger is None and log_file is not None else None
        email_sent = send_summary_email(
            stats=stats,
            log_file_path=log_file_for_email,
            output_file_path=output_file,
            email_config=email_config
        )
        if email_sent:
            data_logger.info("✓ Email summary sent successfully")
        else:
            data_logger.warning("⚠ Email summary failed to send (check logs)")
    else:
        data_logger.info("Email notifications disabled (no email config found)")
    
    # Cleanup COM
    try:
        pythoncom.CoUninitialize()
    except:
        pass
    
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
        default=None,
        help='Number of weeks of history to fetch (default: None = use config, always 5 years for indicators)'
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
    parser.add_argument(
        '--workers-outrights',
        type=int,
        default=10,
        help='Number of parallel workers for fetching outrights (default: 10)'
    )
    parser.add_argument(
        '--workers-spreads',
        type=int,
        default=20,
        help='Number of parallel workers for calculating spreads (default: 20)'
    )
    
    args = parser.parse_args()
    
    pull_all_ohlc_data(
        symbols_file=args.symbols,
        weeks_back=args.weeks,
        output_dir=args.output_dir,
        snapshot_date=args.date,
        max_workers_outrights=args.workers_outrights,
        max_workers_spreads=args.workers_spreads
    )

