"""
Main runner script for signal generator system.
Orchestrates data loading, signal generation, and report creation.
"""
import sys
from pathlib import Path
from datetime import datetime
import argparse
import logging
import pandas as pd

# Add signal_generator to path
sys.path.insert(0, str(Path(__file__).parent / 'signal_generator'))

from data_loaders import load_data, prepare_data, load_curve_prices
from config import load_config
from signal_generators import TrendFollowingSignals, EnhancedTrendFollowingSignals, MeanReversionSignals, MacdRsiExhaustionSignals, PointCalculator, ICEChatFormatter
from reports import ReportGenerator

# Setup logging - ensure logs directory exists
logs_dir = Path(__file__).parent / 'signal_generator' / 'logs'
logs_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(logs_dir / 'signal_generator.log')
    ]
)

logger = logging.getLogger(__name__)


def main(target_date=None, data_dir=None):
    """
    Main execution function.
    
    Args:
        target_date: Target date for analysis (None = most recent)
        data_dir: Data directory path (None = default)
    """
    logger.info("=" * 80)
    logger.info("SIGNAL GENERATOR - STARTING")
    logger.info("=" * 80)
    
    try:
        # Step 1: Load configuration
        logger.info("\n[Step 1] Loading configuration...")
        config = load_config()
        logger.info("✓ Configuration loaded")
        
        # Step 2: Load data
        logger.info("\n[Step 2] Loading data...")
        if data_dir is None:
            data_dir = str(Path(__file__).parent / 'full_unfiltered_historicals')
        
        df = load_data(target_date=target_date, data_dir=data_dir)
        if df is None:
            logger.error("Failed to load data")
            return 1
        
        # If target_date is None, determine it from the most recent date in the data
        if target_date is None and 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            most_recent_date_in_data = df['Date'].max()
            if pd.notna(most_recent_date_in_data):
                if isinstance(most_recent_date_in_data, pd.Timestamp):
                    target_date = most_recent_date_in_data.to_pydatetime()
                else:
                    target_date = pd.to_datetime(most_recent_date_in_data).to_pydatetime()
                logger.info(f"No target_date specified, using most recent date in data: {target_date.strftime('%Y-%m-%d')}")
        
        prepared_df = prepare_data(df, target_date=target_date)
        logger.info(f"✓ Data loaded: {len(prepared_df)} rows, {prepared_df['ice_connect_symbol'].nunique()} unique symbols")
        
        # Extract actual data date from DataFrame (from Date column) - use the target_date or most recent
        data_date = None
        if target_date:
            data_date = target_date
        elif 'Date' in prepared_df.columns and len(prepared_df) > 0:
            # Get the most recent date (max) from the filtered data
            date_values = prepared_df['Date'].dropna()
            if len(date_values) > 0:
                data_date = date_values.max()
                # Ensure it's a datetime object
                if isinstance(data_date, pd.Timestamp):
                    data_date = data_date.to_pydatetime()
                elif not isinstance(data_date, datetime):
                    data_date = pd.to_datetime(data_date).to_pydatetime()
                logger.info(f"✓ Data date extracted from DataFrame: {data_date.strftime('%Y-%m-%d')}")
        
        if data_date is None:
            # Fallback to current date
            data_date = datetime.now()
            logger.warning(f"Could not extract data date, using {data_date.strftime('%Y-%m-%d')}")
        
        # Step 3: Initialize components
        logger.info("\n[Step 3] Loading curve data for delta sizing...")
        try:
            curve_data = load_curve_prices()
            logger.info(f"✓ Curve data loaded: {len(curve_data)} commodities")
        except Exception as e:
            logger.warning(f"Could not load curve data: {e}. Delta sizing will use fallback prices.")
            curve_data = {}
        
        logger.info("\n[Step 4] Initializing signal generators...")
        point_calculator = PointCalculator(config)
        
        trend_signal_gen = TrendFollowingSignals(config, point_calculator)
        enhanced_trend_gen = EnhancedTrendFollowingSignals(config, point_calculator)
        mean_reversion_gen = MeanReversionSignals(config, point_calculator)
        macd_rsi_exhaustion_gen = MacdRsiExhaustionSignals(config, point_calculator)
        
        ice_chat_formatter = ICEChatFormatter(config, curve_data=curve_data, prepared_df=prepared_df, data_date=data_date)
        report_generator = ReportGenerator(config)
        
        logger.info("✓ All components initialized")
        
        # Step 5: Generate signals
        logger.info("\n[Step 5] Generating standard trend following signals (MACD cross)...")
        trend_signals = trend_signal_gen.generate_signals(prepared_df, target_date=target_date)
        trend_buy_count = len(trend_signals.get('buy_signals', []))
        trend_sell_count = len(trend_signals.get('sell_signals', []))
        logger.info(f"✓ Standard trend following: {trend_buy_count} buy, {trend_sell_count} sell signals")
        
        logger.info("\n[Step 5.5] Generating enhanced trend following signals (multi-trigger)...")
        enhanced_trend_signals = enhanced_trend_gen.generate_signals(prepared_df, target_date=target_date)
        enhanced_buy_count = len(enhanced_trend_signals.get('buy_signals', []))
        enhanced_sell_count = len(enhanced_trend_signals.get('sell_signals', []))
        logger.info(f"✓ Enhanced trend following: {enhanced_buy_count} buy, {enhanced_sell_count} sell signals")
        
        logger.info("\n[Step 6] Generating mean reversion signals...")
        mean_reversion_signals = mean_reversion_gen.generate_signals(prepared_df, target_date=target_date)
        mr_buy_count = len(mean_reversion_signals.get('buy_signals', []))
        mr_sell_count = len(mean_reversion_signals.get('sell_signals', []))
        logger.info(f"✓ Mean reversion: {mr_buy_count} buy, {mr_sell_count} sell signals")
        
        logger.info("\n[Step 6.5] Generating MACD/RSI exhaustion signals...")
        macd_rsi_exhaustion_signals = macd_rsi_exhaustion_gen.generate_signals(prepared_df, target_date=target_date)
        exhaustion_buy_count = len(macd_rsi_exhaustion_signals.get('buy_signals', []))
        exhaustion_sell_count = len(macd_rsi_exhaustion_signals.get('sell_signals', []))
        logger.info(f"✓ MACD/RSI exhaustion: {exhaustion_buy_count} buy, {exhaustion_sell_count} sell signals")
        
        # Step 7.5: Check prior week signals
        logger.info("\n[Step 7.5] Checking prior week signals...")
        from signal_generator.utils.prior_week_checker import check_prior_week_signals
        
        all_current_signals = {
            'trend_following': trend_signals,
            'mean_reversion': mean_reversion_signals,
            'macd_rsi_exhaustion': macd_rsi_exhaustion_signals
        }
        
        prior_week_results = check_prior_week_signals(
            current_signals=all_current_signals,
            data_date=data_date,
            data_dir=data_dir,
            config=config
        )
        logger.info(f"✓ Prior week check complete: {len(prior_week_results)} signals checked")
        
        # Step 8: Generate report
        logger.info("\n[Step 8] Generating HTML report...")
        total_symbols = prepared_df['ice_connect_symbol'].nunique()
        html_report = report_generator.generate_html_report(
            trend_signals=trend_signals,
            enhanced_trend_signals=enhanced_trend_signals,
            mean_reversion_signals=mean_reversion_signals,
            macd_rsi_exhaustion_signals=macd_rsi_exhaustion_signals,
            ice_chat_formatter=ice_chat_formatter,
            run_date=datetime.now(),
            data_date=data_date,
            total_symbols=total_symbols,
            curve_data=curve_data
        )
        logger.info("✓ HTML report generated")
        
        # Step 9: Save report
        logger.info("\n[Step 9] Saving report...")
        output_path = report_generator.save_report(html_report)
        logger.info(f"✓ Report saved to: {output_path}")
        
        # Step 10: Generate ICE Connect text file
        logger.info("\n[Step 10] Generating ICE Connect text file...")
        ice_connect_file = report_generator.generate_ice_connect_text_file(
            trend_signals=trend_signals,
            enhanced_trend_signals=enhanced_trend_signals,
            mean_reversion_signals=mean_reversion_signals,
            macd_rsi_exhaustion_signals=macd_rsi_exhaustion_signals,
            ice_chat_formatter=ice_chat_formatter,
            data_date=data_date
        )
        if ice_connect_file:
            logger.info(f"✓ ICE Connect text file saved to: {ice_connect_file}")
        else:
            logger.warning("⚠️  Could not generate ICE Connect text file")
        
        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("SIGNAL GENERATION COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Total Signals Generated:")
        logger.info(f"  Trend Following: {trend_buy_count + trend_sell_count} ({trend_buy_count} buy, {trend_sell_count} sell)")
        logger.info(f"  Enhanced Trend Following: {enhanced_buy_count + enhanced_sell_count} ({enhanced_buy_count} buy, {enhanced_sell_count} sell)")
        logger.info(f"  Mean Reversion: {mr_buy_count + mr_sell_count} ({mr_buy_count} buy, {mr_sell_count} sell)")
        logger.info(f"  MACD/RSI Exhaustion: {exhaustion_buy_count + exhaustion_sell_count} ({exhaustion_buy_count} buy, {exhaustion_sell_count} sell)")
        logger.info(f"  Total: {trend_buy_count + trend_sell_count + enhanced_buy_count + enhanced_sell_count + mr_buy_count + mr_sell_count + exhaustion_buy_count + exhaustion_sell_count}")
        logger.info(f"\nReport saved to: {output_path}")
        if ice_connect_file:
            logger.info(f"ICE Connect text file: {ice_connect_file}")
        logger.info("=" * 80)
        
        # Flush output to ensure summary is visible
        import sys
        sys.stdout.flush()
        sys.stderr.flush()
        
        return 0
        
    except Exception as e:
        logger.error(f"Error in signal generation: {e}", exc_info=True)
        logger.error("=" * 80)
        logger.error("SIGNAL GENERATION FAILED - See error above")
        logger.error("=" * 80)
        return 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate trade signals and HTML report')
    parser.add_argument(
        '--date',
        type=str,
        help='Target date for analysis (YYYY-MM-DD). If not specified, uses most recent data.'
    )
    parser.add_argument(
        '--data-dir',
        type=str,
        help='Data directory path (default: full_unfiltered_historicals)'
    )
    
    args = parser.parse_args()
    
    # Parse target date if provided
    target_date = None
    if args.date:
        try:
            target_date = datetime.strptime(args.date, '%Y-%m-%d')
        except ValueError:
            logger.error(f"Invalid date format: {args.date}. Use YYYY-MM-DD")
            sys.exit(1)
    
    exit_code = main(target_date=target_date, data_dir=args.data_dir)
    sys.exit(exit_code)
