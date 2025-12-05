"""
Analyze position sizing formula performance across different volatility levels.
Helps understand current behavior and identify potential improvements.
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

def calculate_position_size_current(row, base_size=100):
    """
    Current position sizing formula.
    """
    atr_pct = row.get('atr_pct_of_price', np.nan)
    if pd.isna(atr_pct) or atr_pct <= 0:
        return base_size
    
    # Current formula
    position_size = base_size / (atr_pct / 10)
    
    # Cap between 10% and 200%
    position_size = max(10.0, min(200.0, position_size))
    
    return round(position_size, 2)

def calculate_position_size_improved(row, base_size=100, target_atr_pct=5.0):
    """
    Improved position sizing formula with target ATR% baseline.
    """
    atr_pct = row.get('atr_pct_of_price', np.nan)
    if pd.isna(atr_pct) or atr_pct <= 0:
        return base_size
    
    # Improved formula
    position_size = base_size * (target_atr_pct / atr_pct)
    
    # Cap between 10% and 200%
    position_size = max(10.0, min(200.0, position_size))
    
    return round(position_size, 2)

def analyze_position_sizing(data_file=None):
    """
    Analyze position sizing across different volatility levels.
    """
    print("=" * 80)
    print("POSITION SIZING ANALYSIS")
    print("=" * 80)
    
    # Load data
    if data_file is None:
        data_dir = Path(__file__).parent.parent / 'full_unfiltered_historicals'
        # Get most recent file
        data_files = sorted(data_dir.glob('unfiltered_*.csv'))
        if not data_files:
            print("❌ No data files found!")
            return
        data_file = data_files[-1]
        print(f"\n📊 Using data file: {data_file.name}")
    
    df = pd.read_csv(data_file)
    print(f"✓ Loaded {len(df):,} rows")
    
    # Filter to rows with valid ATR% data
    df_valid = df[df['atr_pct_of_price'].notna() & (df['atr_pct_of_price'] > 0)].copy()
    print(f"✓ {len(df_valid):,} rows with valid ATR% data")
    
    if len(df_valid) == 0:
        print("❌ No valid ATR% data found!")
        return
    
    # Calculate position sizes with both formulas
    df_valid['pos_pct_current'] = df_valid.apply(calculate_position_size_current, axis=1)
    df_valid['pos_pct_improved'] = df_valid.apply(calculate_position_size_improved, axis=1, target_atr_pct=5.0)
    
    # Create volatility buckets
    df_valid['atr_pct_bucket'] = pd.cut(
        df_valid['atr_pct_of_price'],
        bins=[0, 2, 5, 10, 20, 100],
        labels=['<2%', '2-5%', '5-10%', '10-20%', '>20%']
    )
    
    print("\n" + "=" * 80)
    print("CURRENT FORMULA ANALYSIS")
    print("=" * 80)
    
    # Analyze current formula
    analysis_current = df_valid.groupby('atr_pct_bucket').agg({
        'pos_pct_current': ['count', 'mean', 'median', 'min', 'max', 'std'],
        'atr_pct_of_price': 'mean'
    }).round(2)
    
    print("\nPosition Size by Volatility Level (Current Formula):")
    print(analysis_current)
    
    # Check for issues
    print("\n⚠️  Potential Issues (Current Formula):")
    maxed_out = (df_valid['pos_pct_current'] == 200.0).sum()
    min_out = (df_valid['pos_pct_current'] == 10.0).sum()
    
    if maxed_out > len(df_valid) * 0.3:
        print(f"  • {maxed_out:,} positions ({maxed_out/len(df_valid)*100:.1f}%) maxed out at 200%")
        print("    → Formula may be too aggressive for low volatility")
    
    if min_out > len(df_valid) * 0.3:
        print(f"  • {min_out:,} positions ({min_out/len(df_valid)*100:.1f}%) at minimum 10%")
        print("    → Formula may be too conservative for high volatility")
    
    # Distribution
    print(f"\n📊 Distribution (Current Formula):")
    print(df_valid['pos_pct_current'].describe())
    
    print("\n" + "=" * 80)
    print("IMPROVED FORMULA ANALYSIS (Target ATR% = 5.0)")
    print("=" * 80)
    
    # Analyze improved formula
    analysis_improved = df_valid.groupby('atr_pct_bucket').agg({
        'pos_pct_improved': ['count', 'mean', 'median', 'min', 'max', 'std'],
        'atr_pct_of_price': 'mean'
    }).round(2)
    
    print("\nPosition Size by Volatility Level (Improved Formula):")
    print(analysis_improved)
    
    # Distribution
    print(f"\n📊 Distribution (Improved Formula):")
    print(df_valid['pos_pct_improved'].describe())
    
    print("\n" + "=" * 80)
    print("COMPARISON")
    print("=" * 80)
    
    # Compare formulas
    comparison = pd.DataFrame({
        'Current Mean': df_valid.groupby('atr_pct_bucket')['pos_pct_current'].mean(),
        'Improved Mean': df_valid.groupby('atr_pct_bucket')['pos_pct_improved'].mean(),
        'Difference': df_valid.groupby('atr_pct_bucket')['pos_pct_improved'].mean() - 
                     df_valid.groupby('atr_pct_bucket')['pos_pct_current'].mean(),
        'Avg ATR%': df_valid.groupby('atr_pct_bucket')['atr_pct_of_price'].mean()
    }).round(2)
    
    print("\nFormula Comparison by Volatility Level:")
    print(comparison)
    
    # Example calculations
    print("\n" + "=" * 80)
    print("EXAMPLE CALCULATIONS")
    print("=" * 80)
    
    examples = [
        ("Low Volatility", 2.0),
        ("Medium Volatility", 5.0),
        ("High Volatility", 10.0),
        ("Very High Volatility", 20.0),
    ]
    
    print("\nCurrent Formula: pos% = 100 / (atr_pct / 10)")
    print("Improved Formula: pos% = 100 × (5.0 / atr_pct)")
    print()
    
    for label, atr_pct in examples:
        current = max(10.0, min(200.0, 100 / (atr_pct / 10)))
        improved = max(10.0, min(200.0, 100 * (5.0 / atr_pct)))
        print(f"{label:25} (ATR% = {atr_pct:5.1f}%):")
        print(f"  Current:  {current:6.1f}%")
        print(f"  Improved: {improved:6.1f}%")
        print()
    
    print("=" * 80)
    print("RECOMMENDATIONS")
    print("=" * 80)
    print("""
1. Review the distribution statistics above
2. Check if too many positions are hitting the 10% or 200% caps
3. Consider implementing the improved formula with target ATR% = 5.0
4. Adjust target ATR% based on your market's typical volatility
5. Consider adding fixed dollar risk method for professional trading
    """)

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyze position sizing formula')
    parser.add_argument(
        '--file',
        type=str,
        help='Path to data file (default: most recent in full_unfiltered_historicals)'
    )
    
    args = parser.parse_args()
    
    analyze_position_sizing(args.file)


