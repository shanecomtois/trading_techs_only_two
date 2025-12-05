"""Quick test of spread calculation logic"""
import pandas as pd
import sys
sys.path.insert(0, '.')

# Force output
import sys
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

# Test with your example data
print("=" * 80)
print("TESTING WITH YOUR EXAMPLE DATA")
print("=" * 80)

# Your example
prl_data = pd.DataFrame({
    'open': [0.67],
    'high': [0.7],
    'low': [0.66],
    'close': [0.67]
}, index=[pd.Timestamp('2025-11-28')])

nbi_data = pd.DataFrame({
    'open': [0.82],
    'high': [0.9],
    'low': [0.82],
    'close': [0.85]
}, index=[pd.Timestamp('2025-11-28')])

print("\nPRL Data:")
print(prl_data)
print("\nNBI Data:")
print(nbi_data)

# Test current logic
from pull_ohlc_data import calculate_spread_ohlc

comp_dict = {'%PRL F!-IEU': prl_data, '%NBI F!-IEU': nbi_data}
spread_current = calculate_spread_ohlc('%PRL F!-IEU', '%NBI F!-IEU', comp_dict)

print("\n" + "=" * 80)
print("CURRENT LOGIC (Widest/Narrowest):")
print("=" * 80)
if spread_current is not None and len(spread_current) > 0:
    row = spread_current.iloc[0]
    print(f"Open:  {row['open']:.5f}  (0.67 - 0.82)")
    print(f"High:  {row['high']:.5f}  (0.7 - 0.82 = High1 - Low2)")
    print(f"Low:   {row['low']:.5f}  (0.66 - 0.9 = Low1 - High2)")
    print(f"Close: {row['close']:.5f}  (0.67 - 0.85)")
    print(f"\nValid? High ({row['high']:.5f}) >= Low ({row['low']:.5f}): {row['high'] >= row['low']}")

# Test simple logic
print("\n" + "=" * 80)
print("SIMPLE LOGIC (High-High, Low-Low):")
print("=" * 80)
simple_high = 0.7 - 0.9
simple_low = 0.66 - 0.82
print(f"Open:  {0.67 - 0.82:.5f}  (0.67 - 0.82)")
print(f"High:  {simple_high:.5f}  (0.7 - 0.9 = High1 - High2)")
print(f"Low:   {simple_low:.5f}  (0.66 - 0.82 = Low1 - Low2)")
print(f"Close: {0.67 - 0.85:.5f}  (0.67 - 0.85)")
print(f"\nValid? High ({simple_high:.5f}) >= Low ({simple_low:.5f}): {simple_high >= simple_low}")

# Check actual output file
print("\n" + "=" * 80)
print("CHECKING ACTUAL OUTPUT FILE")
print("=" * 80)
try:
    df = pd.read_csv('full_unfiltered_historicals/unfiltered_2025-12-05.csv')
    spreads = df[df['is_outright'] == False]
    invalid = spreads[spreads['high_price'] < spreads['low_price']]
    
    print(f"\nTotal spreads in file: {len(spreads):,}")
    print(f"Invalid spreads (High < Low): {len(invalid):,}")
    
    if len(invalid) > 0:
        print("\nFirst 5 invalid spreads:")
        for idx, row in invalid.head(5).iterrows():
            print(f"  {row['spread_name']}: High={row['high_price']:.5f}, Low={row['low_price']:.5f}")
    else:
        print("\n✓ All spreads have valid High >= Low relationship!")
        
except Exception as e:
    print(f"Error reading file: {e}")




