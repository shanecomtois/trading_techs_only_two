"""
Check ALL symbols for 12/5/2025 data availability
"""
import icepython as ice
import pandas as pd
from datetime import datetime
import sys
from pathlib import Path

# Adjust path for running from aux_scripts folder
script_dir = Path(__file__).parent
project_root = script_dir.parent

# Load all outright symbols from symbol matrix
print("Loading symbols from symbol_matrix.csv...")
df_symbols = pd.read_csv(project_root / 'lists_and_matrix' / 'symbol_matrix.csv', keep_default_na=False)

# Filter to outrights only (N only, no quarterlies)
outrights = df_symbols[(df_symbols['spread_type'] == 'outright') & (df_symbols['quarter_numb'] == 'N')]
test_symbols = outrights['ice_symbol'].tolist()

target_date = datetime(2025, 12, 5)
start_date = target_date - pd.Timedelta(weeks=2)
end_date = target_date

print(f"Checking ALL {len(test_symbols)} outright symbols for {target_date.strftime('%Y-%m-%d')} data...")
print("=" * 80)

has_12_5 = []
missing_12_5 = []

for i, symbol in enumerate(test_symbols, 1):
    if i % 10 == 0:
        print(f"  Progress: {i}/{len(test_symbols)} ({i/len(test_symbols)*100:.1f}%)...", end='\r')
    try:
        result = ice.get_timeseries([symbol], ['Close'], 'W', 
                                   start_date.strftime('%Y-%m-%d'),
                                   end_date.strftime('%Y-%m-%d'))
        
        if result and len(result) > 0:
            dates = []
            for row in result:
                if row and len(row) > 0 and row[0]:
                    try:
                        dates.append(pd.to_datetime(row[0]))
                    except:
                        pass
            
            if dates:
                max_date = max(dates).date()
                if max_date == target_date.date():
                    has_12_5.append(symbol)
                else:
                    missing_12_5.append((symbol, max_date))
            else:
                missing_12_5.append((symbol, "No dates"))
        else:
            missing_12_5.append((symbol, "No data"))
    except Exception as e:
        missing_12_5.append((symbol, f"Error: {e}"))

print()  # New line after progress

print("=" * 80)
print(f"\nSummary:")
print(f"  Have 12/5: {len(has_12_5)}/{len(test_symbols)}")
print(f"  Missing 12/5: {len(missing_12_5)}/{len(test_symbols)}")

if missing_12_5:
    print(f"\n{'='*80}")
    print("SYMBOLS MISSING 2025-12-05 DATA:")
    print("=" * 80)
    for symbol, reason in missing_12_5:
        print(f"  {symbol}: {reason}")
    
    # Group by reason to see patterns
    date_counts = {}
    for symbol, reason in missing_12_5:
        if isinstance(reason, datetime):
            date_str = reason.strftime('%Y-%m-%d')
        elif "Latest date:" in str(reason):
            date_str = str(reason)
        else:
            date_str = str(reason)
        date_counts[date_str] = date_counts.get(date_str, 0) + 1
    
    if date_counts:
        print(f"\n{'='*80}")
        print("BREAKDOWN BY LATEST DATE FOUND:")
        print("=" * 80)
        for date_str, count in sorted(date_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {date_str}: {count} symbols")
else:
    print("\n✓ ALL symbols have 2025-12-05 data!")





