"""
Diagnostic script to check which symbols have 12/5/2025 data available
"""
import icepython as ice
import pandas as pd
from datetime import datetime
import sys
from pathlib import Path

# Adjust path for running from aux_scripts folder
script_dir = Path(__file__).parent
project_root = script_dir.parent

# Load symbols
print("Loading symbols from symbol_matrix.csv...")
df_symbols = pd.read_csv(project_root / 'lists_and_matrix' / 'symbol_matrix.csv', keep_default_na=False)

# Filter to outrights only (N only, no quarterlies)
outrights = df_symbols[(df_symbols['spread_type'] == 'outright') & (df_symbols['quarter_numb'] == 'N')]
outright_symbols = outrights['ice_symbol'].tolist()

print(f"Checking {len(outright_symbols)} outright symbols for 2025-12-05 data...")
print("=" * 80)

target_date = datetime(2025, 12, 5)
start_date = target_date - pd.Timedelta(weeks=2)
end_date = target_date

start_str = start_date.strftime('%Y-%m-%d')
end_str = end_date.strftime('%Y-%m-%d')

print(f"Date range: {start_str} to {end_str}")
print(f"Looking for data date: {target_date.strftime('%Y-%m-%d')}")
print("=" * 80)
print()

has_date = []
missing_date = []
errors = []

for i, symbol in enumerate(outright_symbols, 1):
    try:
        result = ice.get_timeseries(
            [symbol],
            ['Close'],
            'W',
            start_str,
            end_str
        )
        
        if result is None or len(result) == 0:
            missing_date.append((symbol, "No data returned"))
            continue
        
        # Find max date in result
        dates = []
        for row in result:
            if row and len(row) > 0:
                date_val = row[0]
                if date_val:
                    try:
                        date_obj = pd.to_datetime(date_val)
                        dates.append(date_obj)
                    except:
                        pass
        
        if dates:
            max_date = max(dates)
            max_date_date = max_date.date() if hasattr(max_date, 'date') else pd.to_datetime(max_date).date()
            
            if max_date_date == target_date.date():
                has_date.append(symbol)
            else:
                missing_date.append((symbol, f"Latest date: {max_date_date}"))
        else:
            missing_date.append((symbol, "Could not parse dates"))
            
    except Exception as e:
        errors.append((symbol, str(e)))
    
    if i % 10 == 0:
        print(f"  Checked {i}/{len(outright_symbols)} symbols...", end='\r')

print()
print("=" * 80)
print("RESULTS")
print("=" * 80)
print(f"\nSymbols WITH 2025-12-05 data: {len(has_date)}/{len(outright_symbols)}")
print(f"Symbols MISSING 2025-12-05 data: {len(missing_date)}/{len(outright_symbols)}")
print(f"Errors: {len(errors)}/{len(outright_symbols)}")

if missing_date:
    print(f"\n{'='*80}")
    print("SYMBOLS MISSING 2025-12-05 DATA:")
    print("=" * 80)
    for symbol, reason in missing_date[:50]:  # Show first 50
        print(f"  {symbol}: {reason}")
    if len(missing_date) > 50:
        print(f"\n  ... and {len(missing_date) - 50} more")

if errors:
    print(f"\n{'='*80}")
    print("ERRORS:")
    print("=" * 80)
    for symbol, error in errors[:20]:  # Show first 20
        print(f"  {symbol}: {error}")
    if len(errors) > 20:
        print(f"\n  ... and {len(errors) - 20} more")

print(f"\n{'='*80}")
print("SUMMARY")
print("=" * 80)
if len(has_date) == len(outright_symbols):
    print("✓ ALL symbols have 2025-12-05 data!")
else:
    print(f"⚠️  Only {len(has_date)}/{len(outright_symbols)} symbols have 2025-12-05 data")
    print(f"   Missing: {len(missing_date)} symbols")
    if missing_date:
        # Show what dates they actually have
        date_counts = {}
        for symbol, reason in missing_date:
            if "Latest date:" in reason:
                date_str = reason.split("Latest date: ")[1]
                date_counts[date_str] = date_counts.get(date_str, 0) + 1
        
        if date_counts:
            print(f"\n   Latest dates found in missing symbols:")
            for date_str, count in sorted(date_counts.items(), key=lambda x: x[1], reverse=True):
                print(f"     {date_str}: {count} symbols")





