"""
Add quarterly symbols to symbol_list_all.csv
Creates 1Q, 2Q, 3Q, 4Q for each base symbol (symbol_root)
"""
import pandas as pd
from pathlib import Path

print("=" * 80)
print("ADDING QUARTERLY SYMBOLS TO symbol_list_all.csv")
print("=" * 80)

# Quarter mapping: quarter -> list of month codes
QUARTER_MONTHS = {
    '1Q': ['F', 'G', 'H'],  # Jan, Feb, Mar
    '2Q': ['J', 'K', 'M'],  # Apr, May, Jun
    '3Q': ['N', 'Q', 'U'],  # Jul, Aug, Sep
    '4Q': ['V', 'X', 'Z']   # Oct, Nov, Dec
}

# Load existing symbols
print("\n[Step 1] Loading symbol_list_all.csv...")
df = pd.read_csv('lists_and_matrix/symbol_list_all.csv', keep_default_na=False)
print(f"   ✓ Loaded {len(df)} existing symbols")

# Get unique symbol roots (base symbols)
symbol_roots = sorted(df['symbol_root'].unique())
print(f"   ✓ Found {len(symbol_roots)} unique symbol roots: {symbol_roots}")

# Generate quarterly symbols
print("\n[Step 2] Generating quarterly symbols...")
quarterly_rows = []

for symbol_root in symbol_roots:
    # Get a sample symbol for this root to determine format (with or without -IEU)
    sample_symbols = df[df['symbol_root'] == symbol_root]['ice_symbol'].tolist()
    if not sample_symbols:
        continue
    
    sample = sample_symbols[0]  # e.g., '%PRL F!-IEU' or '%CL F!'
    
    # Determine suffix (if any)
    if '-IEU' in sample:
        suffix = '-IEU'
    else:
        suffix = ''
    
    # Get metadata from first symbol of this root
    sample_row = df[df['symbol_root'] == symbol_root].iloc[0]
    
    # Generate 4 quarterly formulas
    for quarter, months in QUARTER_MONTHS.items():
        # Build component symbols list (e.g., '%PRL F!-IEU', '%PRL G!-IEU', '%PRL H!-IEU')
        component_symbols_list = [f"%{symbol_root} {month}!{suffix}" for month in months]
        
        # Generate formula: =(('%PRL F!-IEU')+('%PRL G!-IEU')+('%PRL H!-IEU'))/3
        formula = f"=((('{component_symbols_list[0]}')+('{component_symbols_list[1]}')+('{component_symbols_list[2]}'))/3)"
        
        # Component symbols as comma-separated: 'F,G,H'
        component_symbols_str = ','.join(months)
        
        # Create quarterly row
        quarterly_row = {
            'ice_symbol': formula,
            'symbol_root': symbol_root,
            'product': sample_row['product'],
            'location': sample_row['location'],
            'molecule': sample_row['molecule'],
            'native_uom': sample_row['native_uom'],
            'convert_to_$usg': sample_row['convert_to_$usg'],
            'quarter_numb': 'Y',
            'quarter_pos': quarter,
            'component_months': component_symbols_str,  # 'F,G,H'
            'component_symbols': ','.join(component_symbols_list)  # Full symbols comma-separated
        }
        
        quarterly_rows.append(quarterly_row)
        print(f"   ✓ {symbol_root} {quarter}: {formula}")

print(f"\n   ✓ Generated {len(quarterly_rows)} quarterly symbols")

# Add quarterlies to dataframe
print("\n[Step 3] Adding quarterlies to symbol list...")
quarterly_df = pd.DataFrame(quarterly_rows)
combined_df = pd.concat([df, quarterly_df], ignore_index=True)

print(f"   ✓ Total symbols: {len(combined_df)} (original: {len(df)}, quarterlies: {len(quarterly_df)})")

# Save to CSV
print("\n[Step 4] Saving updated symbol_list_all.csv...")
output_file = Path('lists_and_matrix/symbol_list_all.csv')
combined_df.to_csv(output_file, index=False)
print(f"   ✓ Saved to {output_file}")

# Validation
print("\n[Step 5] Validation...")
quarterly_count = len(combined_df[combined_df['quarter_numb'] == 'Y'])
print(f"   ✓ Quarterlies in file: {quarterly_count}")
print(f"   ✓ Expected: {len(symbol_roots) * 4} (should match)")

# Show sample
print("\n[Step 6] Sample quarterly symbols:")
sample_quarterlies = combined_df[combined_df['quarter_numb'] == 'Y'].head(4)
for _, row in sample_quarterlies.iterrows():
    print(f"   {row['symbol_root']} {row['quarter_pos']}: {row['ice_symbol']}")
    print(f"      Component symbols: {row['component_symbols']}")

print("\n" + "=" * 80)
print("QUARTERLY SYMBOLS ADDED SUCCESSFULLY")
print("=" * 80)




