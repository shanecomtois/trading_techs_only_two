import pandas as pd
import sys

# Force UTF-8 output
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

print("=" * 80)
print("ADDING QUARTERLY SYMBOLS")
print("=" * 80)

# Load existing
df = pd.read_csv('lists_and_matrix/symbol_list_all.csv', keep_default_na=False)
print(f"\nLoaded {len(df)} existing symbols")

# Quarters
quarters = {'1Q': ['F','G','H'], '2Q': ['J','K','M'], '3Q': ['N','Q','U'], '4Q': ['V','X','Z']}

# Get roots
roots = sorted(df['symbol_root'].unique())
print(f"Found {len(roots)} symbol roots")

# Generate quarterlies
quarterly_rows = []
for root in roots:
    sample = df[df['symbol_root'] == root].iloc[0]
    suffix = '-IEU' if '-IEU' in sample['ice_symbol'] else ''
    
    for q, months in quarters.items():
        comp_syms = [f"%{root} {m}!{suffix}" for m in months]
        formula = f"=((('{comp_syms[0]}')+('{comp_syms[1]}')+('{comp_syms[2]}'))/3)"
        
        quarterly_rows.append({
            'ice_symbol': formula,
            'symbol_root': root,
            'product': sample['product'],
            'location': sample['location'],
            'molecule': sample['molecule'],
            'native_uom': sample['native_uom'],
            'convert_to_$usg': sample['convert_to_$usg'],
            'quarter_numb': 'Y',
            'quarter_pos': q,
            'component_months': ','.join(months),
            'component_symbols': ','.join(comp_syms)
        })
    print(f"  Generated {root}: 1Q, 2Q, 3Q, 4Q")

# Combine
print(f"\nAdding {len(quarterly_rows)} quarterly symbols...")
new_df = pd.concat([df, pd.DataFrame(quarterly_rows)], ignore_index=True)
new_df.to_csv('lists_and_matrix/symbol_list_all.csv', index=False)
print(f"✓ Saved. Total symbols: {len(new_df)} (original: {len(df)}, quarterlies: {len(quarterly_rows)})")

# Verify
quarterly_count = len(new_df[new_df['quarter_numb'] == 'Y'])
print(f"✓ Quarterlies in file: {quarterly_count}")
print(f"✓ Expected: {len(roots) * 4}")

# Show samples
print("\nSample quarterlies:")
for i, row in pd.DataFrame(quarterly_rows).head(4).iterrows():
    print(f"  {row['symbol_root']} {row['quarter_pos']}: {row['ice_symbol']}")

print("\n" + "=" * 80)
print("COMPLETE")
print("=" * 80)




