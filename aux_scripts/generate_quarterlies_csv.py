import pandas as pd

# Load existing
df = pd.read_csv('lists_and_matrix/symbol_list_all.csv', keep_default_na=False)

# Quarters
quarters = {'1Q': ['F','G','H'], '2Q': ['J','K','M'], '3Q': ['N','Q','U'], '4Q': ['V','X','Z']}

# Get roots
roots = sorted(df['symbol_root'].unique())

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

# Output as CSV (just the data rows, no header)
quarterly_df = pd.DataFrame(quarterly_rows)
for _, row in quarterly_df.iterrows():
    print(','.join([str(row[col]) for col in quarterly_df.columns]))




