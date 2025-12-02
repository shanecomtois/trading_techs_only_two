"""
Create symbol_matrix.csv with outrights and spreads - Enhanced with expanded metadata
- 176 outright symbols (original) with populated component_months
- 15,400 spread symbols (formulas) with full symbol_1 and symbol_2 metadata
- Total: 15,576 rows
"""
import pandas as pd
import itertools
import re

print("=" * 80)
print("CREATING ENHANCED SYMBOL MATRIX (OUTRIGHTS + SPREADS)")
print("=" * 80)

# Month code to name mapping
MONTH_MAP = {
    'F': 'JAN', 'G': 'FEB', 'H': 'MAR', 'J': 'APR', 
    'K': 'MAY', 'M': 'JUN', 'N': 'JUL', 'Q': 'AUG',
    'U': 'SEP', 'V': 'OCT', 'X': 'NOV', 'Z': 'DEC'
}

# Step 1: Load input data
print("\n[Step 1] Loading symbol_list_all.csv...")
df = pd.read_csv('symbol_list_all.csv', keep_default_na=False)

print(f"   ✓ Loaded {len(df)} symbols")
print(f"   ✓ Found {df['ice_symbol'].nunique()} unique symbols")

# Step 2: Create symbol lookup dictionary
print("\n[Step 2] Creating symbol lookup dictionary...")
symbol_lookup = {}
for _, row in df.iterrows():
    symbol = row['ice_symbol']
    symbol_lookup[symbol] = {
        'symbol_root': row['symbol_root'],
        'product': row['product'],
        'location': row['location'],
        'molecule': row['molecule'],
        'native_uom': row['native_uom'],
        'convert_to_$usg': row['convert_to_$usg'],
        'quarter_numb': row['quarter_numb'],
        'quarter_pos': row['quarter_pos'],
        'component_months': row['component_months'],
        'component_symbols': row['component_symbols']
    }

print(f"   ✓ Created lookup for {len(symbol_lookup)} symbols")

# Step 3: Month extraction and conversion functions
print("\n[Step 3] Creating month extraction functions...")

def extract_month_code_from_symbol(symbol):
    """
    Extract month code from a symbol (e.g., '%PRL F!-IEU' -> 'F')
    Returns single month code or None
    """
    if symbol.startswith('='):
        # Formula - don't extract for non-quarterly
        return None
    
    # Pattern: %SYMBOL_ROOT MONTH_CODE!
    match = re.search(r'%[A-Z]+ ([FGHJKMNQUVXZ])!', symbol)
    return match.group(1) if match else None

def convert_month_codes_to_names(month_codes_str):
    """
    Convert month codes to names (e.g., 'F' -> 'JAN', 'F,G,H' -> 'JAN,FEB,MAR')
    """
    if not month_codes_str or month_codes_str == 'n/a' or month_codes_str == '':
        return ''
    
    codes = [c.strip() for c in month_codes_str.split(',')]
    names = [MONTH_MAP.get(c, c) for c in codes]
    return ','.join(names)

# Test month extraction
print("   Testing month extraction:")
test_symbols = ['%PRL F!-IEU', '%CL G!', '%AFE H!-IEU']
for s in test_symbols:
    code = extract_month_code_from_symbol(s)
    print(f"      {s} → {code}")

# Step 4: Populate component_months for non-quarterly symbols
print("\n[Step 4] Populating component_months for non-quarterly symbols...")
df_enhanced = df.copy()

for idx, row in df_enhanced.iterrows():
    if row['quarter_numb'] == 'N' and (row['component_months'] == 'n/a' or row['component_months'] == ''):
        month_code = extract_month_code_from_symbol(row['ice_symbol'])
        if month_code:
            df_enhanced.at[idx, 'component_months'] = month_code

# Update lookup with enhanced data
for idx, row in df_enhanced.iterrows():
    symbol = row['ice_symbol']
    symbol_lookup[symbol]['component_months'] = row['component_months']

populated_count = len(df_enhanced[(df_enhanced['quarter_numb'] == 'N') & 
                                  (df_enhanced['component_months'] != 'n/a') & 
                                  (df_enhanced['component_months'] != '')])
print(f"   ✓ Populated component_months for {populated_count} non-quarterly symbols")

# Step 5: Formula generation function
print("\n[Step 5] Testing formula generation...")

def generate_spread_formula(symbol_1, symbol_2, lookup):
    """
    Generate spread formula: symbol_1_converted - symbol_2_converted
    """
    meta_1 = lookup[symbol_1]
    meta_2 = lookup[symbol_2]
    
    # Build symbol_1 part
    symbol_1_part = f"('{symbol_1}')"
    if meta_1['convert_to_$usg'] != 'n/a' and meta_1['convert_to_$usg'] != '':
        conversion = meta_1['convert_to_$usg']
        symbol_1_part = f"{symbol_1_part}{conversion}"
    
    # Build symbol_2 part
    symbol_2_part = f"('{symbol_2}')"
    if meta_2['convert_to_$usg'] != 'n/a' and meta_2['convert_to_$usg'] != '':
        conversion = meta_2['convert_to_$usg']
        symbol_2_part = f"{symbol_2_part}{conversion}"
    
    # Combine: symbol_1 - symbol_2
    formula = f"={symbol_1_part}-{symbol_2_part}"
    return formula

# Test formula generation
test_cases = [
    ('%AFE F!-IEU', '%IBC F!-IEU'),
    ('%PRL F!-IEU', '%PRN F!-IEU'),
    ('%CL F!', '%HO F!'),
]
print("   Testing formula generation:")
for sym1, sym2 in test_cases:
    formula = generate_spread_formula(sym1, sym2, symbol_lookup)
    print(f"      {sym1} - {sym2}")
    print(f"      → {formula}")

# Step 6: Create outrights section
print("\n[Step 6] Creating outrights section...")
outrights_rows = []

for _, row in df_enhanced.iterrows():
    component_months = row['component_months']
    component_months_names = convert_month_codes_to_names(component_months)
    
    outright_row = {
        'ice_symbol': row['ice_symbol'],
        'symbol_root': row['symbol_root'],
        'product': row['product'],
        'location': row['location'],
        'molecule': row['molecule'],
        'native_uom': row['native_uom'],
        'convert_to_$usg': row['convert_to_$usg'],
        'quarter_numb': row['quarter_numb'],
        'quarter_pos': row['quarter_pos'],
        'component_months': component_months,
        'component_months_names': component_months_names,
        'component_symbols': row['component_symbols'],
        'symbol_1': '',  # Empty for outrights
        'symbol_2': '',  # Empty for outrights
        'spread_type': 'outright'
    }
    outrights_rows.append(outright_row)

outrights_df = pd.DataFrame(outrights_rows)
print(f"   ✓ Created {len(outrights_df)} outright symbols")

# Step 7: Create spreads section
print("\n[Step 7] Creating spreads section...")
symbols = sorted(df_enhanced['ice_symbol'].tolist())
pairs = list(itertools.combinations(symbols, 2))

print(f"   Generating {len(pairs)} spread formulas...")

spreads_rows = []
for symbol_1, symbol_2 in pairs:
    # Generate formula
    formula = generate_spread_formula(symbol_1, symbol_2, symbol_lookup)
    
    # Get metadata for both symbols
    meta_1 = symbol_lookup[symbol_1]
    meta_2 = symbol_lookup[symbol_2]
    
    # Convert month codes to names
    component_months_1 = meta_1['component_months']
    component_months_2 = meta_2['component_months']
    component_months_names_1 = convert_month_codes_to_names(component_months_1)
    component_months_names_2 = convert_month_codes_to_names(component_months_2)
    
    spread_row = {
        # Primary metadata (from symbol_1)
        'ice_symbol': formula,
        'symbol_root': meta_1['symbol_root'],
        'product': meta_1['product'],
        'location': meta_1['location'],
        'molecule': meta_1['molecule'],
        'native_uom': meta_1['native_uom'],
        'convert_to_$usg': meta_1['convert_to_$usg'],
        'quarter_numb': meta_1['quarter_numb'],
        'quarter_pos': meta_1['quarter_pos'],
        'component_months': component_months_1,
        'component_months_names': component_months_names_1,
        'component_symbols': meta_1['component_symbols'],
        
        # Component symbols
        'symbol_1': symbol_1,
        'symbol_2': symbol_2,
        
        # Symbol_2 metadata (expanded)
        'symbol_root_2': meta_2['symbol_root'],
        'product_2': meta_2['product'],
        'location_2': meta_2['location'],
        'molecule_2': meta_2['molecule'],
        'native_uom_2': meta_2['native_uom'],
        'convert_to_$usg_2': meta_2['convert_to_$usg'],
        'quarter_numb_2': meta_2['quarter_numb'],
        'quarter_pos_2': meta_2['quarter_pos'],
        'component_months_2': component_months_2,
        'component_months_names_2': component_months_names_2,
        'component_symbols_2': meta_2['component_symbols'],
        
        'spread_type': 'spread'
    }
    spreads_rows.append(spread_row)

spreads_df = pd.DataFrame(spreads_rows)
print(f"   ✓ Created {len(spreads_df)} spread symbols")

# Step 8: Combine outrights and spreads
print("\n[Step 8] Combining outrights and spreads...")

# Add empty symbol_2 columns to outrights to match spreads structure
for col in ['symbol_root_2', 'product_2', 'location_2', 'molecule_2', 
            'native_uom_2', 'convert_to_$usg_2', 'quarter_numb_2', 
            'quarter_pos_2', 'component_months_2', 'component_months_names_2', 
            'component_symbols_2']:
    if col not in outrights_df.columns:
        outrights_df[col] = ''

# Ensure column order matches
column_order = [
    'ice_symbol', 'symbol_root', 'product', 'location', 'molecule',
    'native_uom', 'convert_to_$usg', 'quarter_numb', 'quarter_pos',
    'component_months', 'component_months_names', 'component_symbols',
    'symbol_1', 'symbol_2',
    'symbol_root_2', 'product_2', 'location_2', 'molecule_2',
    'native_uom_2', 'convert_to_$usg_2', 'quarter_numb_2', 'quarter_pos_2',
    'component_months_2', 'component_months_names_2', 'component_symbols_2',
    'spread_type'
]

outrights_df = outrights_df[column_order]
spreads_df = spreads_df[column_order]

combined_df = pd.concat([outrights_df, spreads_df], ignore_index=True)

print(f"   ✓ Total rows: {len(combined_df)}")
print(f"   ✓ Expected: 15,576 (176 outrights + 15,400 spreads)")
if len(combined_df) == 15576:
    print(f"   ✓ Correct total!")
else:
    print(f"   ⚠️  WARNING: Expected 15,576, got {len(combined_df)}")

# Step 9: Validation
print("\n[Step 9] Validating output...")

# Check counts
outright_count = len(combined_df[combined_df['spread_type'] == 'outright'])
spread_count = len(combined_df[combined_df['spread_type'] == 'spread'])

print(f"   Outrights: {outright_count} (expected 176)")
print(f"   Spreads: {spread_count} (expected 15,400)")

# Check component_months population
outrights_with_months = len(combined_df[(combined_df['spread_type'] == 'outright') & 
                                        (combined_df['component_months'] != 'n/a') & 
                                        (combined_df['component_months'] != '')])
print(f"   Outrights with component_months populated: {outrights_with_months}")

# Check for duplicates
duplicate_symbols = combined_df['ice_symbol'].duplicated().sum()
if duplicate_symbols == 0:
    print(f"   ✓ No duplicate ice_symbol values")
else:
    print(f"   ⚠️  WARNING: Found {duplicate_symbols} duplicate ice_symbol values")

# Check formula format
spread_formulas = combined_df[combined_df['spread_type'] == 'spread']['ice_symbol']
formulas_start_with_equals = spread_formulas.str.startswith('=').sum()
print(f"   Spread formulas starting with '=': {formulas_start_with_equals}/{len(spread_formulas)}")

# Check month names conversion
sample_outright = combined_df[combined_df['spread_type'] == 'outright'].iloc[0]
if sample_outright['component_months'] != 'n/a':
    print(f"   Sample month conversion: {sample_outright['component_months']} → {sample_outright['component_months_names']}")

# Sample output
print("\n[Step 10] Sample output:")
print("\n   Sample outright:")
print(outrights_df.head(1)[['ice_symbol', 'symbol_root', 'product', 'component_months', 'component_months_names', 'spread_type']].to_string())

print("\n   Sample spread:")
sample_spread = spreads_df.head(1)
print(sample_spread[['ice_symbol', 'symbol_root', 'product', 'component_months', 
                     'symbol_1', 'symbol_2', 'product_2', 'component_months_2']].to_string())

# Step 11: Save to CSV
print("\n[Step 11] Saving to symbol_matrix.csv...")
combined_df.to_csv('symbol_matrix.csv', index=False)
print(f"   ✓ Saved symbol_matrix.csv")

print("\n" + "=" * 80)
print("ENHANCED SYMBOL MATRIX CREATION COMPLETE")
print("=" * 80)
print(f"\nOutput file: symbol_matrix.csv")
print(f"Total rows: {len(combined_df):,}")
print(f"  - Outrights: {outright_count:,}")
print(f"  - Spreads: {spread_count:,}")
print(f"Total columns: {len(combined_df.columns)}")
print(f"\nNew columns added:")
print(f"  - component_months_names (month code to name conversion)")
print(f"  - component_months_2, component_months_names_2 (for spreads)")
print(f"  - All symbol_2 metadata columns (product_2, location_2, etc.)")
