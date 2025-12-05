"""
Create symbol_matrix.csv with outrights and spreads - Enhanced with expanded metadata
- Outright symbols (monthly + quarterly) with populated component_months
- Spread symbols (formulas) with full symbol_1 and symbol_2 metadata
- Quarterlies (quarter_numb == 'Y') are included as outrights (with conversions applied)
- Spreads include: Month v Month, Qtr v Qtr, Month v Qtr (excluding months that are part of the quarter)
"""
import pandas as pd
import itertools
import re
import sys
import traceback
import csv

# Force UTF-8 output
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

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

# Manual CSV parsing to handle malformed rows (quarterlies with unquoted commas in component_symbols)
rows = []
expected_columns = 11  # Based on the CSV structure

try:
    with open('lists_and_matrix/symbol_list_all.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)  # Read header
        
        for row_num, row in enumerate(reader, start=2):  # Start at 2 (after header)
            if len(row) == expected_columns:
                # Normal row - use as is
                rows.append(row)
            elif len(row) > expected_columns:
                # Malformed row (likely quarterly with unquoted commas in component_symbols)
                # Combine extra fields back into component_symbols (last column)
                fixed_row = row[:expected_columns-1]  # All columns except last
                # Combine remaining fields into component_symbols
                component_symbols = ','.join(row[expected_columns-1:])
                fixed_row.append(component_symbols)
                rows.append(fixed_row)
            else:
                # Row with too few fields - skip or pad
                print(f"   ⚠ Warning: Row {row_num} has {len(row)} fields (expected {expected_columns}), skipping")
                continue
    
    # Create DataFrame from manually parsed rows
    df = pd.DataFrame(rows, columns=header)
    df = df.replace('', 'n/a')  # Replace empty strings with 'n/a' for consistency
    
except Exception as e:
    print(f"   ✗ Error loading CSV: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

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
    
    Handles both regular symbols (e.g., '%CL V!') and quarterly formulas (e.g., '=((('%NBI V!-IEU')...')
    
    For quarterly formulas: strip the leading '=' and use the formula part directly (no quotes)
    For regular symbols: wrap in quotes and apply conversion if needed
    """
    meta_1 = lookup[symbol_1]
    meta_2 = lookup[symbol_2]
    
    # Build symbol_1 part
    if symbol_1.startswith('='):
        # Quarterly formula - remove the leading '=' and append conversion if needed
        symbol_1_part = symbol_1[1:]
        # Check if conversion is already in the formula (ends with /521 or similar)
        if meta_1['convert_to_$usg'] != 'n/a' and meta_1['convert_to_$usg'] != '':
            # Check if conversion is already appended
            if not symbol_1_part.endswith(meta_1['convert_to_$usg']):
                symbol_1_part = f"{symbol_1_part}{meta_1['convert_to_$usg']}"
    else:
        # Regular symbol - wrap in quotes
        symbol_1_part = f"('{symbol_1}')"
        if meta_1['convert_to_$usg'] != 'n/a' and meta_1['convert_to_$usg'] != '':
            conversion = meta_1['convert_to_$usg']
            symbol_1_part = f"{symbol_1_part}{conversion}"
    
    # Build symbol_2 part
    if symbol_2.startswith('='):
        # Quarterly formula - remove the leading '=' and append conversion if needed
        symbol_2_part = symbol_2[1:]
        # Check if conversion is already in the formula (ends with /521 or similar)
        if meta_2['convert_to_$usg'] != 'n/a' and meta_2['convert_to_$usg'] != '':
            # Check if conversion is already appended
            if not symbol_2_part.endswith(meta_2['convert_to_$usg']):
                symbol_2_part = f"{symbol_2_part}{meta_2['convert_to_$usg']}"
    else:
        # Regular symbol - wrap in quotes
        symbol_2_part = f"('{symbol_2}')"
        if meta_2['convert_to_$usg'] != 'n/a' and meta_2['convert_to_$usg'] != '':
            conversion = meta_2['convert_to_$usg']
            symbol_2_part = f"{symbol_2_part}{conversion}"
    
    # Combine: symbol_1 - symbol_2 (always add '=' at the beginning for the spread formula)
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

# Step 6: Create outrights section (INCLUDE QUARTERLIES)
print("\n[Step 6] Creating outrights section (including quarterlies)...")
outrights_rows = []

# Include all symbols (both monthly and quarterly)
for _, row in df_enhanced.iterrows():
    component_months = row['component_months']
    component_months_names = convert_month_codes_to_names(component_months)
    
    # For quarterlies, apply conversion factor to the formula if it exists
    ice_symbol = row['ice_symbol']
    if row['quarter_numb'] == 'Y':
        # Quarterly formula - apply conversion if needed
        conversion = row['convert_to_$usg']
        if conversion != 'n/a' and conversion != '':
            # Formula is like =((('%AFE F!-IEU')+('%AFE G!-IEU')+('%AFE H!-IEU'))/3)
            # Need to append conversion: =((('%AFE F!-IEU')+('%AFE G!-IEU')+('%AFE H!-IEU'))/3)/521
            ice_symbol = f"{ice_symbol}{conversion}"
    
    outright_row = {
        'ice_symbol': ice_symbol,
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
monthly_count = len(outrights_df[outrights_df['quarter_numb'] == 'N'])
quarterly_count = len(outrights_df[outrights_df['quarter_numb'] == 'Y'])
print(f"   ✓ Created {len(outrights_df)} outright symbols (monthly: {monthly_count}, quarterly: {quarterly_count})")

# Step 7: Create spreads section (INCLUDE QUARTERLIES with exclusion rule)
print("\n[Step 7] Creating spreads section (including quarterlies with Month v Qtr exclusion rule)...")

def should_exclude_month_vs_quarter(symbol_1, symbol_2, lookup):
    """
    Check if a Month v Qtr spread should be excluded.
    Rule: If monthly contract's month is part of the quarterly's component months, exclude it.
    
    Returns True if spread should be excluded, False otherwise.
    """
    meta_1 = lookup[symbol_1]
    meta_2 = lookup[symbol_2]
    
    # Check if symbol_1 is monthly and symbol_2 is quarterly
    if meta_1['quarter_numb'] == 'N' and meta_2['quarter_numb'] == 'Y':
        month_code = extract_month_code_from_symbol(symbol_1)
        if month_code:
            # Try component_months first, fallback to component_symbols if needed
            quarter_months = []
            if meta_2['component_months'] and meta_2['component_months'] != 'n/a' and meta_2['component_months'] != '':
                quarter_months = [m.strip() for m in str(meta_2['component_months']).split(',')]
            # If component_months parsing failed (only got 1 month), try extracting from component_symbols
            if len(quarter_months) < 3 and meta_2['component_symbols'] and meta_2['component_symbols'] != 'n/a' and meta_2['component_symbols'] != '':
                # Extract month codes from component_symbols (e.g., '%PRN J!-IEU,%PRN K!-IEU,%PRN M!-IEU')
                comp_symbols_str = str(meta_2['component_symbols']).strip().strip('"').strip("'")
                comp_symbols = [s.strip() for s in comp_symbols_str.split(',') if s.strip().startswith('%')]
                for comp_sym in comp_symbols:
                    comp_month = extract_month_code_from_symbol(comp_sym)
                    if comp_month and comp_month not in quarter_months:
                        quarter_months.append(comp_month)
            if month_code in quarter_months:
                return True
    
    # Check if symbol_1 is quarterly and symbol_2 is monthly
    if meta_1['quarter_numb'] == 'Y' and meta_2['quarter_numb'] == 'N':
        month_code = extract_month_code_from_symbol(symbol_2)
        if month_code:
            # Try component_months first, fallback to component_symbols if needed
            quarter_months = []
            if meta_1['component_months'] and meta_1['component_months'] != 'n/a' and meta_1['component_months'] != '':
                quarter_months = [m.strip() for m in str(meta_1['component_months']).split(',')]
            # If component_months parsing failed (only got 1 month), try extracting from component_symbols
            if len(quarter_months) < 3 and meta_1['component_symbols'] and meta_1['component_symbols'] != 'n/a' and meta_1['component_symbols'] != '':
                # Extract month codes from component_symbols
                comp_symbols_str = str(meta_1['component_symbols']).strip().strip('"').strip("'")
                comp_symbols = [s.strip() for s in comp_symbols_str.split(',') if s.strip().startswith('%')]
                for comp_sym in comp_symbols:
                    comp_month = extract_month_code_from_symbol(comp_sym)
                    if comp_month and comp_month not in quarter_months:
                        quarter_months.append(comp_month)
            if month_code in quarter_months:
                return True
    
    return False

# Include all symbols (monthly + quarterly) for spread generation
symbols = sorted(df_enhanced['ice_symbol'].tolist())
pairs = list(itertools.combinations(symbols, 2))

print(f"   Total possible pairs: {len(pairs):,}")
print(f"   Filtering Month v Qtr spreads where month is part of quarter...")

spreads_rows = []
excluded_count = 0
missing_count = 0
for idx, (symbol_1, symbol_2) in enumerate(pairs):
    # Progress indicator every 1000 pairs
    if (idx + 1) % 1000 == 0:
        print(f"   Processing pair {idx + 1:,}/{len(pairs):,}...")
    
    # Check if symbols exist in lookup
    if symbol_1 not in symbol_lookup or symbol_2 not in symbol_lookup:
        missing_count += 1
        continue
    
    # Check if this Month v Qtr spread should be excluded
    if should_exclude_month_vs_quarter(symbol_1, symbol_2, symbol_lookup):
        excluded_count += 1
        continue
    
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

print(f"   ✓ Excluded {excluded_count:,} Month v Qtr spreads (month is component of quarter)")
if missing_count > 0:
    print(f"   ⚠️  Warning: {missing_count:,} pairs skipped (symbols not in lookup)")
print(f"   ✓ Generated {len(spreads_rows):,} spread formulas")

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

print(f"   ✓ Total rows: {len(combined_df):,}")
monthly_outrights = len(outrights_df[outrights_df['quarter_numb'] == 'N'])
quarterly_outrights = len(outrights_df[outrights_df['quarter_numb'] == 'Y'])
print(f"   ✓ Outrights: {len(outrights_df):,} (monthly: {monthly_outrights}, quarterly: {quarterly_outrights})")
print(f"   ✓ Spreads: {len(spreads_df):,}")

# Step 9: Validation
print("\n[Step 9] Validating output...")

# Check counts
outright_count = len(combined_df[combined_df['spread_type'] == 'outright'])
spread_count = len(combined_df[combined_df['spread_type'] == 'spread'])

monthly_outright_count = len(combined_df[(combined_df['spread_type'] == 'outright') & (combined_df['quarter_numb'] == 'N')])
quarterly_outright_count = len(combined_df[(combined_df['spread_type'] == 'outright') & (combined_df['quarter_numb'] == 'Y')])
print(f"   Outrights: {outright_count:,} (monthly: {monthly_outright_count}, quarterly: {quarterly_outright_count})")
print(f"   Spreads: {spread_count}")

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
print("\n[Step 11] Saving to symbol_matrix_with_quarterlies.csv...")
try:
    output_path = 'lists_and_matrix/symbol_matrix_with_quarterlies.csv'
    # Use quoting=csv.QUOTE_MINIMAL to ensure fields with commas are properly quoted
    # This prevents parsing issues when reading the CSV back
    combined_df.to_csv(output_path, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"   ✓ Saved {output_path}")
    print(f"   Please review the file. If correct, rename it to replace symbol_matrix.csv")
except Exception as e:
    print(f"   ✗ Error saving file: {e}")
    import traceback
    traceback.print_exc()
    raise

print("\n" + "=" * 80)
print("ENHANCED SYMBOL MATRIX CREATION COMPLETE")
print("=" * 80)
print(f"\nOutput file: lists_and_matrix/symbol_matrix_with_quarterlies.csv")
print(f"Total rows: {len(combined_df):,}")
print(f"  - Outrights: {outright_count:,} (monthly: {monthly_outright_count}, quarterly: {quarterly_outright_count})")
print(f"  - Spreads: {spread_count:,}")
print(f"Total columns: {len(combined_df.columns)}")
print(f"\nNote: Quarterlies (quarter_numb == 'Y') are now included in the matrix")
print(f"      Month v Qtr spreads exclude months that are part of the quarter")
