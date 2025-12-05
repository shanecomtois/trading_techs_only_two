# Quarterly Delta Sizing Examples

## Key Rules

1. **Quarterly "per mo" volumes** must be converted to **total volumes** (×3) before delta calculation
2. **AFE quarterlies**: Total must be multiple of 39kb (13kb × 3) → "per mo" must be multiple of 13kb
3. **Other quarterlies**: Total must be multiple of 3kb (1kb × 3) → "per mo" can be any 1kb increment
4. After delta calculation, convert quarterly totals back to "per mo" (÷3) for display

---

## Example 1: CL Quarterly Base + XRB Monthly Other

**Spread:** `=('%XRB X!')-((('%CL J!')+('%CL K!')+('%CL M!'))/3)/42`
- Base: CL Q2 '26 (quarterly) - 10kb per mo
- Other: XRB Nov '26 (monthly)
- Prices: CL = $1.40/gallon, XRB = $1.75/gallon
- Pos%: 11.1%

### Current (WRONG):
```
Base: 10kb per mo (uses 10kb for delta - WRONG, should use 30kb total)
Delta ratio: 1.75 / 1.40 = 1.25 (WRONG - should be base_price / other_price)
Other: 10kb × 1.25 = 12.5kb → 13kb
Display: "10kb per mo CL" / "13kb XRB"
Note: This creates dollar imbalance (RBOB more expensive but more volume)
```

### Correct Calculation:
```
Step 1: Convert quarterly to total
Base: 10kb per mo × 3 = 30kb total

Step 2: Round total to increment (CL quarterly = 3kb increments)
30kb → 30kb (already multiple of 3)

Step 3: Calculate delta using total (CORRECTED FORMULA for dollar balancing)
Delta ratio: 1.40 / 1.75 = 0.80 (base_price / other_price)
Other: 30kb × 0.80 = 24kb
Note: RBOB is more expensive, so we need LESS volume for balanced exposure

Step 4: Round other to increment (XRB = 1kb increments)
24kb → 24kb

Step 5: Convert quarterly back to "per mo"
Base: 30kb total ÷ 3 = 10kb per mo

Dollar exposure check:
- Base: 30kb × $1.40 = $42,000
- Other: 24kb × $1.75 = $42,000
- ✅ Balanced!

Display: "10kb per mo WTI Crude" / "24kb RBOB"
```

---

## Example 2: AFE Quarterly Base + CL Monthly Other

**Spread:** AFE Q2 '26 (quarterly) vs CL Apr '26 (monthly)
- Base: AFE Q2 '26 - 10kb per mo
- Other: CL Apr '26
- Prices: AFE = $0.98/gallon, CL = $1.40/gallon
- Pos%: 100%

### Current (WRONG):
```
Base: 10kb per mo (uses 10kb for delta)
Delta ratio: 1.40 / 0.98 = 1.429
Other: 10kb × 1.429 = 14.29kb → 14kb
Display: "10kb per mo AFE" / "14kb CL"
```

### Correct Calculation:
```
Step 1: Convert quarterly to total
Base: 10kb per mo × 3 = 30kb total

Step 2: Round total to AFE quarterly increment (39kb multiples)
30kb → 39kb (nearest multiple of 39kb)
Note: 39kb ÷ 3 = 13kb per mo (valid AFE increment)

Step 3: Calculate delta using rounded total (CORRECTED FORMULA)
Delta ratio: 0.98 / 1.40 = 0.70 (base_price / other_price)
Other: 39kb × 0.70 = 27.3kb
Note: CL is more expensive, so we need LESS volume for balanced exposure

Step 4: Round other to increment (CL = 1kb increments)
27.3kb → 27kb

Step 5: Convert quarterly back to "per mo"
Base: 39kb total ÷ 3 = 13kb per mo

Dollar exposure check:
- Base: 39kb × $0.98 = $38,220
- Other: 27kb × $1.40 = $37,800
- ✅ Approximately balanced!

Display: "13kb per mo Far East Propane" / "27kb WTI Crude"
```

---

## Example 3: CL Monthly Base + AFE Quarterly Other

**Spread:** CL Apr '26 (monthly) vs AFE Q2 '26 (quarterly)
- Base: CL Apr '26 - 10kb
- Other: AFE Q2 '26
- Prices: CL = $1.40/gallon, AFE = $0.98/gallon
- Pos%: 100%

### Current (WRONG):
```
Base: 10kb
Delta ratio: 0.98 / 1.40 = 0.70
Other: 10kb × 0.70 = 7kb → 7kb per mo (invalid - below 10kb minimum)
Display: "10kb CL" / "7kb per mo AFE" (WRONG - below minimum)
```

### Correct Calculation:
```
Step 1: Base is monthly, no conversion needed
Base: 10kb

Step 2: Calculate delta (CORRECTED FORMULA)
Delta ratio: 1.40 / 0.98 = 1.429 (base_price / other_price)
Other total: 10kb × 1.429 = 14.29kb total
Note: AFE is cheaper, so we need MORE volume for balanced exposure

Step 3: Round other total to AFE quarterly increment (39kb multiples)
14.29kb → 39kb (minimum for AFE quarterly)
Note: 39kb ÷ 3 = 13kb per mo (valid AFE increment)

Step 4: Convert quarterly back to "per mo"
Other: 39kb total ÷ 3 = 13kb per mo

Dollar exposure check:
- Base: 10kb × $1.40 = $14,000
- Other: 39kb × $0.98 = $38,220
- Note: AFE minimum (39kb) creates imbalance, but trading constraint takes precedence

Display: "10kb WTI Crude" / "13kb per mo Far East Propane"
```

---

## Example 4: AFE Quarterly Base + AFE Quarterly Other

**Spread:** AFE Q1 '26 vs AFE Q2 '26 (both quarterly)
- Base: AFE Q1 '26 - 10kb per mo
- Other: AFE Q2 '26
- Prices: AFE Q1 = $1.03/gallon, AFE Q2 = $0.98/gallon
- Pos%: 100%

### Current (WRONG):
```
Base: 10kb per mo (uses 10kb for delta)
Delta ratio: 0.98 / 1.03 = 0.951
Other: 10kb × 0.951 = 9.51kb → 10kb per mo
Display: "10kb per mo AFE" / "10kb per mo AFE"
```

### Correct Calculation:
```
Step 1: Convert both to totals
Base: 10kb per mo × 3 = 30kb total
Other: (will calculate)

Step 2: Round base total to AFE quarterly increment (39kb multiples)
30kb → 39kb (nearest multiple of 39kb)
Base per mo: 39kb ÷ 3 = 13kb per mo

Step 3: Calculate delta using rounded base total (CORRECTED FORMULA)
Delta ratio: 1.03 / 0.98 = 1.051 (base_price / other_price)
Other total: 39kb × 1.051 = 40.99kb
Note: Q2 is cheaper, so we need MORE volume for balanced exposure

Step 4: Round other total to AFE quarterly increment (39kb multiples)
40.99kb → 39kb (nearest multiple of 39kb, rounds down)
Other per mo: 39kb ÷ 3 = 13kb per mo

Dollar exposure check:
- Base: 39kb × $1.03 = $40,170
- Other: 39kb × $0.98 = $38,220
- ✅ Approximately balanced!

Display: "13kb per mo Far East Propane" / "13kb per mo Far East Propane"
```

---

## Example 5: CL Quarterly Base + AFE Quarterly Other

**Spread:** CL Q2 '26 vs AFE Q2 '26 (both quarterly)
- Base: CL Q2 '26 - 10kb per mo
- Other: AFE Q2 '26
- Prices: CL = $1.40/gallon, AFE = $0.98/gallon
- Pos%: 100%

### Correct Calculation:
```
Step 1: Convert base to total
Base: 10kb per mo × 3 = 30kb total

Step 2: Round base total to CL quarterly increment (3kb multiples)
30kb → 30kb (already multiple of 3)
Base per mo: 30kb ÷ 3 = 10kb per mo

Step 3: Calculate delta using base total (CORRECTED FORMULA)
Delta ratio: 1.40 / 0.98 = 1.429 (base_price / other_price)
Other total: 30kb × 1.429 = 42.87kb
Note: AFE is cheaper, so we need MORE volume for balanced exposure

Step 4: Round other total to AFE quarterly increment (39kb multiples)
42.87kb → 39kb (nearest multiple of 39kb, rounds down)
Other per mo: 39kb ÷ 3 = 13kb per mo

Dollar exposure check:
- Base: 30kb × $1.40 = $42,000
- Other: 39kb × $0.98 = $38,220
- Note: AFE minimum (39kb) creates slight imbalance, but trading constraint takes precedence

Display: "10kb per mo WTI Crude" / "13kb per mo Far East Propane"
```

---

## Summary of Rounding Rules

### For Quarterly Totals:
- **AFE quarterly**: Round to nearest multiple of 39kb (13kb × 3)
  - Valid totals: 39kb, 78kb, 117kb, 156kb...
  - Valid "per mo": 13kb, 26kb, 39kb, 52kb...
  
- **Other quarterlies**: Round to nearest multiple of 3kb (1kb × 3)
  - Valid totals: 30kb, 33kb, 36kb, 39kb...
  - Valid "per mo": 10kb, 11kb, 12kb, 13kb...

### For Monthly Quantities:
- **AFE monthly**: Round to nearest multiple of 13kb
- **Other monthlies**: Round to nearest 1kb (minimum 10kb)

### Key Principle:
**Always convert quarterly "per mo" to total volume before delta calculation, then convert back for display.**


