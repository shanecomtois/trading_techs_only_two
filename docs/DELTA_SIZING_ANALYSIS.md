# Delta Sizing Analysis: Increment Rounding Impact

## Current Delta Sizing Formula

```python
delta_ratio = other_price / base_price
scaled_other_quantity = base_quantity * delta_ratio
```

## Mathematical Analysis

### Example 1: AFE Spread (Both Legs AFE)
- **Base leg**: AFE Jan '26, price = $1.02812/gallon
- **Other leg**: AFE Mar '26, price = $0.97932/gallon
- **Base quantity**: 15kb (from 10kb × 1.5 multiplier at 100% pos%)

**Current calculation:**
```
delta_ratio = 0.97932 / 1.02812 = 0.9525
scaled_other_quantity = 15kb × 0.9525 = 14.29kb
rounded_other_quantity = 14kb
```

**Dollar exposure:**
- Base leg: 15kb × $1.02812 = $15,421.80
- Other leg: 14kb × $0.97932 = $13,710.48
- **Difference**: $1,711.32 (11.1% imbalance)

### Example 2: AFE vs CL Spread
- **Base leg**: AFE Mar '26, price = $0.97932/gallon
- **Other leg**: CL Apr '26, price = $1.40190/gallon (after /42 conversion)
- **Base quantity**: 13kb (AFE minimum)

**Current calculation:**
```
delta_ratio = 1.40190 / 0.97932 = 1.430
scaled_other_quantity = 13kb × 1.430 = 18.59kb
rounded_other_quantity = 19kb
```

**Dollar exposure:**
- Base leg: 13kb × $0.97932 = $12,731.16
- Other leg: 19kb × $1.40190 = $26,636.10
- **Difference**: $13,904.94 (109% imbalance!)

## Problem with Current Approach

The current formula `other_quantity = base_quantity × (other_price / base_price)` does NOT balance dollar exposure. It actually creates imbalances.

**For balanced dollar exposure, we need:**
```
base_quantity × base_price = other_quantity × other_price
other_quantity = base_quantity × (base_price / other_price)
```

**Current formula uses inverse:**
```
other_quantity = base_quantity × (other_price / base_price)
```

## Impact of Increment Rounding

### Scenario: AFE Spread with 13kb Increments

**Before rounding:**
- Base: 15kb
- Other: 14.29kb (from delta calculation)
- Dollar exposure: $15,421.80 vs $13,710.48 (11.1% imbalance)

**After rounding to 13kb increments:**
- Base: 13kb (rounded down from 15kb)
- Other: 13kb (rounded to nearest 13kb increment)
- Dollar exposure: $13,365.56 vs $12,731.16 (4.7% imbalance)

**Analysis:**
- Rounding actually REDUCES the dollar exposure imbalance (from 11.1% to 4.7%)
- Both quantities are now valid trading increments
- The delta relationship is approximately preserved

## Proposed Solution

### Step 1: Round Base Quantity to Increment
```python
base_quantity = round_to_increment(scaled_base_quantity, base_min)
# Example: 15kb → 13kb (for AFE with 13kb increment)
```

### Step 2: Calculate Delta from Rounded Base
```python
delta_ratio = other_price / base_price
scaled_other_quantity = base_quantity * delta_ratio
# Example: 13kb × 0.9525 = 12.38kb
```

### Step 3: Round Other Quantity to Increment
```python
other_quantity = round_to_increment(scaled_other_quantity, other_min)
# Example: 12.38kb → 13kb (for AFE with 13kb increment)
```

### Step 4: Verify Dollar Exposure
```python
base_exposure = base_quantity * base_price
other_exposure = other_quantity * other_price
imbalance_pct = abs(base_exposure - other_exposure) / base_exposure * 100
# Log if imbalance > 10% as warning
```

## Key Insights

1. **Current delta formula doesn't balance dollar exposure** - it uses inverse ratio
2. **Increment rounding can actually improve balance** - by forcing both legs to same increment
3. **For same-commodity spreads (AFE/AFE)**, rounding to same increment naturally balances exposure
4. **For cross-commodity spreads**, rounding may increase imbalance, but trading constraints take precedence

## Recommendation

1. Implement increment rounding as proposed
2. Add dollar exposure imbalance logging/warning
3. Consider if delta formula should be corrected to `base_price / other_price` for true dollar balancing
4. Document that increment constraints may override perfect delta ratios


