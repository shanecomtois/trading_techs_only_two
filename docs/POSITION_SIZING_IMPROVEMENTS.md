# Position Sizing System Improvements

This document outlines specific improvements to the position sizing system, with code examples and analysis.

## Current System Analysis

### Current Formula
```python
position_size = 100 / (atr_pct / 10)
```

### Issues Identified
1. **Arbitrary normalization** (`/10`) - not based on market data
2. **Formula sensitivity** - small ATR% changes cause large position size swings
3. **No account size consideration** - fixed volumes regardless of account size
4. **No correlation adjustment** - multiple correlated positions can overexpose
5. **Confusing 1.5x multiplier** - 100% pos% doesn't mean 100% of base

## Recommended Improvements

### Option 1: Improved Inverse ATR Formula (Easiest)

**Concept:** Use a target ATR% as the "normal" volatility baseline.

**Formula:**
```python
target_atr_pct = 5.0  # Configurable: "normal" volatility level
position_size = base_size × (target_atr_pct / actual_atr_pct)
```

**Benefits:**
- More intuitive: "If volatility is half of normal, use 2x position"
- Easier to understand and tune
- Still volatility-adjusted

**Example:**
- Target ATR% = 5%
- Actual ATR% = 2.5% → pos% = 100 × (5/2.5) = 200%
- Actual ATR% = 10% → pos% = 100 × (5/10) = 50%

**Implementation:**
```python
def calculate_position_size(self, row: pd.Series) -> float:
    base_size = self.config['position_sizing']['base_size']
    method = self.config['position_sizing']['method']
    
    if method == 'inverse_atr_pct':
        atr_pct = row.get('atr_pct_of_price', np.nan)
        if pd.isna(atr_pct) or atr_pct <= 0:
            return base_size
        
        # Get target ATR% from config (default 5%)
        target_atr_pct = self.config['position_sizing'].get('target_atr_pct', 5.0)
        
        # Improved formula: position = base × (target / actual)
        position_size = base_size * (target_atr_pct / atr_pct)
        
        # Cap between 10% and 200%
        position_size = max(10.0, min(200.0, position_size))
        
        return round(position_size, 2)
    
    return float(base_size)
```

**Config Addition:**
```json
"position_sizing": {
    "base_size": 100,
    "method": "inverse_atr_pct",
    "target_atr_pct": 5.0,
    "comment": "Target ATR% represents 'normal' volatility. Position size = base × (target / actual)"
}
```

---

### Option 2: Fixed Dollar Risk Per Trade (Most Professional)

**Concept:** Risk a fixed dollar amount per trade based on stop loss distance.

**Formula:**
```python
dollar_risk_per_trade = account_size × risk_percent_per_trade
stop_loss_distance = atr × stop_multiplier
position_size = dollar_risk_per_trade / (stop_loss_distance × price)
```

**Benefits:**
- Consistent dollar risk across all trades
- Professional risk management approach
- Works with any account size

**Example:**
- Account size: $1,000,000
- Risk per trade: 1% = $10,000
- Price: $1.00/gallon
- ATR: $0.05
- Stop multiplier: 0.56
- Stop distance: $0.05 × 0.56 = $0.028
- Position size: $10,000 / ($0.028 × $1.00) = 357,143 gallons = 357kb

**Implementation:**
```python
def calculate_position_size_fixed_risk(self, row: pd.Series) -> float:
    """
    Calculate position size based on fixed dollar risk per trade.
    """
    # Get config values
    account_size = self.config['position_sizing'].get('account_size', 1000000)
    risk_percent = self.config['position_sizing'].get('risk_percent_per_trade', 1.0)
    atr = row.get('atr', np.nan)
    close = row.get('close', np.nan)
    stop_mult = self.atr_stop_mult
    
    if pd.isna(atr) or pd.isna(close) or atr <= 0 or close <= 0:
        return self.config['position_sizing']['base_size']
    
    # Calculate dollar risk per trade
    dollar_risk = account_size * (risk_percent / 100.0)
    
    # Calculate stop loss distance
    stop_distance = atr * stop_mult
    
    # Calculate position size in units
    position_units = dollar_risk / stop_distance
    
    # Convert to kb (assuming 1 unit = 1 gallon, 1kb = 1000 gallons)
    position_kb = position_units / 1000.0
    
    # Convert to percentage of base volume for compatibility
    base_volume = self.config['position_sizing']['base_volume_outright']
    position_pct = (position_kb / base_volume) * 100.0
    
    # Cap between 10% and 200%
    position_pct = max(10.0, min(200.0, position_pct))
    
    return round(position_pct, 2)
```

**Config Addition:**
```json
"position_sizing": {
    "method": "fixed_dollar_risk",
    "account_size": 1000000,
    "risk_percent_per_trade": 1.0,
    "comment": "Risk 1% of account per trade. Position size calculated to match dollar risk."
}
```

---

### Option 3: Hybrid Approach (Best of Both)

**Concept:** Use volatility adjustment but with fixed dollar risk as the base.

**Formula:**
```python
# Step 1: Calculate base position from fixed dollar risk
base_position = calculate_fixed_risk_position(...)

# Step 2: Adjust for volatility
volatility_adjustment = target_atr_pct / actual_atr_pct

# Step 3: Apply adjustment (with limits)
final_position = base_position × volatility_adjustment
```

**Benefits:**
- Consistent dollar risk (professional)
- Still volatility-adjusted (risk-aware)
- Best of both worlds

---

## Additional Improvements

### 1. Correlation Adjustment

**Concept:** Reduce position size if signal is correlated with existing positions.

**Implementation:**
```python
def adjust_for_correlation(self, position_size: float, symbol: str, open_positions: List[Dict]) -> float:
    """
    Reduce position size if correlated with existing positions.
    """
    correlation_threshold = 0.7
    max_correlation_exposure = 0.5  # Max 50% of normal size if highly correlated
    
    for pos in open_positions:
        correlation = self.get_correlation(symbol, pos['symbol'])
        if correlation > correlation_threshold:
            # Reduce position size proportionally
            reduction = 1.0 - (correlation - correlation_threshold) / (1.0 - correlation_threshold)
            position_size = position_size * max(max_correlation_exposure, reduction)
    
    return position_size
```

### 2. Remove or Clarify 1.5x Multiplier

**Option A: Remove it**
```json
"base_multiplier_at_100pct": 1.0
```

**Option B: Make it clearer**
```json
"base_multiplier_at_100pct": 1.5,
"comment": "Standard position size multiplier. 100% pos% = base × 1.5. This represents the 'normal' trading size for signals with average volatility."
```

### 3. Add Position Size Analysis

**Create a diagnostic script:**
```python
# analyze_position_sizing.py
import pandas as pd
import numpy as np

def analyze_position_sizing(df):
    """
    Analyze how position sizing formula performs across different volatility levels.
    """
    # Calculate pos% for all rows
    df['pos_pct'] = df.apply(calculate_position_size, axis=1)
    
    # Group by ATR% ranges
    df['atr_pct_bucket'] = pd.cut(df['atr_pct_of_price'], 
                                   bins=[0, 2, 5, 10, 20, 100],
                                   labels=['<2%', '2-5%', '5-10%', '10-20%', '>20%'])
    
    # Analyze
    analysis = df.groupby('atr_pct_bucket').agg({
        'pos_pct': ['mean', 'median', 'min', 'max', 'std'],
        'atr_pct_of_price': 'mean'
    })
    
    print("Position Sizing Analysis:")
    print(analysis)
    
    # Check for issues
    print("\nPotential Issues:")
    if (df['pos_pct'] == 200.0).sum() > len(df) * 0.3:
        print("⚠️  Too many positions maxed out at 200% - formula may be too aggressive")
    if (df['pos_pct'] == 10.0).sum() > len(df) * 0.3:
        print("⚠️  Too many positions at minimum 10% - formula may be too conservative")
```

---

## Testing Recommendations

### 1. Backtest Position Sizing
- Run historical analysis to see how position sizes would have varied
- Check if formula produces reasonable sizes across different market conditions

### 2. Stress Test
- Test with extreme volatility scenarios
- Verify caps work correctly
- Check edge cases (missing ATR data, zero prices, etc.)

### 3. Compare Methods
- Run both current and improved formulas on same data
- Compare results
- Choose method that produces most consistent risk

---

## Recommended Implementation Order

1. **Immediate (Easy):** Implement Option 1 (Improved Inverse ATR Formula)
   - Add `target_atr_pct` to config
   - Update formula
   - Test and tune `target_atr_pct` value

2. **Short-term:** Add position sizing analysis script
   - Understand current behavior
   - Identify issues
   - Validate improvements

3. **Medium-term:** Consider Option 2 (Fixed Dollar Risk)
   - More professional approach
   - Requires account size configuration
   - Better for live trading

4. **Long-term:** Add correlation adjustment
   - Track open positions
   - Adjust for correlation
   - Prevent overexposure

---

## Configuration Template

```json
{
  "position_sizing": {
    "base_size": 100,
    "method": "inverse_atr_pct_improved",
    "target_atr_pct": 5.0,
    "base_volume_outright": 10,
    "base_volume_spread_per_side": 10,
    "max_volume": 30,
    "min_volume": 10,
    "base_multiplier_at_100pct": 1.0,
    "account_size": 1000000,
    "risk_percent_per_trade": 1.0,
    "enable_correlation_adjustment": false,
    "correlation_threshold": 0.7,
    "comment": "Improved position sizing with target ATR% baseline. Can switch to fixed_dollar_risk method for professional risk management."
  }
}
```

---

*Last Updated: 2025-12-04*


