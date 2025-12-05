# Strategy Rules Documentation

This document provides detailed rules for how the two trading strategies work in the signal generator system.

---

## 📈 Strategy 1: Trend Following Signals

### **Entry Condition: MACD Cross**

**Buy Signal:**
- **Trigger:** MACD line crosses **above** the signal line
- **Logic:** 
  - Previous period: `macd_line < macd_signal`
  - Current period: `macd_line > macd_signal`
  - This indicates momentum shift to the upside

**Sell Signal:**
- **Trigger:** MACD line crosses **below** the signal line
- **Logic:**
  - Previous period: `macd_line > macd_signal`
  - Current period: `macd_line < macd_signal`
  - This indicates momentum shift to the downside

**Requirements:**
- Both `macd_line` and `macd_signal` must be valid (not NaN)
- Previous row data must be available to detect the cross
- If previous row is missing, no signal is generated

### **Base Points: 50**

Every MACD cross that meets the entry condition receives **50 base points**.

### **Confluence Bonus Indicators**

Additional points are awarded when other indicators align with the signal direction:

#### **1. RSI Aligned (+10 points)**
- **Buy Signal:** RSI < 30 (oversold, confirming upward momentum)
- **Sell Signal:** RSI > 70 (overbought, confirming downward momentum)
- **Points:** 10

#### **2. Stochastic Aligned (+10 points)**
- **Buy Signal:** Stochastic %K < 20 (oversold)
- **Sell Signal:** Stochastic %K > 80 (overbought)
- **Points:** 10

#### **3. CCI Aligned (+10 points)**
- **Buy Signal:** CCI < -100 (oversold)
- **Sell Signal:** CCI > 100 (overbought)
- **Points:** 10

#### **4. ADX Strong (+15 points)**
- **Condition:** ADX > 25 (indicates strong trend, regardless of direction)
- **Points:** 15
- **Note:** This confirms trend strength but doesn't need to align with buy/sell direction

#### **5. Bollinger Bands Aligned (+10 points)**
- **Buy Signal:** Price ≤ Lower Bollinger Band × 1.02 (within 2% of lower band)
- **Sell Signal:** Price ≥ Upper Bollinger Band × 0.98 (within 2% of upper band)
- **Points:** 10

#### **6. Correlation High (+10 points) - Spreads Only**
- **Condition:** Correlation > 0.7 between spread components
- **Points:** 10
- **Note:** Only applies to spreads, not outrights

#### **7. Cointegration (+15 points) - Spreads Only**
- **Condition:** Cointegration p-value < 0.10 (statistically significant cointegration)
- **Points:** 15
- **Note:** Only applies to spreads, not outrights

### **Example Scoring:**
- Base: 50 points
- RSI aligned: +10
- ADX strong: +15
- **Total: 75 points** ✅ (Meets minimum threshold)

---

## 🔄 Strategy 2: Mean Reversion Signals

### **Entry Condition: Price Percentile Extreme**

**Buy Signal:**
- **Trigger:** Price percentile < 25
- **Logic:** Price is in the bottom 25% of its historical range (oversold)
- **Expectation:** Price should bounce back up toward the mean

**Sell Signal:**
- **Trigger:** Price percentile > 75
- **Logic:** Price is in the top 25% of its historical range (overbought)
- **Expectation:** Price should pull back down toward the mean

**Requirements:**
- `percentile_close` must be valid (not NaN)
- No previous row needed (percentile is absolute, not a cross)

### **Base Points: 50**

Every price percentile extreme that meets the entry condition receives **50 base points**.

### **Confluence Bonus Indicators**

Additional points are awarded when other indicators confirm the mean reversion setup:

#### **1. RSI Percentile Aligned (+15 points)**
- **Buy Signal:** RSI percentile < 25 (RSI also oversold, confirming extreme)
- **Sell Signal:** RSI percentile > 75 (RSI also overbought, confirming extreme)
- **Points:** 15
- **Note:** This is the RSI's percentile rank, not the RSI value itself

#### **2. MACD Reversal (+10 points)**
- **Buy Signal:** MACD histogram > 0 (turning positive, indicating reversal from negative)
- **Sell Signal:** MACD histogram < 0 (turning negative, indicating reversal from positive)
- **Points:** 10
- **Note:** Simplified check - could be enhanced with previous value comparison

#### **3. Bollinger Bands Extreme (+10 points)**
- **Buy Signal:** Price ≤ Lower Bollinger Band × 1.02 (within 2% of lower band)
- **Sell Signal:** Price ≥ Upper Bollinger Band × 0.98 (within 2% of upper band)
- **Points:** 10
- **Note:** Same logic as trend following, but confirms extreme position

#### **4. Correlation High (+10 points) - Spreads Only**
- **Condition:** Correlation > 0.7 between spread components
- **Points:** 10
- **Note:** Only applies to spreads, not outrights

#### **5. Cointegration (+20 points) - Spreads Only**
- **Condition:** Cointegration p-value < 0.10 (statistically significant cointegration)
- **Points:** 20
- **Note:** Higher weight for mean reversion (20 vs 15 for trend following)
- **Rationale:** Cointegration is more important for mean reversion strategies

### **Example Scoring:**
- Base: 50 points
- RSI percentile aligned: +15
- Bollinger extreme: +10
- **Total: 75 points** ✅ (Meets minimum threshold)

---

## 🎯 Common Rules & Filters

### **Minimum Points Threshold: 75**

- Only signals with **total points ≥ 75** are displayed in the report
- Total points = Base points + Confluence bonus points
- Signals below 75 are filtered out

### **Maximum Signals Per Type: 10**

- Maximum **10 buy signals** per strategy
- Maximum **10 sell signals** per strategy
- Signals are sorted by total points (highest first)
- Top 10 are selected for each type

### **Risk Management**

#### **Stop Loss Calculation:**
- **Formula:** `Stop = Entry Price ± (0.56 × ATR)`
- **Buy Signal:** Stop = Entry Price - (0.56 × ATR)
- **Sell Signal:** Stop = Entry Price + (0.56 × ATR)
- **ATR:** Average True Range (volatility measure)

#### **Target Calculation:**
- **Formula:** `Target = Entry Price ± (0.83 × ATR)`
- **Buy Signal:** Target = Entry Price + (0.83 × ATR)
- **Sell Signal:** Target = Entry Price - (0.83 × ATR)

#### **Risk/Reward Ratio:**
- Risk: 0.56 × ATR
- Reward: 0.83 × ATR
- Ratio: ~1.48:1 (favorable risk/reward)

### **Position Sizing**

#### **Method: Inverse ATR Percentage**
- **Formula:** Position size is inversely proportional to ATR% of price
- **Logic:** Higher volatility = smaller position size
- **Base Size:** 100 (configurable)

#### **ICE Chat Quantity Calculation:**
Based on position size percentage (`pos_pct`):
- **≥ 5.0%:** 30kb (or 30kb per mo for quarterlies)
- **≥ 4.0%:** 25kb (or 25kb per mo for quarterlies)
- **≥ 3.0%:** 20kb (or 20kb per mo for quarterlies)
- **≥ 2.0%:** 15kb (or 15kb per mo for quarterlies)
- **< 2.0%:** 10kb (or 10kb per mo for quarterlies)

### **Alignment Score (0-100)**

A weighted score indicating how many confluence indicators are aligned:

#### **Calculation:**
1. Each aligned indicator contributes its weight
2. Sum of aligned indicator weights
3. Divided by total possible weight sum
4. Multiplied by 100

#### **Indicator Weights:**
- **RSI:** 1.0
- **Stochastic:** 1.0
- **CCI:** 1.0
- **ADX:** 1.5 (more important)
- **Bollinger:** 1.0
- **Correlation:** 1.2
- **Cointegration:** 1.5 (more important)
- **MACD:** 2.0 (most important)

#### **Alignment Icons:**
- **✅✅ Strong Agree:** Alignment score ≥ 90
- **✅ Agree:** Alignment score 80-89
- **⚪ Neutral:** Alignment score 70-79
- **❌ Disagree:** Alignment score 60-69
- **❌❌ Strong Disagree:** Alignment score < 60

### **Score Breakdown Display**

All indicators are shown in the score breakdown, even if they contribute 0 points:
- **Format:** `Base 50 + ADX +0 + RSI +15 + CMF +0 + BBW +0 = Final 75.0`
- This provides transparency on which indicators aligned and which didn't

---

## 📊 Signal Generation Process

### **Step-by-Step Flow:**

1. **Load Data:** Load OHLC data and indicators from CSV
2. **For Each Symbol:**
   - Check entry condition for each strategy
   - If entry condition met:
     - Calculate base points
     - Calculate confluence bonuses
     - Calculate total points
     - Calculate stop/target (ATR-based)
     - Calculate position size
     - Calculate alignment score
3. **Filter Signals:**
   - Remove signals with points < 75
   - Sort by total points (descending)
   - Take top 10 buy and top 10 sell per strategy
4. **Format ICE Chat Messages:**
   - Generate ICE Chat verbiage for each signal
   - Include quantity, product name, location code, date
5. **Generate Report:**
   - Create HTML report with all signals
   - Display in UET light theme design

---

## 🔧 Configuration

All rules are configurable in `signal_generator/config/signal_settings.json`:

- **Base points:** Adjustable per strategy
- **Confluence bonus points:** Adjustable per indicator
- **Minimum points threshold:** Default 75
- **Max signals per type:** Default 10
- **ATR multipliers:** Stop (0.56) and Target (0.83)
- **Alignment weights:** Adjustable per indicator type

---

## 📝 Notes

- **Spreads vs Outrights:** Some indicators (correlation, cointegration) only apply to spreads
- **Missing Data:** If an indicator value is NaN, it contributes 0 points (no error)
- **Backtesting Ready:** System can generate signals for any historical date
- **Modular Design:** Easy to add new strategies or indicators

---

*Last Updated: 2025-12-04*


