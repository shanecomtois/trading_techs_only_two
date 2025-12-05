# Complete Pay/Rcv Logic Trace - All Scenarios

## Stop and Target Calculation
- **BUY**: Stop = Entry - ATR×0.56, Target = Entry + ATR×0.83
- **SELL**: Stop = Entry + ATR×0.56, Target = Entry - ATR×0.83

---

## BUY SPREAD SCENARIOS

### Scenario 1A: BUY Spread, Positive Entry (+$0.0135)

**Entry:**
- Price = +$0.0135 → **Pay $0.0135** ✓

**Stop Loss (Entry - $0.0480):**
- Stop = +$0.0135 - $0.0480 = **-$0.0345**
- Price moved DOWN (worse) = Loss
- **Result: Pay $0.0345** ✓

**Target Profit (Entry + $0.0711):**
- Target = +$0.0135 + $0.0711 = **+$0.0846**
- Price moved UP (better) = Profit
- **Result: Rcv $0.0846** ✓

---

### Scenario 1B: BUY Spread, Negative Entry (-$0.0135)

**Entry:**
- Price = -$0.0135 → **Rcv $0.0135** ✓

**Stop Loss (Entry - $0.0480):**
- Stop = -$0.0135 - $0.0480 = **-$0.0615**
- Price moved MORE NEGATIVE (worse) = Loss
- **Result: Pay $0.0615** ✓

**Target Profit (Entry + $0.0711):**
- Target = -$0.0135 + $0.0711 = **+$0.0576**
- Price moved to POSITIVE (better) = Profit
- **Result: Rcv $0.0576** ✓

---

## SELL SPREAD SCENARIOS

### Scenario 2A: SELL Spread, Positive Entry (+$0.0135)

**Entry:**
- Price = +$0.0135 → **Rcv $0.0135** ✓

**Stop Loss (Entry + $0.0480):**
- Stop = +$0.0135 + $0.0480 = **+$0.0615**
- Price moved UP (worse for short) = Loss
- **Result: Pay $0.0615** ✓

**Target Profit (Entry - $0.0711):**
- Target = +$0.0135 - $0.0711 = **-$0.0576**
- Price moved DOWN (better for short) = Profit
- **Result: Rcv $0.0576** ✓

---

### Scenario 2B: SELL Spread, Negative Entry (-$0.0135)

**Entry:**
- Price = -$0.0135 → **Pay $0.0135** ✓

**Stop Loss (Entry + $0.0480):**
- Stop = -$0.0135 + $0.0480 = **+$0.0345**
- Price moved to POSITIVE (worse for short) = Loss
- **Result: Pay $0.0345** ✓

**Target Profit (Entry - $0.0711):**
- Target = -$0.0135 - $0.0711 = **-$0.0846**
- Price moved MORE NEGATIVE (better for short) = Profit
- **Result: Rcv $0.0846** ✓

---

## SUMMARY TABLE

| Signal | Entry Sign | Entry | Stop | Target | Entry Result | Stop Result | Target Result |
|--------|------------|-------|------|--------|--------------|-------------|---------------|
| BUY Spread | + | +$0.0135 | -$0.0345 | +$0.0846 | Pay | Pay | Rcv |
| BUY Spread | - | -$0.0135 | -$0.0615 | +$0.0576 | Rcv | Pay | Rcv |
| SELL Spread | + | +$0.0135 | +$0.0615 | -$0.0576 | Rcv | Pay | Rcv |
| SELL Spread | - | -$0.0135 | +$0.0345 | -$0.0846 | Pay | Pay | Rcv |

## CODE LOGIC VERIFICATION

### BUY Spread:
- **Price**: `prefix = "Pay" if value >= 0 else "Rcv"` ✓
- **Stop**: `prefix = "Pay" if value < 0 else "Rcv"` (selling at exit) ✓
- **Target**: `prefix = "Rcv" if value >= 0 else "Pay"` (selling at exit) ✓

### SELL Spread:
- **Price**: `prefix = "Pay" if value < 0 else "Rcv"` ✓
- **Stop**: `prefix = "Pay" if value >= 0 else "Rcv"` (buying at exit) ✓
- **Target**: `prefix = "Rcv" if value < 0 else "Pay"` (buying at exit) ✓

## CONCLUSION
**All logic is now correct!** The code properly handles the sign of stop/target prices and whether you're buying or selling at exit.


