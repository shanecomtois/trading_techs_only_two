import icepython as ice
from datetime import datetime, timedelta

test_symbol = '%PRL F!-IEU'
today = datetime.now()

# Test the last few Fridays
print("Checking recent Friday dates for ICE data availability...\n")

# Find most recent Friday
days_since_friday = (today.weekday() - 4) % 7
if days_since_friday == 0 and today.weekday() != 4:
    days_since_friday = 7
most_recent_friday = today - timedelta(days=days_since_friday)

# Test last 4 Fridays
for i in range(4):
    test_friday = most_recent_friday - timedelta(days=7*i)
    end_date = test_friday.strftime('%Y-%m-%d')
    start_date = (test_friday - timedelta(days=14)).strftime('%Y-%m-%d')
    
    print(f"Testing {end_date} (Friday {i+1} weeks back)...")
    try:
        result = ice.get_timeseries([test_symbol], ['Close'], 'W', start_date, end_date)
        if result and len(result) > 0:
            # Get the latest date
            latest = max(r.get('Date', '') for r in result if 'Date' in r)
            print(f"  ✓ Data available, latest date: {latest}")
        else:
            print(f"  ✗ No data")
    except Exception as e:
        print(f"  ✗ Error: {e}")
    print()





