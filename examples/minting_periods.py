import grid3.minting.period
import datetime
import sys

if len(sys.argv) > 1:
    year = sys.argv[1]
else:
    year = 2025

# Find the first period of the target year
offset = 0
first_period = None

# Search forward until we find the first period of the target year
while True:
    period = grid3.minting.period.Period(offset=offset)
    end_date = datetime.datetime.fromtimestamp(period.end)
    if end_date.year == year and end_date.day > 15:
        first_period = period
        break
    else:
        offset += 1

print("| Period Offset | Start Date | End Date |")
print("|---------------|------------|----------|")
for i in range(first_period.offset, first_period.offset + 12):
    period = grid3.minting.period.Period(offset=i)
    start_date = datetime.datetime.fromtimestamp(period.start).isoformat()
    end_date = datetime.datetime.fromtimestamp(period.end).isoformat()
    print(f"| {period.offset} | {start_date} | {end_date} |")
