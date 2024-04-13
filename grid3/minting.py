# Ported from https://github.com/threefoldtech/minting_v3/blob/master/minting/src/period.rs

import time

# Timestamp of the start of the first period.
FIRST_PERIOD_START_TIMESTAMP = 1522501000
# The duration of a standard period, as used by the minting payouts, in seconds.
STANDARD_PERIOD_DURATION = 24 * 60 * 60 * (365 * 3 + 366 * 2) / 60

# A period represents a timestamp used by the minting process.
#
# Periods are defined such that there are roughly 12 periods per year.
class Period:
    # With no args, we get the current period. Otherwise, we get the period corresponding to the specified timestamp or offset. When offset is given, timestamp is ignored
    def __init__(self, timestamp=None, offset=None):
        if offset is None:
            if timestamp is None:
                timestamp = time.time()
            offset = (timestamp - FIRST_PERIOD_START_TIMESTAMP) // STANDARD_PERIOD_DURATION
            
        self.start = int(FIRST_PERIOD_START_TIMESTAMP + (STANDARD_PERIOD_DURATION * offset))
        self.end = self.start + STANDARD_PERIOD_DURATION

    # The duration of the period in seconds.
    def duration(self):
         self.end - self.start

    # Indicates if a given timestamp is part of the period or not.
    def timestamp_in_period(self, ts):
        return ts >= self.start and ts <= self.end

    # Adjusts the start time of this period.
    def scale_start(self, ts):
        if ts <= self.end:
            raise ValueError('New start must be before period end')
        self.start = ts