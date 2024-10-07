import time, datetime

# Timestamp of the start of the first period.
FIRST_PERIOD_START_TIMESTAMP = 1522501000
# The duration of a standard period, as used by minting, in seconds.
STANDARD_PERIOD_DURATION = 24 * 60 * 60 * (365 * 3 + 366 * 2) // 60


class Period:
    """A class representing a minting period. When instantiated with no args,
    the period containing the current point in time is returned. Otherwise,
    either a timestamp or a minting period offset can be passed.

    All calculations should match the canonical forms found in the minting
    code:

    https://github.com/threefoldtech/minting_v3/blob/master/minting/src/period.rs

    There are 12 minting periods per year, with the boundaries falling
    roughly at normal month boundaries.
    """

    def __init__(self, timestamp=None, offset=None):
        if offset is None:
            if timestamp is None:
                timestamp = time.time()
            self.offset = (
                timestamp - FIRST_PERIOD_START_TIMESTAMP
            ) // STANDARD_PERIOD_DURATION
        else:
            self.offset = offset

        self.start = int(
            FIRST_PERIOD_START_TIMESTAMP + (STANDARD_PERIOD_DURATION * self.offset)
        )
        self.end = self.start + STANDARD_PERIOD_DURATION

        # Each minting period falls almost entirely into a single month. The
        # start or end day might be in a different month though. So using the
        # middle of the period, we get the "human" interpretation of which
        # month this period corresponds to
        middle = datetime.datetime.fromtimestamp((self.start + self.end) / 2)
        self.month = middle.month
        self.month_name = middle.strftime("%B")
        self.year = middle.year

    # The duration of the period in seconds.
    def duration(self):
        self.end - self.start

    # Indicates if a given timestamp is part of the period or not.
    def timestamp_in_period(self, ts):
        return ts >= self.start and ts <= self.end

    # Adjusts the start time of this period.
    def scale_start(self, ts):
        if ts <= self.end:
            raise ValueError("New start must be before period end")
        self.start = ts
