"""This module contains a port of the minting code found here:
https://github.com/threefoldtech/minting_v3/blob/master/minting/src/main.rs

It's translated line for line where possible, and Rustisms are converted into
equivalent Python logic where necessary. The primary difference is that it
works against a sqlite database containing relevant events collected from
tfchain, rather than tfchain itself. Code for the "ingester" to create those
databases is currently hosted on this repo:

https://github.com/threefoldfoundation/node-status-bot

The tfchain code is already heavily nested, so it's left as top level functions
rather than methods of the Minting Node class, to save another indentation
level. For clarity, the code is split into three functions representing three
logical phases of minting, though actual minting is a single function.

Our MintingNode does not implement most of the normal minting functions like
determinting the payout owed. Instead, it focuses on uptime accrual and records
a log of every uptime credit, along with any implied downtime. The node object
holds this data as a property and can export it as a CSV file too.
"""

import collections, csv
from datetime import datetime

# from .period import Period

UPTIME_GRACE_PERIOD_SECONDS = 60  # 1 Minute
CLOCK_SKEW_INTERVAL = 2 * UPTIME_GRACE_PERIOD_SECONDS
NODE_UPTIME_REPORT_INTERVAL_SECONDS = 60 * 40  # 40 minutes
MAX_POWER_MANAGER_DOWNTIME = 60 * 60 * 24
MAX_POWER_MANAGER_BOOT_TIME = 60 * 30
MAX_ALLOWED_BOOT_VIOLATIONS = 1
POST_PERIOD = 60 * 60 * 27
PERIOD_CATCH = 30

# A primitive logging solution that supports either printing to stdout or
# logging into a file
logging_mode = "print"
log_file = None


def log(*args):
    if logging_mode == "print":
        print(*args)
    elif logging_mode == "file":
        log_file.write(" ".join([str(arg) for arg in args]))


def process_period(node, events, period):
    node.last_uptime_added_ts = period.start
    for event in events:
        if isinstance(event, NodeUptimeReported):
            current_time = event.timestamp
            reported_uptime = event.uptime
            # We are power managed and got a request to wake up.
            if node.power_managed is not None and node.power_manage_boot is not None:
                # Ignore the event if it is sent after the node is supposed to
                # go down, this will be accounted for once the node starts up
                # again. For the node to have been properly power managed, it
                # must be booted after it was set to down.
                time_set_down = node.power_managed
                boot_request = node.power_manage_boot
                if (current_time - reported_uptime) > time_set_down:
                    # node got power managed to down
                    time_delta = current_time - time_set_down
                    assert time_delta >= 0, "uptime events can't travel back in time"
                    if node.uptime_info is None:
                        total_uptime = 0
                    else:
                        total_uptime = node.uptime_info[2]
                    # Only add uptime if node boot did not violate any constraints.
                    credit_uptime = True
                    if time_delta > MAX_POWER_MANAGER_DOWNTIME:
                        credit_uptime = False
                        log(
                            datetime.fromtimestamp(event.timestamp),
                            f"Refusing to credit uptime for power managed node {node.id} as the last boot was {time_delta} seconds ago, more than the allowed 24 hours\n",
                        )
                    if (
                        current_time - reported_uptime
                    ) - boot_request > MAX_POWER_MANAGER_BOOT_TIME:
                        credit_uptime = False
                        # Mark a violation on the node
                        node.boot_duration_violations += 1
                        log(
                            datetime.fromtimestamp(event.timestamp),
                            "Detected farmer bot boot violation for node {}, request was done at {} but node only came online at {}\n".format(
                                node.id,
                                datetime.fromtimestamp(boot_request),
                                datetime.fromtimestamp(current_time - reported_uptime),
                            ),
                        )
                    if credit_uptime:
                        # Check and scale to match the actual period start if needed
                        if time_set_down < period.start:
                            total_uptime += current_time - period.start
                            log(
                                datetime.fromtimestamp(event.timestamp),
                                "Added {} seconds of uptime for node {}, scaled in period start\n".format(
                                    current_time - period.start, node.id
                                ),
                            )
                            node.credit_uptime(
                                current_time - period.start,
                                event.timestamp,
                                "Crediting standby node for first wakeup of the period",
                            )
                        else:
                            total_uptime += time_delta
                            log(
                                datetime.fromtimestamp(event.timestamp),
                                f"Added {time_delta} seconds of uptime for node {node.id}",
                            )
                            node.credit_uptime(
                                time_delta, event.timestamp, "Crediting standby node"
                            )

                    # Clear the fact that we got power managed, if it is still
                    # the case, it will be set again in the proper event
                    # handler.
                    node.power_managed = None
                    node.power_manage_boot = None
                    node.uptime_info = (current_time, reported_uptime, total_uptime)
                    # Also mark a boot
                    node.boot_time = (current_time - reported_uptime, current_time)
                else:
                    log(
                        datetime.fromtimestamp(event.timestamp),
                        "Ignoring uptime event for node {} as it happened before the node powered down after being requested to do so\n".format(
                            node.id
                        ),
                    )

            # We are power managed but woke up without boot request. We
            # explicitly ignore this: being put to sleep by the farmer bot
            # requires a wakeup from the farmer bot. This case also means
            # nodes just go to sleep anyhow.
            elif node.power_managed is not None and node.power_manage_boot is None:
                log(
                    datetime.fromtimestamp(event.timestamp),
                    "Ignoring boot for node {} which is power managed, but did not get a boot request from the farmer bot\n".format(
                        node.id
                    ),
                )

            # We got a wakeup request from farmer bot but we are not sleeping
            # due to the farmer bot. This should not happen.
            elif node.power_managed is None and node.power_manage_boot is not None:
                log(
                    datetime.fromtimestamp(event.timestamp),
                    "Ignoring uptime for node {} after farmer bot asked for a boot while the node was not sleeping as a result of farmer bot\n".format(
                        node.id
                    ),
                )

            elif node.power_managed is None and node.power_manage_boot is None:
                if node.uptime_info is not None:
                    last_reported_at, last_reported_uptime, total_uptime = (
                        node.uptime_info
                    )
                    report_delta = current_time - last_reported_at
                    uptime_delta = reported_uptime - last_reported_uptime
                    # There are quite some situations here. Notice that due to
                    # the blockchain only producing blocks every 6 seconds, and
                    # network delay + a host of other issues, we will allow a
                    # node to report uptime with "grace period" of a minute or
                    # so in either direction.
                    #
                    # 1. uptime_delta > report_delta + GRACE_PERIOD. Node is
                    # talking rubish.
                    if uptime_delta > report_delta + UPTIME_GRACE_PERIOD_SECONDS:
                        log(
                            datetime.fromtimestamp(event.timestamp),
                            "Violation detected, uptime too high",
                        )
                        node.uptime_info = (current_time, reported_uptime, total_uptime)

                        log(
                            datetime.fromtimestamp(event.timestamp),
                            f"Node {node.id} reported an uptime increase of {uptime_delta} seconds, while reports are {report_delta} seconds appart\n",
                        )

                    # 2. The difference in uptime is within reason of the
                    # difference in report times, i.e. the node is properly
                    # reporting.
                    if (
                        uptime_delta <= report_delta + UPTIME_GRACE_PERIOD_SECONDS
                        and uptime_delta >= report_delta - UPTIME_GRACE_PERIOD_SECONDS
                    ):
                        # check skew
                        if node.boot_time is not None:
                            boot, detected = node.boot_time
                            new_boot = current_time - reported_uptime
                            if abs(new_boot - boot) >= CLOCK_SKEW_INTERVAL:
                                # This is a violation
                                log(
                                    datetime.fromtimestamp(event.timestamp),
                                    "Node {} has a detected clock skew of {} seconds, more than the allowed {} seconds\n".format(
                                        node.id,
                                        abs(new_boot - boot),
                                        CLOCK_SKEW_INTERVAL,
                                    ),
                                )
                        else:
                            exit("node does not have boot time but does have uptime")

                        # It is technically possible for the delta to be less
                        # than 0 and within the expected time frame. If nodes
                        # boot, send uptime, then immediately reboot that is
                        # possible. In those cases, handle that below, as that
                        # is the reboot detection.
                        if uptime_delta > 0:
                            # Simply add the uptime delta. If this is too large
                            # or low by a couple of seconds it will be
                            # corrected by the next pings anyhow. That being
                            # said, we also limit the amount of uptime credit
                            # to the uptime report interval + grace period, as
                            # healthy nodes _must_ ping every interval amount
                            # of time
                            credit = min(
                                uptime_delta,
                                (
                                    NODE_UPTIME_REPORT_INTERVAL_SECONDS
                                    + UPTIME_GRACE_PERIOD_SECONDS
                                ),
                            )
                            total_uptime += credit
                            if credit != uptime_delta:
                                log(
                                    datetime.fromtimestamp(event.timestamp),
                                    f"credited node {node.id} with {credit} seconds of uptime, less than the reported {uptime_delta} seconds as the gap is too big",
                                )
                                node.credit_uptime(
                                    credit,
                                    event.timestamp,
                                    "Less than reported, gap is too big",
                                )
                            else:
                                log(
                                    datetime.fromtimestamp(event.timestamp),
                                    f"credited node {node.id} with {credit} seconds of reported uptime\n",
                                )
                                node.credit_uptime(credit, event.timestamp)

                            node.uptime_info = (
                                current_time,
                                reported_uptime,
                                total_uptime,
                            )
                            continue

                    # 3. The difference in uptime is too low. Again there are
                    # multiple scenarios. Either way we consider the node
                    # rebooted. Depending on the reported uptime, the node
                    # reports legit uptime, or it reports an uptime which is
                    # too high.
                    #
                    #    1. Uptime is within bounds.
                    if reported_uptime <= report_delta:
                        credit = min(
                            reported_uptime,
                            (
                                NODE_UPTIME_REPORT_INTERVAL_SECONDS
                                + UPTIME_GRACE_PERIOD_SECONDS
                            ),
                        )
                        total_uptime += credit
                        if reported_uptime != credit:
                            log(
                                datetime.fromtimestamp(event.timestamp),
                                f"credited node {node.id} with {credit} seconds of uptime after a reboot, less than the reported {reported_uptime} seconds as the gap is too big\n",
                            )
                            node.credit_uptime(
                                credit,
                                event.timestamp,
                                "Less than reported, gap is too big",
                            )
                        else:
                            log(
                                datetime.fromtimestamp(event.timestamp),
                                f"credited node {node.id} with {credit} seconds of reported uptime after a reboot\n",
                            )
                            node.credit_uptime(credit, event.timestamp, "Node rebooted")
                        node.uptime_info = (current_time, reported_uptime, total_uptime)
                        node.boot_time = (current_time - reported_uptime, current_time)
                        continue

                    #    2. Uptime is actually higher than difference in
                    #    timestamp, but not high enough to be valid. This means
                    #    the node was supposedly rebooted _before_ the previous
                    #    uptime report, meaning either that report is invalid
                    #    or this report is invalid.
                    if reported_uptime > last_reported_uptime:
                        log(
                            datetime.fromtimestamp(event.timestamp),
                            f"Node {node.id} reported uptime of {reported_uptime} seconds, so time would have advanced slower on the node than in the universe\n",
                        )
                        continue

                    #    3. Uptime is too high, this is garbage
                    log(
                        datetime.fromtimestamp(event.timestamp),
                        f"Node {node.id} reported uptime of {reported_uptime} seconds, so time would have advanced faster on the node than in the universe\n",
                    )
                    continue
                else:
                    period_duration = current_time - period.start
                    # Make sure we don't give more credit than the current
                    # length of the period. Account for uptime period
                    up_in_period = min(
                        min(period_duration, reported_uptime),
                        NODE_UPTIME_REPORT_INTERVAL_SECONDS
                        + UPTIME_GRACE_PERIOD_SECONDS,
                    )
                    log(
                        datetime.fromtimestamp(event.timestamp),
                        f"Node {node.id} reported uptime of {reported_uptime} seconds, scaled to {up_in_period} seconds\n",
                    )
                    node.credit_uptime(
                        up_in_period, event.timestamp, "Possibly scaled to period start"
                    )
                    # Save uptime info
                    node.uptime_info = (current_time, reported_uptime, up_in_period)
                    node.boot_time = (current_time - reported_uptime, current_time)

        elif isinstance(event, PowerTargetChanged):
            log(
                datetime.fromtimestamp(event.timestamp),
                "Power target changed from {} to {}\n".format(
                    node.power_target, event.target
                ),
            )
            # Remember a rising edge here to validate node actually boots. This
            # is cleared when a node sends an uptime report of a _reboot_. It
            # is allowed for this to happen if a rising edge is not consumed
            # yet, in which case the new event is ignored, as we want to
            # measure time from the first event and it is actually a good idea
            # to send multiple of these if the node does not react. Of course,
            # we also only want to track this if the node is currently power
            # managed. While we shouldn't try to boot an online node, there is
            # no _real_ harm in doing it anyway.
            if event.target == "Up" and node.power_state == "Down":
                # Only remember the first boot request.
                if node.power_manage_boot is None:
                    node.power_manage_boot = event.timestamp
                    log(
                        datetime.fromtimestamp(event.timestamp),
                        "Remembered boot request time for node {}\n".format(node.id),
                    )
            node.power_target = event.target

        elif isinstance(event, PowerStateChanged):
            log(
                datetime.fromtimestamp(event.timestamp),
                "Power state changed from {} to {}\n".format(
                    node.power_state, event.state
                ),
            )
            # Add exception to allow node 1 uptime ping once it gets back on
            # which indicates a reboot. Also, we only allow this if the target
            # is down as well.
            if node.power_target == "Down":
                # Only on state transition
                if node.power_state == "Up" and event.state == "Down":
                    # Keep track of this timestamp Either this is
                    # Some(timestamp), indicating a previous state transition
                    # which was not followed by an uptime ping once the node
                    # came online. In this case, we ignore that here. This
                    # would mean the node did not come up again. Otherwise, if
                    # None, set the current timestamp as time of going down.
                    if node.power_managed is None:
                        # Also add an implicit uptime.
                        node.power_managed = event.timestamp
                        # While we are at it, credit uptime since last uptime
                        # event as well, as we will use this timestamp as the
                        # base for future uptime calculations. We don't have to
                        # overwrite this since future calculations will first
                        # work on the saved power_managed variable, and will
                        # have a reboot either way.
                        if node.uptime_info is not None:
                            last_reported_at, last_reported_uptime, total_uptime = (
                                node.uptime_info
                            )
                            delta = event.timestamp - last_reported_at
                            assert (
                                delta >= 0
                            ), "Power state changes can't travel back in time"
                            total_uptime += delta
                            log(
                                datetime.fromtimestamp(event.timestamp),
                                f"credited node {node.id} with {delta} seconds of uptime when node is going to sleep",
                            )
                            node.credit_uptime(
                                delta, event.timestamp, "Node is going to sleep"
                            )
                            node.uptime_info = (event.timestamp, 0, total_uptime)
                        log(
                            datetime.fromtimestamp(event.timestamp),
                            "Remembered farmer bot shutdown",
                        )

            node.power_state = event.state


def process_post_period(node, events, period):
    # Collect post-period uptime events. Violations don't matter here, those
    # will be handled next period.
    for event in events:
        if isinstance(event, NodeUptimeReported):
            current_time = event.timestamp
            reported_uptime = event.uptime
            if node.power_managed is not None and node.power_manage_boot is not None:
                time_set_down = node.power_managed
                boot_request = node.power_manage_boot
                # node got power managed to down
                time_delta = current_time - time_set_down
                assert time_delta >= 0, "uptime events can't travel back in time"
                if node.uptime_info is not None:
                    last_reported_at, last_reported_uptime, total_uptime = (
                        node.uptime_info
                    )
                    if last_reported_at > node.end_ts:
                        log(
                            f"Ignoring more than 1 farmer bot uptime event after period for node {node.id}\n"
                        )
                        continue
                else:
                    total_uptime = 0

                # Verify farmer bot boot constraints
                if (
                    current_time - reported_uptime
                ) - boot_request > MAX_POWER_MANAGER_BOOT_TIME:
                    # Mark a violation on the node.
                    node.boot_duration_violations += 1
                    log(
                        datetime.fromtimestamp(event.timestamp),
                        "Detected farmer bot boot violation for node {}, request was done at {} but node only came online at {}\n".format(
                            node.id,
                            datetime.fromtimestamp(boot_request),
                            datetime.fromtimestamp(current_time - reported_uptime),
                        ),
                    )
                # Only add uptime if node came back online in time.
                elif time_delta <= MAX_POWER_MANAGER_DOWNTIME:
                    uptime_diff = period.end - max(period.start, time_set_down)
                    if uptime_diff < 0:
                        log(
                            f"Ignoring farmer bot wakeup for node {node.id} which went down after the period ended"
                        )
                    total_uptime += uptime_diff
                    log(
                        datetime.fromtimestamp(event.timestamp),
                        f"Added {uptime_diff} seconds of uptime for node {node.id}, for farmer bot boot post period\n",
                    )
                    node.credit_uptime(
                        uptime_diff, event.timestamp, "Farmerbot post period", True
                    )

                # Clear the fact that we got power managed, if it is still the
                # case, it will be set again in the proper event handler.
                node.power_managed = None
                node.power_manage_boot = None
                node.uptime_info = (current_time, reported_uptime, total_uptime)
                # Also mark a boot
                node.boot_time = (current_time - reported_uptime, current_time)

            # We are power managed but woke up without boot request. We
            # explicitly ignore this: being put to sleep by the farmer bot
            # requires a wakeup from the farmer bot. This case also means
            # nodes just go to sleep anyhow.
            elif node.power_managed is not None and node.power_manage_boot is None:
                log(
                    datetime.fromtimestamp(event.timestamp),
                    "Ignoring boot for node {} which is power managed, but did not get a boot request from the farmer bot in the period\n".format(
                        node.id
                    ),
                )

            # We got a wakeup request from farmer bot but we are not sleeping
            # due to the farmer bot. This should not happen.
            elif node.power_managed is None and node.power_manage_boot is not None:
                log(
                    datetime.fromtimestamp(event.timestamp),
                    "Ignoring uptime for node {} after farmer bot asked for a boot while the node was not sleeping as a result of farmer bot\n".format(
                        node.id
                    ),
                )

            elif node.power_managed is None and node.power_manage_boot is None:
                if node.uptime_info is not None:
                    last_reported_at, last_reported_uptime, total_uptime = (
                        node.uptime_info
                    )
                    # only collect 1 uptime event after the period ended
                    if last_reported_at >= period.end:
                        continue

                    report_delta = current_time - last_reported_at
                    uptime_delta = reported_uptime - last_reported_uptime
                    delta_in_period = period.end - last_reported_at
                    # There are quite some situations here. Notice that due to
                    # the blockchain only producing blocks every 6 seconds, and
                    # network delay + a host of other issues, we will allow a
                    # node to report uptime with "grace period" of a minute or
                    # so in either direction.
                    #
                    # 1. uptime_delta > report_delta + GRACE_PERIOD. Node is
                    # talking rubish.
                    if uptime_delta > report_delta + UPTIME_GRACE_PERIOD_SECONDS:
                        # We need to register the violation here as we won't be
                        # able to next period (since we don't scrape points
                        # from before the period atm).
                        node.uptime_info = (current_time, reported_uptime, total_uptime)
                        log(
                            datetime.fromtimestamp(event.timestamp),
                            f"Node {node.id} reported an uptime increase of {uptime_delta} seconds, while reports are {report_delta} seconds apart. This is a violation\n",
                        )

                    # 2. The difference in uptime is within reason of the
                    # difference in report times, i.e. the node is properly
                    # reporting.
                    if (
                        uptime_delta <= report_delta + UPTIME_GRACE_PERIOD_SECONDS
                        and uptime_delta >= report_delta - UPTIME_GRACE_PERIOD_SECONDS
                    ):
                        # check skew
                        if node.boot_time is not None:
                            boot, detected = node.boot_time
                            new_boot = current_time - reported_uptime
                            if abs(new_boot - boot) >= CLOCK_SKEW_INTERVAL:
                                # This is a violation
                                log(
                                    datetime.fromtimestamp(event.timestamp),
                                    "Node {} has a detected clock skew of {} seconds, more than the allowed {} seconds\n".format(
                                        node.id,
                                        abs(new_boot - boot),
                                        CLOCK_SKEW_INTERVAL,
                                    ),
                                )
                        else:
                            exit(
                                "Panic! Node does not have boot time but does have uptime"
                            )

                        # It is technically possible for the delta to be less
                        # than 0 and within the expected time frame. If nodes
                        # boot, send uptime, then immediately reboot that is
                        # possible. In those cases, handle that below, as that
                        # is the reboot detection.
                        if uptime_delta > 0:
                            # Simply add the uptime delta. If this is too large
                            # or low by a couple of seconds it will be
                            # corrected by the next pings anyhow.
                            #
                            # Make sure we don't add too much based on the
                            # period.
                            credit = min(
                                delta_in_period,
                                (
                                    NODE_UPTIME_REPORT_INTERVAL_SECONDS
                                    + UPTIME_GRACE_PERIOD_SECONDS
                                ),
                            )
                            total_uptime += credit
                            if credit != delta_in_period:
                                log(
                                    datetime.fromtimestamp(event.timestamp),
                                    f"credited node {node.id} with {credit} seconds of uptime, less than the reported {delta_in_period} seconds as the gap is too big\n",
                                )
                                node.credit_uptime(
                                    credit,
                                    event.timestamp,
                                    "Less than reported, gap is too big. Possibly scaled to period end",
                                    True,
                                )
                            else:
                                log(
                                    datetime.fromtimestamp(event.timestamp),
                                    f"credited node {node.id} with {credit} seconds of reported uptime\n",
                                )
                                node.credit_uptime(
                                    credit,
                                    event.timestamp,
                                    "Possibly scaled to period end",
                                    True,
                                )

                            node.uptime_info = (
                                current_time,
                                reported_uptime,
                                total_uptime,
                            )
                            continue

                    # 3. The difference in uptime is too low. Again there are
                    # multiple scenarios. Either way we consider the node
                    # rebooted. Depending on the reported uptime, the node
                    # reports legit uptime, or it reports an uptime which is
                    # too high.
                    #
                    #    1. Uptime is within bounds.
                    if reported_uptime <= report_delta:
                        # Account for the fact that we are actually out of the period
                        out_of_period = current_time - period.end
                        if out_of_period < reported_uptime:
                            credit = min(
                                reported_uptime - out_of_period,
                                (
                                    NODE_UPTIME_REPORT_INTERVAL_SECONDS
                                    + UPTIME_GRACE_PERIOD_SECONDS
                                ),
                            )
                            total_uptime += credit
                            if (reported_uptime - out_of_period) != credit:
                                log(
                                    datetime.fromtimestamp(event.timestamp),
                                    "credited node {} with {} seconds of uptime after a reboot, less than the reported {} seconds as the gap is too big\n".format(
                                        node.id, credit, reported_uptime - out_of_period
                                    ),
                                )
                                node.credit_uptime(
                                    credit,
                                    event.timestamp,
                                    "Less than reported, gap is too big. Possibly scaled to period end",
                                    True,
                                )
                            else:
                                log(
                                    datetime.fromtimestamp(event.timestamp),
                                    "credited node {} with {} seconds of reported uptime after a reboot\n".format(
                                        node.id, credit
                                    ),
                                )
                                node.credit_uptime(
                                    credit,
                                    event.timestamp,
                                    "Node rebooted. Possibly scaled to period end",
                                    True,
                                )
                        node.uptime_info = (current_time, reported_uptime, total_uptime)
                        node.boot_time = (current_time - reported_uptime, current_time)
                        continue

                    #    2. Uptime is actually higher than difference in
                    #    timestamp, but not high enough to be valid. This means
                    #    the node was supposedly rebooted _before_ the previous
                    #    uptime report, meaning either that report is invalid
                    #    or this report is invalid.
                    if reported_uptime > last_reported_uptime:
                        log(
                            datetime.fromtimestamp(event.timestamp),
                            f"Node {node.id} reported uptime of {reported_uptime} seconds, so time would have advanced slower on the node than in the universe\n",
                        )
                        continue

                    #    3. Uptime is too high, this is garbage
                    log(
                        datetime.fromtimestamp(event.timestamp),
                        f"Node {node.id} reported uptime of {reported_uptime} seconds, so time would have advanced faster on the node than in the universe\n",
                    )
        elif isinstance(event, PowerTargetChanged):
            log(
                "Power target changed for from {} to {}\n".format(
                    node.power_target, event.target
                )
            )
            # Remember a rising edge here to validate node actually boots. This
            # is cleared when a node sends an uptime report of a _reboot_. It
            # is allowed for this to happen if a rising edge is not consumed
            # yet, in which case the new event is ignored, as we want to
            # measure time from the first event and it is actually a good idea
            # to send multiple of these if the node does not react. Of course,
            # we also only want to track this if the node is currently power
            # managed. While we shouldn't try to boot an online node, there is
            # no _real_ harm in doing it anyway.
            if event.target == "Up" and node.power_state == "Down":
                # Only remember the first boot request.
                if not node.power_manage_boot:
                    node.power_manage_boot = event.timestamp
                    log("Remembered boot request time for node {}\n".format(node.id))
            node.power_target = event.target

            # Technically this is not needed since we don't care for actual
            # state changes after the period. After all, we only use this to
            # arm a trigger to catch farmerbot wakes up. This trigger is set
            # when the node goes from up to down, and in doing so it also sends
            # an uptime report to chain. Since we are post period now, if the
            # node goes to sleep now its sleep time won't influence the current
            # period. Regardless add this code here so we can keep track of
            # state changes and rely on the fact that we only allow 1 uptime
            # post period to do the proper thing.

        elif isinstance(event, PowerStateChanged):
            log(
                "Power state changed from {} to {}\n".format(
                    node.power_state, event.state
                )
            )
            # Add exception to allow node 1 uptime ping once it gets back on
            # which indicates a reboot. Also, we only allow this if the target
            # is down as well. if node_power.target == Power::Down { Only on
            # state transition
            if node.power_state == "Up" and event.state == "Down":
                # Either this is Some(timestamp), indicating a previous state
                # transition which was not followed by an uptime ping once the
                # node came online. In this case, we ignore that here. This
                # would mean the node did not come up again. Otherwise, if
                # None, set the current timestamp as time of going down.
                if node.power_managed is None:
                    # Also add an implicit uptime.
                    node.power_managed = event.timestamp
                    log("Remembered farmer bot shutdown\n")
                    node.power_state = event.state


# At this point we are done fetching events. Note that for the case of power
# manager boot requests, we haven't checked the case where the node does not
# respond at all. We already fetched a days worth of blocks after the period
# ended, and don't keep track of power on requests there. So any leftover
# requests here are already a day old, which is way too much. So if any node
# has an outstanding power on request here, mark a boot failure.
#
# On top of this, if a node has more than the allowed amount of boot failures,
# stick a violation on them if there isn't anohter one already.
def final_check(node, start_block_ts, end_block_ts):
    # First see if we need to mark another failure to boot in time.
    if node.power_manage_boot is not None:
        boot_request = node.power_manage_boot
        # Ignore if this is the same as start, no need to slap a violation on
        # what is likely a dead node.
        if boot_request == start_block_ts:
            log(
                "Not giving node {} a slow boot violation since it never tried to boot in the first place\n".format(
                    node.id
                )
            )
        elif boot_request > end_block_ts:
            # Boot request (and possible failure) is entirely past the current period so we
            # reserve the violation for next minting.
            log(
                "Not giving node {} a slow boot violation since the wakup request happened post period\n".format(
                    node.id
                )
            )
        else:
            node.boot_duration_violations += 1
            log(
                "Detected farmer bot boot violation for node {}, request was done at {} but node never booted\n".format(
                    node.id, datetime.fromtimestamp(boot_request)
                )
            )

    if node.power_managed is not None:
        log(
            "Node was asleep at end of period. Time elapsed from shutdown to period end is:",
            end_block_ts - node.power_managed,
        )

    # Then slap on a violation if needed
    if node.boot_duration_violations > MAX_ALLOWED_BOOT_VIOLATIONS:
        log(
            "Node got a violation for failing to wake within allowed boot time. Instances: {}".format(
                node.boot_duration_violations
            )
        )


NodeUptimeReported = collections.namedtuple(
    "NodeUptimeReported", "uptime, timestamp, event_index"
)
PowerTargetChanged = collections.namedtuple(
    "PowerTargetChanged", "target, timestamp, event_index"
)
PowerStateChanged = collections.namedtuple(
    "PowerStateChanged", "state, timestamp, event_index"
)


class MintingNode:
    def __init__(self, node_id, period, verbose=False, grace_periods=[]):
        self.id = node_id
        self.period = period
        self.end_ts = period.end
        self.verbose = verbose
        self.uptime_info = None
        self.boot_time = None
        self.boot_duration_violations = 0

        self.power_target = None
        self.power_state = None
        self.power_managed = None
        self.power_manage_boot = None

        # This is probably always the same as the "last_reported_at" value used
        # in minting, but we track it separately here to avoid disturbing the
        # code to make sure it's set before we need it
        self.last_uptime_added_ts = period.start
        self.uptime = 0
        self.downtime = 0
        self.events = []

        self.grace_periods = []
        if grace_periods:
            for grace_period in grace_periods:
                # Each grace period is like a mini minting period
                grace_period = {
                    'start': grace_period[0],
                    'end': grace_period[1],
                    'seconds_set': grace_period[2],
                    'name': grace_period[3],
                    'uptime': 0,
                    'boot_violations': 0,
                    'events': []
                }
                self.grace_periods.append(grace_period)

    def credit_uptime(self, uptime, timestamp, note="", post_period=False):
        self.uptime += uptime
        # We scale our elapsed time the same as how uptime is scaled after the
        # period ends, so that we don't generate too much downtime
        if post_period:
            elapsed = self.end_ts - self.last_uptime_added_ts
        else:
            elapsed = timestamp - self.last_uptime_added_ts
        # "Downtime" can be slightly negative sometimes and still be valid. We
        # track this mostly as a sanity check, because it should always be
        # equal to the period duration minus the uptime
        downtime = elapsed - uptime
        self.downtime += downtime

        if self.verbose:
            log(
                datetime.fromtimestamp(timestamp),
                "Seconds elapsed since last uptime added:",
                elapsed,
                "Missing uptime:",
                downtime,
                note,
            )

        event = [
            datetime.fromtimestamp(timestamp),
            timestamp,
            uptime,
            elapsed,
            downtime,
            note,
        ]
        self.events.append(event)

        self.last_uptime_added_ts = timestamp

        # Grace period handling
        # A grace period is range of time during which we want to give all nodes
        # full uptime credit. These would be periods where some outage or
        # problem affected the ability of some or all nodes to create uptime
        # reports. We want to find the amount of time contained in this uptime
        # credit which is also contained in the grace period. The idea is that
        # once we know the total uptime that the node managed to accrue during
        # the grace period, if any, then we can calculate the downtime over that
        # period and apply an additional credit. We also make note of the events
        # that generated this uptime, in case inspecting them later is of
        # interest. Rather than dealing with the additional complication of
        # calculating downtime here, we'll do it all at once at the end
        if self.grace_periods:
            credit_seconds_set = set(range(timestamp - uptime, timestamp))
            for grace_period in self.grace_periods:
                overlap = grace_period['seconds_set'] & credit_seconds_set
                if overlap:
                    grace_period['uptime'] += max(overlap) - min(overlap)
                    grace_period['events'].append(event)

    def write_csv(self, path=None):
        if path is None:
            path = f"./node_{self.id}.csv"
        with open(path, "w", newline="") as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(
                [
                    "Date",
                    "Timestamp",
                    "Uptime credited",
                    "Elapsed time",
                    "Downtime",
                    "Note",
                ]
            )
            for event in self.events:
                csv_writer.writerow(event)


def get_events(con, node_id, start, end):
    uptimes = con.execute(
        "SELECT uptime, timestamp, event_index FROM NodeUptimeReported WHERE node_id=? AND timestamp>=?  AND timestamp<=?",
        (node_id, start, end),
    ).fetchall()
    targets = con.execute(
        "SELECT target, timestamp, event_index FROM PowerTargetChanged WHERE node_id=? AND timestamp>=?  AND timestamp<=?",
        (node_id, start, end),
    ).fetchall()
    states = con.execute(
        "SELECT state, timestamp, event_index FROM PowerStateChanged WHERE node_id=? AND timestamp>=? AND timestamp<=?",
        (node_id, start, end),
    ).fetchall()

    events = []
    events.extend([NodeUptimeReported(*u) for u in uptimes])
    events.extend([PowerStateChanged(*s) for s in states])
    events.extend([PowerTargetChanged(*t) for t in targets])

    events = sorted(events, key=lambda e: (e.timestamp, e.event_index))
    return events


def check_node(con, node_id, period, logging_mode=None, log_file=None, grace_periods=[]):
    # Just making the globals assignment explicit here This is a temporary
    # solution, of course ;)
    globals()["logging_mode"] = logging_mode
    globals()["log_file"] = log_file

    # We assume this one is only run on finished minting periods
    #
    # Since we only fetch initial power configs for the beginning of each
    # period, there's no risk of fetching the wrong one unless we're off by a
    # month. On the other hand, getting the exact timestamp of the block or the
    # block number is relatively expensive, so we use a bit of a hack here.
    # Maybe a better approach is caching the period start/end info inside the
    # db
    initial_power = con.execute(
        "SELECT state, down_time, target, timestamp FROM PowerState WHERE node_id=? AND timestamp>=?  AND timestamp<=?",
        [node_id, (period.start - PERIOD_CATCH), (period.start + PERIOD_CATCH)],
    ).fetchone()

    # If there's no entry in the db, it would mean either the node was not
    # created yet at this point in time (thus the default value), or the
    # fetching of this data is not completed. The latter case is potentially
    # problematic, but as long as we get the data eventually, we will catch any
    # associated violations eventually too
    if initial_power is None:
        initial_power = "Up", None, "Up", None
    state, down_time, target, timestamp = initial_power

    if state == "Down":
        # This is now using the same approach as minting (that is, we only care
        # about the actual time the node went to sleep, not when a boot was
        # requested if it happened in the previous minting period). While maybe
        # not immediately obvious, we need the time the node went to sleep here
        # to correctly check if the boot time is greater below
        power_managed = down_time
        if target == "Up":
            power_manage_boot = timestamp  # Block time of first block in period
        else:
            power_manage_boot = None
    else:
        power_managed = None
        power_manage_boot = None

    period_events = get_events(con, node_id, period.start, period.end)
    post_period_events = get_events(
        con, node_id, period.end + 1, period.end + POST_PERIOD
    )

    node = MintingNode(node_id, period, grace_periods=grace_periods)
    node.power_target = target
    node.power_state = state
    node.power_managed = power_managed
    node.power_manage_boot = power_manage_boot

    process_period(node, period_events, period)
    process_post_period(node, post_period_events, period)
    final_check(node, timestamp, period.end)  # Should be end block time
    return node


# Legacy code from the original CLI script version. Would need to be
# updated to account for the change in CSV output

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser()
#     parser.add_argument("node_id", help="Specify the node id to check", type=int)
#     parser.add_argument(
#         "-f",
#         "--file",
#         help="Specify the database file name.",
#         type=str,
#         default="tfchain.db",
#     )
#     parser.add_argument("-c", "--csv", help="Generate a csv file", action="store_true")
#
#     args = parser.parse_args()
#     con = sqlite3.connect(args.file)
#     period = Period(offset=Period().offset - 1)
#     node = check_node(con, args.node_id, period, args.csv)
