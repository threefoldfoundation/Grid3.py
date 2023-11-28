"""
Prints a report in Markdown with statistics about the ThreeFold Grid v3

The output is posted weekly here:
https://forum.threefold.io/t/grid-stats-new-nodes-utilization-overview/3291/90

When the file is first executed, it will ask for the last week's total numbers.
The parser expects copying and pasting the rendered html, rather than the
original markdown. This probably has some platform specific quirks, but you can
just skip it and the percent changes will be omitted.

One subtler point here is that Grid capacity falls into three categories: total
capacity, rented capacity, and capacity reserved by Zos. When computing
utilization figures, we subtract the Zos reserved amounts from the totals,
because these amounts cannot be rented. Our utilization percentages compare
rented capacity versus capacity available to rent.

There is also some mixing of decimal and binary forms, as astute readers may
notice. We call a GB 1024 bytes for the purpose of Zos reservations, because it
is accurate to how Zos behaves. In the case of printing total figures, we call a
GB 1000 bytes for the same reason that disk manufacturers do :)
"""

import time
import grid3.network
from grid3.types import Node

resource_types = ["cru", "mru", "sru", "hru"]

def get_nodes(graphql):
    """
    We fetch all nodes with signs of life in the last 36 hours. 
    Standby nodes only come online every 24 hours, and we add some wiggle room
    """
    active = int(time.time()) - 60 * 60 * 36
    fields = ["nodeID", "createdAt", "resourcesTotal", "power", "country"]
    nodes = graphql.nodes(fields, updatedAt_gt=active)
    return list(map(Node, nodes))


def get_contracts(graphql):
    return graphql.nodeContracts(["resourcesUsed"], state_eq="Created")


def capacity_total(nodes):
    resources = {resource: 0 for resource in resource_types}
    for node in nodes:
        for resource in resources:
            resources[resource] += int(node.resourcesTotal[resource])
    for resource in resources:
        if resource != "cru":
            resources[resource] /= 10**9
    return resources


def utilization_total(contracts):
    resources = {resource: 0 for resource in resource_types}
    for contract in contracts:
        if contract["resourcesUsed"]:
            for resource in contract["resourcesUsed"].items():
                resources[resource[0]] += int(resource[1])
    for resource in resources:
        if resource != "cru":
            resources[resource] /= 10**9
    return resources


def zos_resources(nodes):
    """
    Zos reserves 10% of available RAM, so this is accurate for that case. For
    SSD, the reservation is dynamic in increments of 5gb. Like Grid Proxy, we
    assume it's 20gb, which is probably a decent average for how much Zos needs
    """
    resources = {"mru": 0, "sru": 0}
    for node in nodes:
        # Zos reserves 10% of RAM with a minimum of 2gb
        resources["mru"] += max(int(node.resourcesTotal["mru"]) * 0.1, 2147483648)
        # Probably no node has less than 20gb SSD, but just in case
        resources["sru"] += min(int(node.resourcesTotal["sru"]), 21474836480)

    for resource in resources:
        resources[resource] /= 10**9

    return resources


def new_nodes(nodes):
    """
    Find nodes that were created within the last week
    """
    last_week = time.time() - 7 * 24 * 60 * 60
    return [node for node in nodes if node.createdAt > last_week]


def parse_old_totals(text):
    resources = {resource: 0 for resource in resource_types}
    for line in text:
        for label, resource in [
            ("Cores", "cru"),
            ("RAM", "mru"),
            ("SSD", "sru"),
            ("HDD", "hru"),
        ]:
            if label in line:
                resources[resource] = int(
                    line[: line.rfind(",") + 4].strip(label).replace(",", "")
                )
    return resources


def print_new_nodes(nodes):
    countries = {node.country: 0 for node in nodes}

    for node in nodes:
        countries[node.country] += 1

    print("Total new: " + str(len(nodes)))

    for country, count in countries.items():
        print("* " + country + ": " + str(count))
    print()


def print_node_totals(node_count, sleeping_count):
    print("## Total Capacity")
    print("Online nodes: {}".format(node_count - sleeping_count))
    print("Sleeping nodes: {}".format(sleeping_count))
    print("**Total nodes:** {}".format(node_count))
    print()


def print_capacity_totals(new_totals, old_totals):
    if old_totals:
        percent_change = {}
        for resource in resource_types:
            new = new_totals[resource]
            old = old_totals[resource]
            percent_change[resource] = round((new - old) / old * 100, 2)
    else:
        percent_change = {resource: "N/A" for resource in resource_types}

    print("|||Change|")
    print("| --- | --- | --- |")
    for label, resource in [
        ("Cores", "cru"),
        ("RAM", "mru"),
        ("SSD", "sru"),
        ("HDD", "hru"),
    ]:
        if label == "Cores":
            print(
                "| **{}** |{:,}| {}% |".format(
                    label, round(new_totals[resource]), percent_change[resource]
                )
            )
        else:
            print(
                "| **{}** |{:,} GB| {}% |".format(
                    label, round(new_totals[resource]), percent_change[resource]
                )
            )
    print()


def print_utilization(net, used, total, zos):
    # Here we subtract amounts reserved by Zos from totals
    print("## {} Utilization".format(net))
    print("||||")
    print("|----|----|----|")
    for label, resource in [
        ("Cores", "cru"),
        ("RAM", "mru"),
        ("SSD", "sru"),
        ("HDD", "hru"),
    ]:
        u = used[resource]
        if resource in zos:
            t = total[resource] - zos[resource]
        else:
            t = total[resource]
        if label == "Cores":
            print(
                "| **{}** | {:,} / {:,} | ({}%) |".format(
                    label, round(u), round(t), round(u / t * 100, 2)
                )
            )
        else:
            print(
                "| **{}** | {:,} / {:,} GB | ({}%) |".format(
                    label, round(u), round(t), round(u / t * 100, 2)
                )
            )
    print()


print(
    "Please input old totals, or leave blank to skip calculating percent changes (hit ctl/cmd-d to proceed)"
)
text = []
while True:
    try:
        line = input()
    except EOFError:
        break
    text.append(line)

print()
print("Retrieving data now. One moment...")
if text:
    old_capacity_totals = parse_old_totals(text)
else:
    old_capacity_totals = {}

nets = {"main": {}, "test": {}}
testnet = grid3.network.GridNetwork("test")
for net in nets:
    graphql = grid3.network.GridNetwork(net).graphql

    nodes = get_nodes(graphql)
    contracts = get_contracts(graphql)

    nets[net]["new_nodes"] = new_nodes(nodes)
    nets[net]["zos_resources"] = zos_resources(nodes)
    nets[net]["capacity_total"] = capacity_total(nodes)
    nets[net]["utilization_total"] = utilization_total(contracts)

    sleepers = [node for node in nodes if node.power and node.power["state"] == "Down"]
    nets[net]["sleepers"] = len(sleepers)
    nets[net]["total_nodes"] = len(nodes)

new_nodes = nets["main"]["new_nodes"] + nets["test"]["new_nodes"]
total_nodes = nets["main"]["total_nodes"] + nets["test"]["total_nodes"]
total_sleepers = nets["main"]["sleepers"] + nets["test"]["sleepers"]

new_capacity_totals = {
    resource: nets["main"]["capacity_total"][resource]
    + nets["test"]["capacity_total"][resource]
    for resource in resource_types
}
utilization_totals = {
    resource: nets["main"]["utilization_total"][resource]
    + nets["test"]["utilization_total"][resource]
    for resource in resource_types
}
zos_totals = {
    resource: nets["main"]["zos_resources"][resource]
    + nets["test"]["zos_resources"][resource]
    for resource in ["mru", "sru"]
}

print()
print("# Insert Dates Here")
print_new_nodes(new_nodes)
print_node_totals(total_nodes, total_sleepers)
print_capacity_totals(new_capacity_totals, old_capacity_totals)

print("# Utilization")
print_utilization(
    "Mainnet",
    nets["main"]["utilization_total"],
    nets["main"]["capacity_total"],
    nets["main"]["zos_resources"],
)
print_utilization(
    "Testnet",
    nets["test"]["utilization_total"],
    nets["test"]["capacity_total"],
    nets["test"]["zos_resources"],
)
print_utilization("Total", utilization_totals, new_capacity_totals, zos_totals)
