#!/usr/bin/env python3
"""Capacity arithmetic for glidein slots.

per slot type, `Cpus`, `Memory` and `GPUs` mean different things:
    partitionable          how much is REMAINING for new claims
    dynamic                how much that claim HOLDS

so:
    partitionable slots    -> unclaimed (remaining) and stranded
    dynamic slots          -> claimed (held)

labels come from the same slot as the numbers, 
so `claimed` carries the dynamic slot's own state and activity, not the partitionable slot's.

to estimate capacity nobody can use, we're assuming a claim needs at least 4 GiB of memory per core:
    pairable = min(remaining_cpus, remaining_memory // 4 GiB)
    stranded = remaining - pairable

this likely still underestimates unusable capacity because most jobs ask for more. 
but 4 GiB seems like a reasonable lower bound since most clusters have harware with ~4GiB of RAM per core.
"""

from typing import NamedTuple

MiB = 1024 ** 2
MIN_CLAIM_MEMORY = 4096 * MiB


class SlotKey(NamedTuple):
    """what a measure is grouped by, and the single source of truth for label names."""
    pool: str
    site: str
    state: str
    activity: str
    type: str


# every label in exposition order, for the slot count metric
SLOT_LABELS = SlotKey._fields
# capacity measures cover both slot types, so their label set omits type
LABELS = tuple(name for name in SLOT_LABELS if name != "type")


def slot_type(ad):
    """SlotType is only set for partitionable and dynamic slots. 
    static slots have no such attribute, so defaulting to static if nothing is given.
    """
    return ad.get("SlotType") or "Static"


def label_values(pool, ad):
    """the label set for one ad, no aggregation."""
    return {
        "pool": pool,
        "site": ad.get("site", "unknown"),
        "state": (ad.get("State") or "unknown").lower(),
        "activity": (ad.get("Activity") or "unknown").lower(),
    }


def remaining(ad):
    """a partitionable slot's unclaimed resources, plus the stranded split."""
    cpus = ad.get("Cpus") or 0
    mem = (ad.get("Memory") or 0) * MiB
    gpus = ad.get("GPUs") or 0

    pairable = min(cpus, mem // MIN_CLAIM_MEMORY)

    return {
        "cores_unclaimed": cpus,
        "memory_unclaimed_bytes": mem,
        "gpus_unclaimed": gpus,
        "cores_stranded": cpus - pairable,
        "memory_stranded_bytes": mem - pairable * MIN_CLAIM_MEMORY,
        "gpus_stranded": max(0, gpus - pairable),
    }


def held(ad):
    """a dynamic slot's claimed resources."""
    return {
        "cores_claimed": ad.get("Cpus") or 0,
        "memory_claimed_bytes": (ad.get("Memory") or 0) * MiB,
        "gpus_claimed": ad.get("GPUs") or 0,
    }


def summarize(slots):
    """[(pool, ad), ...] -> {SlotKey: {measure: value}}.

    partitionable slots give unclaimed and stranded, dynamic slots give claimed. 
    static slots are counted but contribute no capacity, since we do not run any for glideins.
    """
    out = {}

    for pool, ad in slots:
        kind = slot_type(ad).lower()
        if kind == "partitionable":
            values = remaining(ad)
        elif kind == "dynamic":
            values = held(ad)
        else:
            values = {}

        labels = label_values(pool, ad)
        key = SlotKey(type=kind, **labels)
        acc = out.setdefault(key, dict.fromkeys(values, 0.0))
        acc["slots"] = acc.get("slots", 0) + 1

        for name, value in values.items():
            acc[name] += value

    return out
