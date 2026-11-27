#!/usr/bin/env python3
"""Capacity arithmetic for glidein slots.

per slot type, `Cpus`, `Memory` and `GPUs` mean different things:
    partitionable          how much is REMAINING for new claims
    dynamic                how much that claim HOLDS
    static                 the slot's whole size, in use or not

so:
    partitionable slots    -> unclaimed (remaining) and stranded
    dynamic slots          -> claimed (held)
    static slots           -> claimed when busy, unclaimed otherwise, never stranded

static slots are indivisible: condor reports `Cpus` equal to `TotalSlotCpus` whether a
job is running or not, so there is no leftover to measure and no claim to carve. a
static slot with spare cores inside it cannot host anything else, which makes counting
it as fully claimed correct for capacity purposes.

labels come from the same slot as the numbers,
so `claimed` carries the dynamic slot's own state and activity, not the partitionable slot's.

to estimate capacity nobody can use, we're assuming a claim needs at least 4 GiB of memory per core:
    pairable = min(remaining_cpus, remaining_memory // 4 GiB)
    stranded = remaining - pairable

this likely still underestimates unusable capacity because most jobs ask for more.
but 4 GiB seems like a reasonable lower bound since most clusters have hardware with ~4GiB of RAM per core.
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
    tenant: str


# every label in exposition order. 
# `type` is there to distinguis between unclaimed capacity from partitionable and static slots. 
# `tenant` is there to separate series based on who owns the machine.
LABELS = SlotKey._fields


def number(ad, key):
    """a numeric classad attribute parser, or None when it is absent or unset.

    condor reports an unset attribute as either a missing key 
    or a classad value that comes through as a string, never as a number. 
    returning None here keeps the choice of how to handle missing values to the call site.
    """
    value = ad.get(key)
    return value if isinstance(value, (int, float)) else None


def slot_type(ad):
    """SlotType getter. falls back to Static if missing """
    return ad.get("SlotType") or "Static"


def state_of(ad):
    """the ad's State, lowercased the same way the label uses it."""
    return (ad.get("State") or "unknown").lower()


def tenant_of(ad, ours):
    """icecube for slots we hold, shared for the rest.

    `ours` comes from the collector's --ours flag. 
    '*' matches every slot, which is what a pool that is wholly ours passes. 
    otherwise only a dynamic or static slot whose RemoteOwner contains the value counts. 
    partitionable slots have no RemoteOwner at all so are always shared unless ours is set to *.
    """
    if ours == "*":
        return "icecube"
    owner = (ad.get("RemoteOwner") or "").lower()
    return "icecube" if ours in owner else "shared"


def label_values(pool, ad, ours):
    """the label set for one ad, no aggregation."""
    return {
        "pool": pool,
        "site": ad.get("site", "unknown"),
        "state": state_of(ad),
        "activity": (ad.get("Activity") or "unknown").lower(),
        "tenant": tenant_of(ad, ours),
    }


def remaining(ad):
    """a partitionable slot's unclaimed resources, plus the stranded split."""
    cpus = number(ad, "Cpus") or 0
    mem = (number(ad, "Memory") or 0) * MiB
    gpus = number(ad, "GPUs") or 0

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
    """a dynamic slot's claimed resources, and how much of its memory is in use.

    MemoryUsage is the job's peak usage in MB, reported by the starter. a slot that has
    only just started a job has no job attributes yet, so it is absent for a minute or
    two: absent means no used figure at all, so sums cover only measured slots.
    """
    values = {
        "cores_claimed": number(ad, "Cpus") or 0,
        "memory_claimed_bytes": (number(ad, "Memory") or 0) * MiB,
        "gpus_claimed": number(ad, "GPUs") or 0,
    }
    # optional: an unmeasured slot contributes nothing rather than a zero
    used = number(ad, "MemoryUsage")
    if used is not None:
        values["memory_used_bytes"] = used * MiB
    return values


def static(ad):
    """a static slot's resources, as claimed or unclaimed.

    no stranded: a static slot is handed out whole, so there is no claim to carve and
    the core/memory pairing does not describe anything.
    """
    if state_of(ad) in ("claimed", "preempting"):
        return held(ad)
    return {
        "cores_unclaimed": number(ad, "Cpus") or 0,
        "memory_unclaimed_bytes": (number(ad, "Memory") or 0) * MiB,
        "gpus_unclaimed": number(ad, "GPUs") or 0,
    }


def summarize(slots, ours):
    """[(pool, ad), ...] and {pool: ours pattern} -> {SlotKey: {measure: value}}.

    partitionable slots give unclaimed and stranded. dynamic slots give claimed.
    static slots are indivisible: claimed when in use, unclaimed when not, 
    and never stranded, since there is no claim to carve and no core/memory pairing to make.
    """
    out = {}

    for pool, ad in slots:
        kind = slot_type(ad).lower()
        if kind == "partitionable":
            values = remaining(ad)
        elif kind == "dynamic":
            values = held(ad)
        elif kind == "static":
            values = static(ad)
        else:
            values = {}

        labels = label_values(pool, ad, ours.get(pool, "*"))
        key = SlotKey(type=kind, **labels)
        acc = out.setdefault(key, {})
        acc["slots"] = acc.get("slots", 0) + 1

        # accumulated lazily rather than seeded from the first ad, since a measure can be
        # present on some ads in a bucket and absent on others
        for name, value in values.items():
            acc[name] = acc.get(name, 0.0) + value

    return out
