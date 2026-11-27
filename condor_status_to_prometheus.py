#!/usr/bin/env python3
"""Serve condor slot capacity as prometheus metrics.

one process can scrape several pools. 
each `--collector name:address` names a pool, and an optional `--constraint name:expression` narrows it, 
which is how a shared pool is reduced to the capacity we control. e.g.:

    condor_status_to_prometheus.py \
        --collector glidein:glidein-cm.icecube.wisc.edu \
        --collector osg-dedicated:cm-1.ospool.osg-htc.org \
        --constraint 'osg-dedicated:regexp("icecube",GLIDECLIENT_Group,"i")'

and to check the outputs:
    curl -s localhost:9100/metrics

the gauges are read at scrape time from a snapshot dict, which the refresh loop swaps in whole under a lock. 
nothing is cleared and repopulated in place, so a scrape cannot land mid-refresh and see a partial pool.
"""

import logging
import threading
import time
from optparse import OptionParser
from typing import NamedTuple

import prometheus_client
from prometheus_client.core import GaugeMetricFamily
from prometheus_client import Gauge

from condor_capacity import LABELS, summarize
from condor_utils import read_status_from_collector

HELP = {
    "cores_unclaimed": "Cores free on partitionable and static slots. (to get approximate usable capacity subtract cores_stranded)",
    "cores_claimed": "Cores held by dynamic slots and claimed static slots",
    "cores_stranded": "Cores on partitionable slots that remaining memory cannot back at 4 GiB per core",
    "memory_unclaimed_bytes": "Memory free on partitionable and static slots",
    "memory_claimed_bytes": "Memory held by dynamic slots and claimed static slots",
    "memory_used_bytes": "Peak memory actually used by the job in a dynamic or busy static slot. absent until the starter reports it",
    "memory_stranded_bytes": "Memory on partitionable slots that remaining cores cannot use at 4 GiB per core",
    "gpus_unclaimed": "Gpus free on partitionable and static slots",
    "gpus_claimed": "Gpus held by dynamic slots and claimed static slots",
    "gpus_stranded": "Gpus on partitionable slots with no remaining core left to drive them",
}

# derived, so the metric list and the help text cannot drift apart
MEASURES = tuple(HELP)

# updated on every successful refresh, per pool, and never reset on failure. 
# a live exporter whose refresh loop has died still serves its last values, 
# and prometheus cannot tell those from fresh ones. this gauge is how we differentiate those cases.
LAST_SUCCESS = Gauge(
    "condor_capacity_last_success_timestamp_seconds",
    "Unix time of the last successful collector query",
    ["pool"],
)


class Pool(NamedTuple):
    """one pool to scrape: which collector answers for it, and how to read its ads."""

    name: str
    address: str
    constraint: str | None = None
    ours: str = "*"


def parse_specs(collectors, constraints, ours):
    """--collector, --constraint and --ours -> [Pool, ...].

    each flag takes name:value and is matched to the collector of the same name. 
    a collector without an --ours flag is wholly ours, which is true of every pool we fully own. 
    a --constraint or --ours naming an unknown collector will error loudly.
    """
    pools = []
    for spec in collectors:
        name, sep, address = spec.partition(":")
        if not sep or not name or not address:
            raise ValueError(f"expected name:address, got {spec!r}")
        pools.append(Pool(name, address))

    names = {pool.name for pool in pools}
    if len(names) != len(pools):
        raise ValueError("duplicate --collector name")

    def by_name(specs, what):
        values = {}
        for spec in specs or ():
            name, sep, value = spec.partition(":")
            if not sep or not name or not value:
                raise ValueError(f"expected name:{what}, got {spec!r}")
            if name not in names:
                raise ValueError(f"{what} for unknown collector {name!r}")
            values[name] = value
        return values

    constraint_by_name = by_name(constraints, "expression")
    ours_by_name = by_name(ours, "value")

    return [
        pool._replace(constraint=constraint_by_name.get(pool.name),
                      ours=ours_by_name.get(pool.name, "*").lower())
        for pool in pools
    ]


def fetch(pools):
    """[(pool, ad), ...] and the set of pools that answered.

    a pool that raises is left out of both, so the caller can keep its previous
    numbers rather than publishing an empty pool as if it were real.
    """
    ads = []
    ok = set()
    for pool in pools:
        logging.info("querying %s (%s)%s", pool.name, pool.address,
                     f" [{pool.constraint}]" if pool.constraint else "")
        try:
            found = list(read_status_from_collector(pool.address,
                                                    slot_constraint=pool.constraint,
                                                    raise_on_error=True))
        except Exception:
            logging.exception("%s: query failed, keeping previous values", pool.name)
            continue
        if not found:
            # a successful query returning nothing is likely a broken constraint
            # rather than an empty pool, and the freshness gauge cannot tell them apart.
            logging.warning("%s: query returned no ads%s", pool.name,
                            f" for constraint {pool.constraint}" if pool.constraint else "")
        logging.info("%s: %d ads", pool.name, len(found))
        ads.extend((pool.name, ad) for ad in found)
        ok.add(pool.name)
    return ads, ok


class SnapshotCollector:
    """serves the most recent complete snapshot, at scrape time.

    the refresh loop swaps in a whole dict under a lock, 
    so a scrape reads one consistent pool rather than whatever has been set so far.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._snapshot = {}

    def replace(self, snapshot, ok):
        with self._lock:
            kept = {key: values for key, values in self._snapshot.items()
                    if key.pool not in ok}
            kept.update(snapshot)
            self._snapshot = kept

    def collect(self):
        with self._lock:
            snapshot = self._snapshot

        by_measure = {name: GaugeMetricFamily(
            f"condor_capacity_{name}", HELP[name], labels=list(LABELS))
            for name in MEASURES}
        slots = GaugeMetricFamily(
            "condor_capacity_slots", "Slots advertised to the collector",
            labels=list(LABELS))

        for key, values in snapshot.items():
            labels = [getattr(key, name) for name in LABELS]
            for name, family in by_measure.items():
                if name in values:
                    family.add_metric(labels, values[name])
            slots.add_metric(labels, values.get("slots", 0))

        yield from by_measure.values()
        yield slots


def refresh(exporter, pools, interval):
    """query, summarize, swap. runs until the process exits."""
    ours = {pool.name: pool.ours for pool in pools}
    while True:
        start = time.time()
        try:
            ads, ok = fetch(pools)
            snapshot = summarize(ads, ours)
            exporter.replace(snapshot, ok)
        except Exception:
            logging.exception("refresh failed, keeping the previous snapshot")
        else:
            now = time.time()
            for name in ok:
                LAST_SUCCESS.labels(pool=name).set(now)
            # one snapshot entry per label set, so a collision here means prometheus
            # would reject the scrape as duplicate series. 
            # this fires if LABELS ever stops covering everything SlotKey varies on.
            emitted = {tuple(getattr(k, label) for label in LABELS) for k in snapshot}
            if len(emitted) != len(snapshot):
                logging.warning("snapshot has duplicate label sets, scrape may be rejected")
            logging.info("refreshed %d of %d pool(s), %d label set(s), %.0fs",
                         len(ok), len(pools), len(snapshot), now - start)

        elapsed = time.time() - start
        if elapsed < interval:
            time.sleep(interval - elapsed)


def main():
    parser = OptionParser("usage: %prog --collector name:address [...]")

    parser.add_option(
        "--collector",
        action="append",
        dest="collectors",
        metavar="NAME:ADDRESS",
        help="pool name and collector address, repeatable",
    )
    parser.add_option(
        "-p", "--port", default=9100, type=int,
        help="port for the prometheus exporter",
    )
    parser.add_option(
        "-i", "--interval", default=300, type=int,
        help="collector query interval in seconds",
    )
    parser.add_option(
        "-d", "--dry-run", default=False, action="store_true",
        help="query once, print the result, do not serve metrics",
    )
    parser.add_option(
        "--constraint",
        action="append",
        dest="constraints",
        metavar="NAME:EXPRESSION",
        help="condor expression to narrow one pool, repeatable. pools without one are queried unfiltered"
    )
    parser.add_option(
        "--ours",
        action="append",
        dest="ours",
        metavar="NAME:VALUE",
        help="what makes a slot ours, lowercased and matched against RemoteOwner. repeatable. defaults to '*' (the whole pool is ours)."
    )
    parser.add_option(
        "-v", "--verbose", default=False, action="store_true",
        help="debug logging",
    )
    options, _args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if options.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s : %(message)s",
    )

    if not options.collectors:
        parser.error("at least one --collector is required")
    try:
        pools = parse_specs(options.collectors, options.constraints, options.ours)
    except ValueError as exc:
        parser.error(str(exc))
    ours = {pool.name: pool.ours for pool in pools}

    if options.dry_run:
        ads, ok = fetch(pools)
        logging.info("answered: %s", ", ".join(sorted(ok)) or "none")
        for key, values in sorted(summarize(ads, ours).items()):
            measures = " ".join(f"{k}={v:.0f}" for k, v in sorted(values.items()))
            print(f"{key.pool:<14} {key.site:<16} {key.type:<12} {key.tenant:<8} "
                  f"{key.state:<10} {key.activity:<12} {measures}")
        return

    prometheus_client.REGISTRY.unregister(prometheus_client.GC_COLLECTOR)
    prometheus_client.REGISTRY.unregister(prometheus_client.PLATFORM_COLLECTOR)
    prometheus_client.REGISTRY.unregister(prometheus_client.PROCESS_COLLECTOR)

    exporter = SnapshotCollector()
    prometheus_client.REGISTRY.register(exporter)

    # start_http_server runs the scrape server on its own thread, so the refresh loop
    # below is free to block on the collector query.
    prometheus_client.start_http_server(options.port)
    logging.info("serving /metrics on port %d", options.port)

    refresh(exporter, pools, options.interval)


if __name__ == "__main__":
    main()
