#!/usr/bin/env python3
"""Serve glidein capacity as prometheus metrics.

the gauges are read at scrape time from a snapshot dict, which the refresh loop swaps in whole under a lock. 
nothing is cleared and repopulated in place, so a scrape cannot land mid-refresh and see a partial pool.

usage:
    condor_status_to_prometheus.py --collector grid:glidein-cm.icecube.wisc.edu
    curl -s localhost:9100/metrics
"""

import logging
import threading
import time
from optparse import OptionParser

import prometheus_client
from prometheus_client.core import GaugeMetricFamily
from prometheus_client import Gauge

from condor_capacity import LABELS, SLOT_LABELS, summarize
from condor_utils import read_status_from_collector

HELP = {
    "cores_unclaimed": "Cores free on partitionable slots. (to get approximate usable capacity subtract cores_stranded)",
    "cores_claimed": "Cores held by dynamic slots",
    "cores_stranded": "Cores on partitionable slots that remaining memory cannot back at 4 GiB per core",
    "memory_unclaimed_bytes": "Memory free on partitionable slots",
    "memory_claimed_bytes": "Memory held by dynamic slots",
    "memory_stranded_bytes": "Memory on partitionable slots that remaining cores cannot use at 4 GiB per core",
    "gpus_unclaimed": "Gpus free on partitionable slots",
    "gpus_claimed": "Gpus held by dynamic slots",
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


def parse_collectors(specs):
    """[name:address, ...] -> [(name, address), ...], failing loud on bad input."""
    pairs = []
    for spec in specs:
        name, sep, address = spec.partition(":")
        if not sep or not name or not address:
            raise ValueError(f"expected name:address, got {spec!r}")
        pairs.append((name, address))
    return pairs


def fetch(pairs):
    """[(pool, ad), ...] and the set of pools that answered.

    a pool that raises is left out of both, so the caller can keep its previous
    numbers rather than publishing an empty pool as if it were real.
    """
    ads = []
    ok = set()
    for pool, address in pairs:
        logging.info("querying %s (%s)", pool, address)
        try:
            found = list(read_status_from_collector(address,
                                                    slot_constraint=None,
                                                    raise_on_error=True))
        except Exception:
            logging.exception("%s: query failed, keeping previous values", pool)
            continue
        logging.info("%s: %d ads", pool, len(found))
        ads.extend((pool, ad) for ad in found)
        ok.add(pool)
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
            "condor_capacity_slots", "Glidein slots advertised to the collector",
            labels=list(SLOT_LABELS))

        for key, values in snapshot.items():
            labels = [getattr(key, name) for name in LABELS]
            for name, family in by_measure.items():
                if name in values:
                    family.add_metric(labels, values[name])
            slots.add_metric([getattr(key, name) for name in SLOT_LABELS],
                             values.get("slots", 0))

        yield from by_measure.values()
        yield slots


def refresh(collector, pairs, interval):
    """query, summarize, swap. runs until the process exits."""
    while True:
        start = time.time()
        try:
            ads, ok = fetch(pairs)
            snapshot = summarize(ads)
            collector.replace(snapshot, ok)
        except Exception:
            logging.exception("refresh failed, keeping the previous snapshot")
        else:
            now = time.time()
            for pool in ok:
                LAST_SUCCESS.labels(pool=pool).set(now)
            logging.info("refreshed %d of %d pool(s), %d label set(s), %.0fs",
                         len(ok), len(pairs), len(snapshot), now - start)

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
    pairs = parse_collectors(options.collectors)

    if options.dry_run:
        ads, ok = fetch(pairs)
        logging.info("answered: %s", ", ".join(sorted(ok)) or "none")
        for key, values in sorted(summarize(ads).items()):
            measures = " ".join(f"{k}={v:.0f}" for k, v in sorted(values.items()))
            print(f"{key.pool:<6} {key.site:<16} {key.type:<12} {key.state:<10} "
                  f"{key.activity:<12} {measures}")
        return

    prometheus_client.REGISTRY.unregister(prometheus_client.GC_COLLECTOR)
    prometheus_client.REGISTRY.unregister(prometheus_client.PLATFORM_COLLECTOR)
    prometheus_client.REGISTRY.unregister(prometheus_client.PROCESS_COLLECTOR)

    collector = SnapshotCollector()
    prometheus_client.REGISTRY.register(collector)

    # start_http_server runs the scrape server on its own thread, so the refresh loop
    # below is free to block on the collector query.
    prometheus_client.start_http_server(options.port)
    logging.info("serving /metrics on port %d", options.port)

    refresh(collector, pairs, options.interval)


if __name__ == "__main__":
    main()
