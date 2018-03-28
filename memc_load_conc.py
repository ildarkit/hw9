#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import gzip
import glob
import queue
import logging
import threading
import collections
import multiprocessing as mp
from optparse import OptionParser

# pip install python-memcached
import memcache
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2

AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])
SENTINEL = object()
ERROR_THRESHOLD = 0.01


class Worker(threading.Thread):

    def __init__(self, queue, counters, addr, dry):
        super().__init__(daemon=True)
        self._queue = queue
        self.counters = counters
        self.addr = addr
        self.dry = dry
        self.all = 0
        self.errors = 0

    def run(self):
        conn = memcache.Client((self.addr, ))

        while True:
            try:
                task = self._queue.get_nowait()
            except queue.Empty:
                continue
            if task == SENTINEL:
                self.counters.put((self.all, self.errors))
                self._queue.task_done()
                logging.info('{} - {}: total processed {} with errors {}'.format(
                    mp.current_process().name,
                    threading.current_thread().name,
                    self.all,
                    self.errors
                ))
                break
            else:
                self.all += 1
                apps = parse_appsinstalled(task)
                if not apps:
                    self.errors += 1
                    continue
                if not insert_appsinstalled(conn, apps, self.dry):
                    self.errors += 1
                self._queue.task_done()


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc_addr, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    # @TODO persistent connection
    # @TODO retry and timeouts!
    try:
        if dry_run:
            logging.debug("%s - %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
        else:
            memc = memcache.Client([memc_addr])
            memc.set(key, packed)
    except Exception as e:
        logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
        return False
    return True


def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    args = []
    for path in glob.iglob(options.pattern):
        args.append((path, device_memc, options.dry))
    args = sorted(args, key=lambda arg: arg[0])

    proc_pool = mp.Pool(options.workers)
    for path in proc_pool.imap(dispatcher, args):
        dot_rename(path)
        logging.info('File was renamed to {}'.format(path))


def dispatcher(args):
    path, devices, dry = args
    workers = []
    _queue = {}
    counters = queue.Queue()

    for dev_type, addr in devices.items():
        _queue[dev_type] = queue.Queue()
        worker = Worker(_queue, counters, addr, dry)
        workers.append(worker)

    for worker in workers:
        worker.start()

    processed = put_to_queue(path, devices, _queue)

    for dev_type in devices:
        _queue[dev_type].put(SENTINEL)

    for worker in workers:
        worker.join()

    _all = errors = 0
    while not counters.empty():
        result = counters.get()
        _all += result[0]
        errors += result[1]

    if processed.all or _all:
        error_ratio = processed.errors + errors / processed.all + _all
        if error_ratio > ERROR_THRESHOLD:
            logging.info("Many errors occurred: {} > {}".format(error_ratio, ERROR_THRESHOLD))

    return path


def put_to_queue(path, devices, _queue):
    _all = 0
    errors = 0
    logging.info('Process file {}'.format(path))
    with gzip.open(path, mode="rt") as tracker_log:
        for line in tracker_log:
            if not line:
                continue
            line = line.strip()
            _all += 1
            dev_type = line.split(maxsplit=1)[0]
            if dev_type not in devices:
                errors += 1
                logging.error("Unknown device type: {}".format(dev_type))
                continue
            _queue[dev_type].put(line)

    return collections.namedtuple('Counters', ('all', 'errors'))(_all, errors)


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    op.add_option("-w", "--workers", action="store", default=4, type="int")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
