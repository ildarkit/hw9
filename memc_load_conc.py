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
SENTINEL = bytes('quit_task', encoding='utf-8')
ERROR_THRESHOLD = 0.01


class Worker(threading.Thread):

    def __init__(self, in_queue, out_queue, counters, tasks_size=1024):
        super().__init__(daemon=True)
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.counters = counters
        self.all = 0
        self.errors = 0
        self.tasks = {}
        self.tasks_size = tasks_size

    def run(self):

        while True:
            try:
                task = self.in_queue.get_nowait()
            except queue.Empty:
                continue

            if task == SENTINEL:
                self.counters.put((self.all, self.errors))
                self.in_queue.task_done()
                logging.info('Read statistics for {} - {}: total processed {}, errors {}'.format(
                    mp.current_process().name,
                    threading.current_thread().name,
                    self.all,
                    self.errors
                ))
                if self.tasks:
                    self.out_queue.put(self.tasks)
                self.out_queue.put(SENTINEL)
                break
            else:
                self.all += 1
                apps = self.parse_appsinstalled(task)
                if not apps:
                    self.errors += 1
                    continue
                key, packed = self.serialize(apps)
                self.tasks[key] = packed

                if len(self.tasks) == self.tasks_size:
                    self.out_queue.put(self.tasks)
                    self.tasks.clear()

    def serialize(self, appsinstalled):
        ua = appsinstalled_pb2.UserApps()
        ua.lat = appsinstalled.lat
        ua.lon = appsinstalled.lon
        key = "{}:{}".format(appsinstalled.dev_type, appsinstalled.dev_id)
        ua.apps.extend(appsinstalled.apps)

        packed = ua.SerializeToString()

        return key, packed

    def parse_appsinstalled(self, line):
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
            logging.info("Not all user apps are digits: `{}`".format(line))
        try:
            lat, lon = float(lat), float(lon)
        except ValueError:
            logging.info("Invalid geo coords: `{}`".format(line))
        return AppsInstalled(dev_type, dev_id, lat, lon, apps)


class MemcWorker(threading.Thread):

    def __init__(self, out_queue, counters, addr, dry, socket_timeout=2, attempts=0):
        super().__init__()
        self.out_queue = out_queue
        self.counters = counters
        self.addr = addr
        self.attempts = attempts
        self.dry = dry
        self.all = 0
        self.errors = 0
        self.memc_client = memcache.Client((addr,), socket_timeout=socket_timeout)

    def run(self):

        while True:
            try:
                task = self.out_queue.get_nowait()
            except queue.Empty:
                continue

            if task == SENTINEL:
                self.counters.put((self.all, self.errors))
                self.out_queue.task_done()
                break
            else:
                self.all += 1
                if self.dry:
                    logging.debug("{} - {} -> {}".format(self.addr, *task))
                elif not self.memc_write(task):
                    self.errors += 1

    def memc_write(self, task):
        counter = self.attempts if self.attempts > 0 else 1
        result = False

        while counter:
            if self.attempts > 0 and counter > 0:
                counter -= 1
            try:
                result = self.memc_client.set_multi(task)
            except Exception as err:
                logging.exception(
                    "An unexpected error occurred while writing to memc {}: {}".format(self.addr, err)
                )
                break
            if result:
                break
            elif counter == 0:
                result = False
                logging.error("Cannot write to memc {}".format(self.addr))

        return result


def start_workers_for_memc(queues, options):
    dry = options.dry
    socket_timeout = options.socket_timeout
    attempts = options.attempts
    workers = []
    counters = queue.Queue()
    for out_queue, addr in queues:
        for _ in range(options.workers):
            worker = MemcWorker(
                out_queue, counters,
                addr, dry, socket_timeout, attempts
            )
            workers.append(worker)

    for worker in workers:
        worker.start()

    for worker in workers:
        worker.join()

    _all = errors = 0
    while not counters.empty():
        result = counters.get()
        _all += result[0]
        errors += result[1]

    if _all:
        error_ratio = errors / _all
        if error_ratio > ERROR_THRESHOLD:
            logging.info("Many write errors occurred: {} > {}".format(error_ratio, ERROR_THRESHOLD))


def put_to_queue(path, in_queue):
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
            if dev_type not in in_queue:
                errors += 1
                logging.error("Unknown device type: {}".format(dev_type))
                continue
            in_queue[dev_type].put(line)

    return collections.namedtuple('Counters', ('all', 'errors'))(_all, errors)


def dispatcher(args):
    path, in_queue, out_queue = args
    workers = []
    counters = queue.Queue()

    for dev_type, in_q in in_queue.items():
        worker = Worker(in_q, out_queue[dev_type], counters)
        workers.append(worker)

    for worker in workers:
        worker.start()

    processed = put_to_queue(path, in_queue)

    for dev_type in in_queue:
        in_queue[dev_type].put(SENTINEL)

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
            logging.info("Many read errors occurred: {} > {}".format(error_ratio, ERROR_THRESHOLD))

    return path


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    to = os.path.join(head, "." + fn)
    os.rename(path, to)
    return to


def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }

    thread_args = []
    in_queue = {}
    out_queue = {}
    manager = mp.Manager()
    for dev_type, addr in device_memc.items():
        in_queue[dev_type] = manager.Queue()
        out_queue[dev_type] = manager.Queue()

        thread_args.append((out_queue[dev_type], addr, ))

    # в отдельном процессе запускаются треды для записи
    # в мемкеши соответсвующих устройств device_memc
    mp.Process(
        target=start_workers_for_memc,
        args=(thread_args, options)
    ).start()

    proc_args = []
    for path in glob.iglob(options.pattern):
        proc_args.append((path, in_queue, out_queue))
    proc_args = sorted(proc_args, key=lambda arg: arg[0])

    # пул процессов, в каждом из которых запускаются
    # len(device_memc) потоков
    proc_pool = mp.Pool(options.workers)
    for path in proc_pool.imap(dispatcher, proc_args):
        to = dot_rename(path)
        logging.info('File {} was renamed to {}'.format(path, to))


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
    op.add_option("-s", "--socket_timeout", action="store", default=2, type="int")
    op.add_option("-a", "--attempts", action="store", default=0, type="int")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: {}".format(opts))
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: {}".format(e))
        sys.exit(1)
