import queue
import unittest
from optparse import OptionParser

import memcache

import appsinstalled_pb2
from memc_load_conc import Worker, AppsInstalled


def cases(test_cases):
    def decorator(method):
        def wrapper(self):
            for test in test_cases:
                method(self, test)

        return wrapper

    return decorator


class WorkerTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        op = OptionParser()
        op.add_option("--idfa", action="store", default="127.0.0.1:33013")
        (opts, args) = op.parse_args()

        counters = queue.Queue()
        _queue = {'idfa': opts.idfa}
        cls.worker = Worker(_queue, counters, opts.idfa, attempts=2)

    @cases((
            'idfa e7e1a50c0ec2747ca56cd9e1558c0d7c 67.7835424444 -22.8044005471 7942,8519,4232,3032,4766,9283,5682,155',
            'idfa f5ae5fe6122bb20d08ff2c2ec43fb4c4 -104.68583244 -51.24448376 4877,7862,7181,6071,2107,2826,2293,3103',
            'idfa 3261cf44cbe6a00839c574336fdf49f6 137.790839567 56.8403675248 7462,1115,5205,6700,865,5317,4967,2104',
    ))
    def test_parse_appsinstalled(self, line):
        appsinstalled1 = self.worker.parse_appsinstalled(line)

        dev_type, dev_id, lat, lon, raw_apps = line.strip().split()
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)

        self.assertEqual(appsinstalled1, AppsInstalled(dev_type, dev_id, lat, lon, apps))


def has_memc_connect():
    client = memcache.Client(
        ("127.0.0.1:33013", ),
        socket_timeout=2,
        debug=1, dead_retry=3
    )
    result = client.servers[0].connect()
    return result == 1


flag_has_memc = has_memc_connect()


@unittest.skipUnless(flag_has_memc, 'Memcache tests are skipping')
class MemcacheTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        op = OptionParser()
        op.add_option("--idfa", action="store", default="127.0.0.1:33013")
        (opts, args) = op.parse_args()

        counters = queue.Queue()
        _queue = {'idfa': opts.idfa}
        cls.worker = Worker(_queue, counters, opts.idfa, attempts=2)

        cls.client = memcache.Client((opts.idfa, ), socket_timeout=2, debug=1, dead_retry=3)

    @cases((
            'idfa e7e1a50c0ec2747ca56cd9e1558c0d7c 67.7835424444 -22.8044005471 7942,8519,4232,3032,4766,9283,5682,155',
            'idfa f5ae5fe6122bb20d08ff2c2ec43fb4c4 -104.68583244 -51.24448376 4877,7862,7181,6071,2107,2826,2293,3103',
            'idfa 3261cf44cbe6a00839c574336fdf49f6 137.790839567 56.8403675248 7462,1115,5205,6700,865,5317,4967,2104',
    ))
    def test_memc_write(self, line):

        dev_type, dev_id, lat, lon, raw_apps = line.strip().split()
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)

        appsinstalled = AppsInstalled(dev_type, dev_id, lat, lon, apps)

        ua = appsinstalled_pb2.UserApps()
        ua.lat = appsinstalled.lat
        ua.lon = appsinstalled.lon
        key = "{}:{}".format(appsinstalled.dev_type, appsinstalled.dev_id)
        ua.apps.extend(appsinstalled.apps)
        packed = ua.SerializeToString()

        self.worker.memc_write(key, packed)

        self.assertEqual(self.client.get(key), packed)