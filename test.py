import queue
import unittest
from optparse import OptionParser

import memcache

import appsinstalled_pb2
from memc_load_conc import MemcWorker, AppsInstalled
from memc_load_conc import parse_appsinstalled, serialize


def cases(test_cases):
    def decorator(method):
        def wrapper(self):
            for test in test_cases:
                method(self, test)

        return wrapper

    return decorator


class SerializeTestCase(unittest.TestCase):

    @cases((
            'idfa\te7e1a50c0ec2747ca56cd9e1558c0d7c\t67.7835424444\t-22.8044005471\t7942,8519,4232,3032,4766,9283,5682',
            'idfa\tf5ae5fe6122bb20d08ff2c2ec43fb4c4\t-104.68583244\t-51.24448376\t4877,7862,7181,6071,2107,2826,2293',
            'idfa\t3261cf44cbe6a00839c574336fdf49f6\t137.790839567\t56.8403675248\t7462,1115,5205,6700,865,5317,4967',
    ))
    def test_serialize_appsinstalled(self, line):
        appsinstalled = parse_appsinstalled(line)
        key, packed = serialize(appsinstalled)

        dev_type, dev_id, lat, lon, raw_apps = line.strip().split()
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        key1 = "{}:{}".format(dev_type, dev_id)
        packed1 = ua.SerializeToString()

        self.assertEqual({key, packed}, {key1, packed1})


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
        out_queue = {'idfa': None}
        cls.worker = MemcWorker(out_queue, counters, opts.idfa, attempts=2)

        cls.client = memcache.Client((opts.idfa, ), socket_timeout=2, debug=1, dead_retry=3)

    @cases((
            'idfa\te7e1a50c0ec2747ca56cd9e1558c0d7c\t67.7835424444\t-22.8044005471\t7942,8519,4232,3032,4766,9283,5682',
            'idfa\tf5ae5fe6122bb20d08ff2c2ec43fb4c4\t-104.68583244\t-51.24448376\t4877,7862,7181,6071,2107,2826,2293',
            'idfa\t3261cf44cbe6a00839c574336fdf49f6\t137.790839567\t56.8403675248\t7462,1115,5205,6700,865,5317,4967',
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

        self.worker.memc_write({key: packed})

        self.assertEqual(self.client.get(key), packed)