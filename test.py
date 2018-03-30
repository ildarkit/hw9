import queue
import unittest
from optparse import OptionParser

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

        device_memc = {
            "idfa": opts.idfa,
            "gaid": opts.gaid,
            "adid": opts.adid,
            "dvid": opts.dvid,
        }

        counters = queue.Queue()
        _queue = {'idfa': opts.idfa, 'gaid': opts.gaid, 'adid': opts.adid, 'dvid': opts.dvid}
        cls.worker = Worker(_queue, counters, opts.idfa, attempts=2)

    @cases((
        'idfa e7e1a50c0ec2747ca56cd9e1558c0d7c 67.7835424444 -22.8044005471 7942,8519,4232,3032,4766,9283,5682,155',
        'idfa f5ae5fe6122bb20d08ff2c2ec43fb4c4 -104.68583244 -51.24448376 4877,7862,7181,6071,2107,2826,2293,3103,9433',
        'gaid 3261cf44cbe6a00839c574336fdf49f6 137.790839567 56.8403675248 7462,1115,5205,6700,865,5317,4967,2104',
    ))
    def test_parse_appinstalled(self, line):
        appinstalled1 = self.worker.parse_appsinstalled(line)

        dev_type, dev_id, lat, lon, raw_apps = line.strip().split()
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)

        self.assertEqual(appinstalled1, AppsInstalled(dev_type, dev_id, lat, lon, apps))