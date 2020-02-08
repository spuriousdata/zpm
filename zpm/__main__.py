import os
import sys
import time
import sqlite3
from argparse import ArgumentParser, REMAINDER
from subprocess import check_output, check_call
from functools import partial

from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily, \
                                   SummaryMetricFamily, InfoMetricFamily, \
                                   REGISTRY

"""
import logging
logging.basicConfig(level=logging.DEBUG)
"""


def zlist():
    headers = ['name', 'size', 'alloc', 'free', 'ckpoint', 'expandsz', 'frag', 'cap', 'dedup', 'health', 'altroot',]

    size        = GaugeMetricFamily('zpm_pool_size', 'pool total size in bytes', labels=['pool'], unit='bytes')
    alloc       = GaugeMetricFamily('zpm_pool_alloc', 'pool used size in bytes', labels=['pool'], unit='bytes')
    free        = GaugeMetricFamily('zpm_pool_free', 'pool free size in bytes', labels=['pool'], unit='bytes')
    frag        = GaugeMetricFamily('zpm_pool_frag', 'pool fragmented percent', labels=['pool'], unit='percent')
    cap         = GaugeMetricFamily('zpm_pool_cap', 'pool capacity percent', labels=['pool'], unit='percent')
    dedup_ratio = GaugeMetricFamily('zpm_pool_dedup_ratio', 'pool dedup ratio', labels=['pool'])
    online      = GaugeMetricFamily('zpm_pool_online', 'pool is online == 1', labels=['pool'])

    for line in check_output(["/sbin/zpool", "list", "-p"]).decode('utf8').split('\n')[1:-1]:
        row = dict(zip(headers, [x.replace('%', '').replace('x', '') for x in line.split()]))
        row['online'] = int(row['health'] == 'ONLINE')

        size.add_metric([row['name']], row['size'])
        alloc.add_metric([row['name']], row['alloc'])
        free.add_metric([row['name']], row['free'])
        frag.add_metric([row['name']], row['frag'])
        cap.add_metric([row['name']], row['cap'])
        dedup_ratio.add_metric([row['name']], row['dedup'])
        online.add_metric([row['name']], row['online'])

    return (size, alloc, free, frag, cap, dedup_ratio, online)


def iostat():
    headers = ['device', 'reads', 'writes', 'read_kb', 'write_kb', 'qlen', 'transaction_duration_seconds', 'outstanding_transactions_seconds']

    reads       = CounterMetricFamily('zpm_iostat_reads', 'total reads', labels=['device'])
    writes      = CounterMetricFamily('zpm_iostat_writes', 'total writes', labels=['device'])
    read_bytes  = CounterMetricFamily('zpm_iostat_read', 'read bytes', labels=['device'], unit='bytes')
    write_bytes = CounterMetricFamily('zpm_iostat_write', 'written bytes', labels=['device'], unit='bytes')
    qlen        = GaugeMetricFamily('zpm_iostat_qlen', 'length of write queue', labels=['device'])
    trd         = CounterMetricFamily('zpm_iostat_transaction_duration', 'duration of completed transations in seconds', labels=['device'], unit='seconds')
    otrd        = CounterMetricFamily('zpm_iostat_outstanding_transactions', 'duration of outstanding transations in seconds', labels=['device'], unit='seconds')

    for line in check_output(["iostat", "-t", "da", "-x", "-I"]).decode('utf8').split('\n')[2:-1]:
        row                                     = dict(zip(headers, line.split()))
        row['reads']                            = int(float(row['reads']))
        row['writes']                           = int(float(row['writes']))
        row['read_bytes']                       = int(float(row['read_kb']) * 1024)
        row['write_bytes']                      = int(float(row['write_kb']) * 1024)
        row['qlen']                             = int(float(row['qlen']))
        row['transaction_duration_seconds']     = row['transaction_duration_seconds']
        row['outstanding_transactions_seconds'] = row['outstanding_transactions_seconds']

        reads.add_metric([row['device']], row['reads'])
        writes.add_metric([row['device']], row['writes'])
        read_bytes.add_metric([row['device']], row['read_bytes'])
        write_bytes.add_metric([row['device']], row['write_bytes'])
        qlen.add_metric([row['device']], row['qlen'])
        trd.add_metric([row['device']], row['transaction_duration_seconds'])
        otrd.add_metric([row['device']], row['outstanding_transactions_seconds'])

    return (reads, writes, read_bytes, write_bytes, qlen, trd, otrd)


def arcstats():
    stats = {
        'access_skip'              : CounterMetricFamily('zpm_arcstats_access_skip', 'Number of buffers skipped when updating the access state.'),
        'allocated'                : CounterMetricFamily('zpm_arcstats_allocated', 'Amount of memory allocated to the ARC', unit='bytes'),
        'c_max'                    : GaugeMetricFamily('zpm_arcstats_c_max', 'Max target cache size', unit='bytes'),
        'c_min'                    : GaugeMetricFamily('zpm_arcstats_c_min', 'Min target cache size', unit='bytes'),
        'c'                        : GaugeMetricFamily('zpm_arcstats_c', 'Target size of cache', unit='bytes'),
        'compressed_size'          : GaugeMetricFamily('zpm_arcstats_compressed_size', 'Compressed size of the entire ARC', unit='bytes'),
        'data_size'                : GaugeMetricFamily('zpm_arcstats_data_size', 'Number of bytes consumed by ARC buffers of type equal to ARC_BUFC_DATA.', unit='bytes'),
        'deleted'                  : CounterMetricFamily('zpm_arcstats_deleted', 'Number of times data was deleted from the ARC'),
        'demand_data_hits'         : CounterMetricFamily('zpm_arcstats_demand_data_hits', 'Hit count for demand data'),
        'demand_data_misses'       : CounterMetricFamily('zpm_arcstats_demand_data_misses', 'Miss count for demand data'),
        'demand_metadata_hits'     : CounterMetricFamily('zpm_arcstats_demand_metadata_hits', 'Hit count for demand metadata'),
        'demand_metadata_misses'   : CounterMetricFamily('zpm_arcstats_demand_metadata_misses', 'Miss count for demand metadata'),
        'hash_chain_max'           : GaugeMetricFamily('zpm_arcstats_hash_chain_max', 'Max number of hash chains'),
        'hash_chains'              : GaugeMetricFamily('zpm_arcstats_hash_chains', 'Current number of hash chains'),
        'hash_collisions'          : CounterMetricFamily('zpm_arcstats_hash_collisions', 'Hash collision count'),
        'hits'                     : CounterMetricFamily('zpm_arcstats_hits', 'Overall hit count'),
        'mfu_hits'                 : CounterMetricFamily('zpm_arcstats_mfu_hits', 'Hit count for MFU'),
        'misses'                   : CounterMetricFamily('zpm_arcstats_misses', 'Overall miss count'),
        'mru_hits'                 : CounterMetricFamily('zpm_arcstats_mru_hits', 'Hit count for MRU'),
        'mru_size'                 : GaugeMetricFamily('zpm_arcstats_mru_size', 'Size of MRU', unit='bytes'),
        'mutex_miss'               : CounterMetricFamily('zpm_arcstats_mutex_miss', 'Number of buffers that could not be evicted because the hash lock was held by another thread.'),
        'other_size'               : GaugeMetricFamily('zpm_arcstats_other_size', 'Number of bytes consumed by various buffers and structures not actually backed with ARC buffers.', unit='bytes'),
        'overhead_size'            : GaugeMetricFamily('zpm_arcstats_overhead_size', 'The amount of memory consumed by the arc_buf_ts data buffers', unit='bytes'),
        'p'                        : GaugeMetricFamily('zpm_arcstats_p', 'Target size of the MRU', unit='bytes'),
        'prefetch_data_hits'       : CounterMetricFamily('zpm_arcstats_prefetch_data_hits', 'Hit counter for prefetch data'),
        'prefetch_data_misses'     : CounterMetricFamily('zpm_arcstats_prefetch_data_misses', 'Miss counter for prefetch data'),
        'prefetch_metadata_hits'   : CounterMetricFamily('zpm_arcstats_prefetch_metadata_hits', 'Hit counter for prefetch metadata'),
        'prefetch_metadata_misses' : CounterMetricFamily('zpm_arcstats_prefetch_metadata_misses', 'Miss counter for prefetch metadata'),
        'size'                     : GaugeMetricFamily('zpm_arcstats_size', 'Actual size of the entire arc', unit='bytes'),
        'uncompressed_size'        : GaugeMetricFamily('zpm_arcstats_uncompressed_size', 'Uncompressed size of the entire ARC', unit='bytes'),
    }

    tracked_stats = stats.keys()
    for line in check_output(["sysctl", "-q", "kstat.zfs.misc.arcstats"]).decode('utf8').split('\n')[:-1]:
        k,v = line.split(':')
        k = k.replace('kstat.zfs.misc.arcstats.', '')
        if k not in tracked_stats:
            continue
        stats[k].add_metric([], v)

    return stats.values()


def crondb(dbfile):
    crons = {}
    conn = sqlite3.connect(dbfile)
    cur = None

    with conn:
        cur = conn.execute("select runfreq, cronjob, time from cron order by runfreq")

    for row in cur:
        runfreq, cronjob, timestamp = row
        if crons.get(runfreq, None) is None:
            crons[runfreq] = GaugeMetricFamily('zpm_cron_%s' % runfreq, 'Last run time of %s cron jobs' % runfreq, labels=['cronjob'])
        crons[runfreq].add_metric([cronjob], timestamp)
    return crons.values()


def createdb(args):
    print("Creating %s..." % args.DBFILE)
    conn = sqlite3.connect(args.DBFILE)
    with conn:
        cur = conn.execute("select * from sqlite_master")
        if len([x for x in cur]) == 0:
            conn.execute("create table cron (runfreq text, cronjob text, time real, primary key (runfreq, cronjob))")
            print("Done")
        else:
            print("Db already initialized")


def cron(args):
    # this will raise an exception if the return code != 0
    output = check_call(args.CMD)

    conn = sqlite3.connect(args.DBFILE)
    with conn: # transactions provide locking for writes
        conn.execute("INSERT INTO cron (runfreq, cronjob, time) values "
                     "(?, ?, ?) on conflict (runfreq, cronjob) do update "
                     "set time=excluded.time",
                     [args.RUNFREQ, args.JOBNAME, time.time()])
    if output:
        print(output.decode('utf8'))


class Collector(object):
    def __init__(self, args):
        self.call = []
        if args.iostat:
            self.call.append(iostat)
        if args.list:
            self.call.append(zlist)
        if args.arcstats:
            self.call.append(arcstats)
        if not self.call:
            self.call.append(iostat)
            self.call.append(zlist)
            self.call.append(arcstats)

        if args.crondb: # not inluded by default because it requires an argument (the db file)
            self.call.append(partial(crondb, args.crondb))

    def collect(self):
        for call in self.call:
            for stat in call():
                yield stat


def exporter(args):
    from http.server import ThreadingHTTPServer
    from prometheus_client.exposition import MetricsHandler

    REGISTRY.register(Collector(args))
    httpd = ThreadingHTTPServer((args.listen_address, args.port), MetricsHandler)
    httpd.serve_forever()


def main():
    parser = ArgumentParser()
    parser.set_defaults(func=parser.print_help)

    sp = parser.add_subparsers()

    _exporter = sp.add_parser('exporter')
    _exporter.set_defaults(func=exporter)
    _exporter.add_argument('-l', '--listen_address', help="address to listen on (default: 0.0.0.0)", default="0.0.0.0")
    _exporter.add_argument('-p', '--port', help="port to listen on (default: 9199)", default=9199, type=int)
    _exporter.add_argument('-I', '--iostat', help="monitor iostat (default: -ILA[C])", default=False, action='store_true')
    _exporter.add_argument('-L', '--list', help="monitor `zpool list` (default: -ILA[C])", default=False, action='store_true')
    _exporter.add_argument('-A', '--arcstats', help="monitor arcstats (default: -ILA[C])", default=False, action='store_true')
    _exporter.add_argument('-C', '--crondb', help="path to cron.db -- cron stats will be included in output", default=None)

    _cron = sp.add_parser('cron')
    _cron.set_defaults(func=cron)
    _cron.add_argument('RUNFREQ', help='Run frequency (gets appended to stat name)')
    _cron.add_argument('DBFILE', help='Path to create and manage sqlite3 db')
    _cron.add_argument('JOBNAME', help='Label for cronjob in stat')
    _cron.add_argument('CMD', help='Run command', nargs=REMAINDER)

    _createdb = sp.add_parser('createdb')
    _createdb.set_defaults(func=createdb)
    _createdb.add_argument('DBFILE', help='Path to create and manage sqlite3 db')

    args = parser.parse_args(sys.argv[1:])
    if args.func != parser.print_help:
        args.func(args)
    else:
        args.func()

if __name__ == '__main__':
    main()
