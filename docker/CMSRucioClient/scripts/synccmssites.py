#! /bin/env python
"""
Service synchronizing the sites
"""

from __future__ import absolute_import, division, print_function

import argparse
import copy
import functools
import os
import random
import re
import string
import sys
import time
import traceback
from datetime import datetime, timedelta

import yaml
import monitor #  rucio.core.monitor
from cmsdatareplica import _replica_update
from custom_logging import logging
from instrument import timer, get_timing
from mp_custom import multiprocessing
from multiprocessing_logging import install_mp_handler
from phedex import PhEDEx
from rucio.client.client import Client
from rucio.common.exception import RucioException
from syncaccounts import SYNC_ACCOUNT_FMT
from pystatsd import Client as statsClient

DEFAULT_CONFFILE = '/etc/synccmssites.yaml'
DEFAULT_LOGFILE = '/rucio/logs'

LOADED_CONF = '/tmp/.synccmssites.yaml'

DEFAULT_PNN_SUMMARY = {
    'aborted' : 0,
    'processed': 0,
    'added': 0,
    'removed': 0,
    'updated': 0,
    'skipped': 0,
    'failed': 0,
    'timing': {}
}

DEFAULT_DATADIFF_DICT = {
    'missing': [],
    'to_remove': [],
    'to_update': [],
    'summary': {
        'tot': 0,
        'missing': 0,
        'to_remove': 0,
        'to_update': 0
    }
}

DEFAULT_PNN_CONF = {
    'pool': 1,
    'run': True,
    'dry': False,
    'chunck': 100,
    'allow_clean': False,
    'multi_das_calls': False,
    'select': [r'\S+'],
    'ignore': [],
    'min_deletions': 50,
}

DEFAULT_MAIN_CONF = {
    'sleep': 10,
    'pool': 10,
    'run': True,
    'verbosity': 'SUMMARY'
}

try: # New name
    monitor.SERVER='statsd-exporter-rucio-statsd-exporter'
    monitor.CLIENT = statsClient(host=monitor.SERVER, port=monitor.PORT, prefix=monitor.SCOPE)
except: # Old name
    monitor.SERVER = 'statsd-exporter-svc'
    monitor.CLIENT = statsClient(host=monitor.SERVER, port=monitor.PORT, prefix=monitor.SCOPE)

def _open_yaml(yamlfile, modif=None):
    """
    Opening yaml and managing parsing exceptions
    :yamlfile:  the yaml file
    """

    yaml_lock = multiprocessing.Lock()

    with yaml_lock:
        modif = modif or {}

        retry = 5

        while True:
            try:
                retry -= 1
                with open(yamlfile, 'r') as stream:
                    conf = dict(
                        dict(
                            {'default': {}},
                            **yaml.load(stream)
                        ),
                        **modif
                    )
                break
            except (TypeError, yaml.composer.ComposerError):
                if retry == 0:
                    raise
                else:
                    time.sleep(1)

        if conf is None:
            raise Exception("Problem parsing %s" % yamlfile)

    return conf


def _load_config(conffile, modif=None, starttime=None):
    """
    Gets the conf file and dumps it to the
    working copy
    :conffile:  file to be loaded
    :modif:     dictionnary with modifications

    returns the content dictionnary
    """

    starttime = starttime or datetime.now()

    try:
        conf = _open_yaml(conffile, modif)

    except yaml.parser.ParserError:
        logging.warning('Problem parsing config. Using loaded one.')
        conf = _open_yaml(LOADED_CONF)

    default = dict(DEFAULT_PNN_CONF,
                   **conf.pop('default'))
    main = dict(DEFAULT_MAIN_CONF,
                **conf.pop('main'))

    loaded = dict({'main': main}, **{
        pnn: dict(default, **dict({'rse': pnn}, **sec))
        for pnn, sec in conf.items()
    })

    loaded = {name: _run_status(sec, starttime) for name, sec in loaded.items()}

    logging.my_lvl(loaded['main']['verbosity'])

    logging.debug('Loaded conf %s from %s with modif %s',
                  loaded, conffile, modif)

    with open(LOADED_CONF, 'w') as outfile:
        yaml.dump(loaded, outfile, default_flow_style=False)

    return loaded


def _run_status(conf, starttime):
    """
    define the run status evaulating the conditional expression.
    """

    if 'run' not in conf:
        logging.error('No run key in conf')
        return conf

    run = conf['run']

    if not isinstance(run, basestring):
        return conf

    run_re = re.compile(r'(since|until|for):(\S+)')

    matched = run_re.match(run).groups()

    if matched[0] == 'since':
        run = datetime.now() > datetime.strptime(matched[1], '%Y-%m-%d:%H:%M:%S')
    elif matched[0] == 'until':
        run = datetime.now() < datetime.strptime(matched[1], '%Y-%m-%d:%H:%M:%S')
    elif matched[0] == 'for':
        run = (datetime.now() - starttime) < timedelta(seconds=int(matched[1]))

    conf['run'] = run

    return conf


def _get_config(pnn=None):
    """
    Load configuration from local file
    :pnn:      Section. If None (default) the whole conf is returned.
    """

    conf = _open_yaml(LOADED_CONF)

    if pnn is None:
        return conf

    if not pnn in conf:
        return None

    return conf[pnn]


def _ping(rcli=None):
    """
    pings the Rucio server
    """
    if rcli is None:
        rcli = Client()

    try:
        rcli.ping()

    except RucioException as exc:
        logging.verbose('Ping Exception %s, %s',
                        type(exc).__name__,
                        traceback.format_exc().replace('\n', '~~'))
        return False

    return True


def worker(func):
    """
    Create the worker wrapper.
    """
    @functools.wraps(func)
    def worker_wrapper(*args, **kwargs):
        """
        Wrapper of the worker.
        manages exceptions and logs.
        """
        if 'rds' in kwargs:
            label = 'block_sync=%(pnn)s:%(rds)s'
            level = logging.VNOTICE
        else:
            label = 'pnn_sync=%(pnn)s'
            level = logging.SUMMARY

        logging.my_fmt(label=label % kwargs)

        logging.log(level, 'Starting Task')

        try:
            ret = func(*args, **kwargs)

        #pylint: disable=broad-except
        except Exception as exc:
            logging.error('Exception %s raised: %s',
                          type(exc).__name__, traceback.format_exc().replace('\n', '~~'))
            return None

        timing = ret.pop('timing')
        if 'return' in ret:
            ret = ret['return']

        logging.log(level, 'Finished: %s; timing: %s',
                    ret, timing)

        return ret

    return worker_wrapper


@worker
@timer
def block_sync(pnn, rds, pcli, rcli):
    """
    Synchronize one rucio dataset at one rse
    :pnn:    pnn.
    :rds:    rucio dataset.
    :pcli:   phedex client.
    :rcli:   rucio client.
    """

    conf = _get_config(pnn)

    if 'block_verbosity' in conf:
        logging.my_lvl(conf['block_verbosity'])

    if not conf['run']:
        return 'aborted'

    if not _ping(rcli):
        logging.warning('Cannot Ping, aborting.')
        return 'aborted'

    with monitor.record_timer_block('cms_sync.block_sync'):
        ret = _replica_update(
            dataset=rds,
            pnn=pnn,
            rse=conf['rse'],
            pcli=pcli,
            rcli=rcli,
            dry=conf['dry'],
            monitor=monitor,
        )

    return ret


@worker
@timer
def pnn_sync(pnn, pcli):
    """
    Synchronize one rucio dataset at one rse
    :pnn:    phedex node name.
    :pcli:   phedex client.
    """
    monitor.record_counter('cms_sync.site_started')
    summary = copy.deepcopy(DEFAULT_PNN_SUMMARY)

    conf = _get_config(pnn)
    summary['conf'] = conf

    if 'verbosity' in conf:
        logging.my_lvl(conf['verbosity'])

    rcli = Client(account=SYNC_ACCOUNT_FMT % pnn.lower())

    if _pnn_abort(pnn, summary, rcli):
        return summary
# Do the loop here? with conf['multi_das']


    if conf['multi_das_calls']:
        prefixes = list(string.letters + string.digits)
    else:
        prefixes = [None]

    for prefix in prefixes:
        diff = get_node_diff(pnn, pcli, rcli, conf, prefix=prefix)
        summary['timing'].update(diff['timing'])
        diff = diff['return']
        summary['diff'] = diff['summary']

        if (diff['summary']['tot'] == diff['summary']['to_remove']) and not conf['allow_clean']:
            logging.warning('All datasets to be removed. Aborting.')
            summary['status'] = 'aborted'
            continue
#            return summary

        logging.notice("Got diff=%s, timing=%s", summary['diff'], summary['timing'])

        if _pnn_abort(pnn, summary, rcli):
            return summary

        workers = get_timing(
            _launch_pnn_workers(conf, diff, pnn,
                                pcli, rcli),
            summary['timing']
        )

    summary['workers'] = len(workers)

    logging.notice("Launched %d workers, pool size %d, timing %s", summary['workers'],
                   int(conf['pool']), summary['timing']['_launch_pnn_workers'])

    left = int(conf['chunck']) - summary['workers'] + int(conf['min_deletions'])

    if left > 0:
        workers_st = get_timing(
            _launch_pnn_workers_st(left, diff, pnn, pcli, rcli),
            summary['timing']
        )

        summary['workers_st'] = len(workers_st)

        logging.notice("Launched %d single thread workers, timing %s", summary['workers_st'],
                       summary['timing']['_launch_pnn_workers_st'])

        workers = dict(workers, **workers_st)

    _get_pnn_workers(workers, summary)
    monitor.record_counter('cms_sync.site_completed')

    summary['status'] = 'finished'

    return summary


def _pnn_abort(pnn, summary, rcli):
    """
    checking if the running flag is False
    and in case aborting.
    """
    conf = _get_config(pnn)

    if not _ping(rcli):
        logging.warning('Cannot Ping. Aborting')
        conf['run'] = False

    if not conf['run']:
        summary['status'] = 'aborted'
        return True

    return False


@timer
def get_node_diff(pnn, pcli, rcli, conf, prefix=None):
    """
    Get the diff between the rucio and phedex at a node
    :pnn:  node name
    :pcli: phedex client instance
    :rcli: rucio client instance
    :multi_das_calls: perform one DAS call for each dataset starting letter
    :filters: include and exclude filters (by default all datasets are included)

    return the list of datasets to add, remove and update
    as in DEFAULT_DATADIFF_DICT
    """
    timing = {}
    with monitor.record_timer_block('cms_sync.node_diff'):
        multi_das_calls = conf['multi_das_calls']
        select = conf['select']
        ignore = conf['ignore']

        blocks_at_pnn = get_timing(get_blocks_at_pnn(pnn, pcli, multi_das_calls, prefix=prefix), timing)
        datasets_at_rse = get_timing(get_datasets_at_rse(rcli, prefix=prefix), timing)
        diff = compare_data_lists(blocks_at_pnn, datasets_at_rse, pnn)
        _diff_apply_filter(diff, select, ignore)

        diff['timing'].update(timing)

    return diff


@timer
def _diff_apply_filter(diff, select, ignore):
    """
    Select datasets according to filters
    """

    selected = 0

    select = [re.compile(sel) for sel in select]
    ignore = [re.compile(sel) for sel in ignore]

    for item in ['missing', 'to_remove', 'to_update']:

        diff['return'][item] = [
            dset for dset in diff['return'][item] if
            any(regex.match(dset) for regex in select) and
            not any(regex.match(dset) for regex in ignore)
        ]

        diff['return']['summary'][item + '_selected'] = len(diff['return'][item])
        selected += diff['return']['summary'][item + '_selected']

    diff['return']['summary']['selected'] = selected

    return diff


@timer
def get_blocks_at_pnn(pnn, pcli, multi_das_calls=True, prefix=None):
    """
    Get the list of completed replicas of closed blocks at a site
    :pnn:  the phedex node name
    :pcli: phedex client instance

    returns a dictionnary with <block name>: <number of files>
    """

    # This is not optimal in terms of calls and time but reduces the memory footprint

    blocks_at_pnn = {}
    if prefix:
        logging.summary('Getting subset of blocks at %s beginning with %s' % (pnn, prefix))
        with monitor.record_timer_block('cms_sync.pnn_blocks_split'):
            logging.summary('Getting blocks at %s starting with %s' % (pnn, prefix))
            some_blocks_at_pnn = pcli.blocks_at_site(pnn=pnn, prefix=prefix)
            blocks_at_pnn.update(some_blocks_at_pnn)
            logging.summary('Got blocks at %s starting with %s' % (pnn, prefix))
    elif multi_das_calls:
        logging.summary('Getting all blocks at %s. Multiple %s' % (pnn, multi_das_calls))
        logging.notice('Getting blocks with multiple das calls. %s', list(string.letters + string.digits))
        for item in list(string.letters + string.digits):
            with monitor.record_timer_block('cms_sync.pnn_blocks_split'):
                logging.summary('Getting blocks at %s starting with %s' % (pnn, item))
                some_blocks_at_pnn = pcli.blocks_at_site(pnn=pnn, prefix=item)
                blocks_at_pnn.update(some_blocks_at_pnn)
                logging.summary('Got blocks at %s starting with %s' % (pnn, item))
    else:
        logging.summary('Getting all blocks at %s in one call' % pnn)
        with monitor.record_timer_block('cms_sync.pnn_blocks_all'):
            blocks_at_pnn = pcli.blocks_at_site(pnn=pnn)

    logging.summary('Got blocks at %s.' % pnn)
    return blocks_at_pnn


@timer
def get_datasets_at_rse(rcli, prefix=None):
    """
    Get the list of rucio datasets at a rse, listing the rules
    belonging to the sync account
    :rcli:  rucio client (with the sync account)

    returns a dictionnary with <dataset name>: <number of files>
    """
    with monitor.record_timer_block('cms_sync.rse_datasets'):
        retval = {
            item['name']: item['locks_ok_cnt']
            for item in rcli.list_account_rules(rcli.__dict__['account'])
            if item['expires_at'] is None and (prefix is None or item['name'].startswith(prefix))
        }
    return retval

@timer
def compare_data_lists(blocks, datasets, pnn):
    """
    Compare the list of blocks at pnn and dataset at rse
    :blocks:   list of file blocks
    :datasets: list of rucio datasets
    :pnn:      phedex node name

    return the liste of datasets to add, remove and update
    as in DEFAULT_DATADIFF_DICT
    """
    with monitor.record_timer_block('cms_sync.compare_rse_datasets'):
        ret = copy.deepcopy(DEFAULT_DATADIFF_DICT)

        dataitems = list(set(blocks.keys() + datasets.keys()))

        for dataset in dataitems:
            if dataset not in datasets:
                ret['missing'].append(dataset)
                ret['summary']['missing'] += 1

            elif dataset not in blocks:
                ret['to_remove'].append(dataset)
                ret['summary']['to_remove'] += 1

            elif blocks[dataset] != datasets[dataset]:
                logging.warning("Dataset %s at pnn %s to update", dataset, pnn)
                ret['to_update'].append(dataset)
                ret['summary']['to_update'] += 1

            ret['summary']['tot'] += 1

    return ret


@timer
def _launch_pnn_workers(conf, diff, pnn, pcli, rcli):
    """
    Launch workers for the pnn worker
    """
    size = int(conf['pool'])
    chunck = int(conf['chunck'])

    pool = multiprocessing.Pool(size)

    workers = {}
    for dataset in diff['missing']:
        if chunck == 0:
            break
        workers[dataset] = pool.apply_async(
            block_sync, (),
            {'pnn': pnn, 'rds': dataset, 'pcli': pcli, 'rcli': rcli}
        )
        chunck -= 1

    pool.close()

    return workers


@timer
def _launch_pnn_workers_st(left, diff, pnn, pcli, rcli):
    """
    Launch workers for the pnn worker single thread (remove and updates)
    """

    pool = multiprocessing.Pool(1)

    workers = {}
    for dataset in diff['to_remove'] + diff['to_update']:
        if left == 0:
            break
        workers[dataset] = pool.apply_async(
            block_sync, (),
            {'pnn': pnn, 'rds': dataset, 'pcli': pcli, 'rcli': rcli}
        )
        left -= 1

    pool.close()

    return workers


@timer
def _get_pnn_workers(workers, summary):
    """
    Gets the finished workers for the pnn worker
    """

    for dataset, work in workers.items():
        ret = work.get()
        logging.verbose("Recovered worker %s, ret %s", dataset, ret)

        _get_pnn_worker_ret(ret, summary)

    return summary


def _get_pnn_worker_ret(ret, summary):
    """
    Modify summary according to one worker return
    """
    if ret == 'aborted':
        summary['aborted'] += 1
        return
    else:
        summary['processed'] += 1

    if ret is None:
        summary['failed'] += 1

    elif ret['rule'] is not None:
        summary[ret['rule']] += 1

    elif ret['replicas']['added'] > 0 or\
        ret['replicas']['removed'] > 0:
        summary['updated'] += 1

    else:
        summary['skipped'] += 1


def _launch_workers(pool, workers, pnns, pcli):
    """
    launch pnn workers
    """
    logging.summary('Launching worker for %s', pnns)

    for pnn in pnns:
        workers[pnn] = pool.apply_async(
            pnn_sync, (), {'pnn':pnn, 'pcli': pcli}
        )


def _poll_workers(workers, pnns):
    """
    poll for finished pnn workers
    """
    logging.debug('polling workers: %s; pnns %s',
                  workers, pnns)

    done = []

    for pnn, work in workers.items():
        logging.debug("Checking worker %s", pnn)
        if work.ready():
            logging.debug("Worker %s is ready", pnn)
            work.get()
            done.append(pnn)
            pnns.append(pnn)
            logging.summary("Got worker %s and re-queued", pnn)

    for pnn in done:
        workers.pop(pnn)


def _drain_up(workers, pnns):
    """
    wait untill all pnn workers have finished.
    Requeus the rse.
    """
    logging.summary('Starting draining up')
    for pnn, work in workers.items():
        work.get()
        pnns.append(pnn)


def sync(config, logs):
    """
    Main Sync process
    """

    logging.my_logfile(logs=logs)
    logging.my_fmt(label='main_sync')
    starttime = datetime.now()
    modify = {}
    workers = {} # this is the array of running pnns
    pnns = None  # this is the array of pnn to be launched
    pool = None

    pcli = PhEDEx()

    install_mp_handler()

    conf = _load_config(config, modify, starttime)

    pnns = []

    size = conf['main']['pool']

    logging.summary('Starting')

    while conf['main']['run']:

        if pool is None:
            logging.notice('Started pool of size %d', size)
            pool = multiprocessing.NDPool(size)

        add = [pnn for pnn, sec in conf.items()
               if pnn != 'main' if sec['run']
               if pnn not in workers if pnn not in pnns]

        pnns += add

        random.shuffle(pnns)

        if not _ping():
            logging.warning('Cannot ping, not launching workers')
        else:
            _launch_workers(pool, workers, pnns, pcli)
            pnns = []

        _poll_workers(workers, pnns)

        conf = _load_config(config, modify, starttime)

        if not conf['main']['run'] or\
            conf['main']['pool'] != size:

            # trigger draining of all workers, close the pool and wait
            # for the task to be over
            conf = _load_config(config, {'default': {'run': False}}, starttime)
            _drain_up(workers, pnns)
            workers = {}
            pool.close()
            pool = None
            size = conf['main']['pool']

        else:
            time.sleep(conf['main']['sleep'])

    logging.summary('Exiting.')

    return config


if __name__ == '__main__':

    PARSER = argparse.ArgumentParser(
        description='''Service for synching rucio and phedex locality data''',
    )
    PARSER.add_argument('--config', dest='config', default=DEFAULT_CONFFILE,
                        help='Configuration file. Default %s.' % DEFAULT_CONFFILE)
    PARSER.add_argument('--logs', dest='logs', default=DEFAULT_LOGFILE,
                        help='Logs file. Default %s.' % DEFAULT_LOGFILE)
    PARSER.add_argument('--nodaemon', dest='daemon', action='store_false',
                        help='Runs in foreground.')

    OPTIONS = PARSER.parse_args()

    if OPTIONS.daemon and os.fork():
        sys.exit()

    sync(OPTIONS.config, OPTIONS.logs)
