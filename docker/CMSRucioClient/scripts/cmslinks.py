#! /bin/env python3
"""
Class definition for the distances (links) among CMS RSEs.
And script for updating the distances.
"""

import argparse
import json
import logging
import os
import re

from rucio.client import Client

DEFAULT_EXCLUDE_LINKS = (
    {'dest': {'type': 'temp'}, 'src': {}},
)

CTA_RSES = ['T0_CH_CERN_Tape']
CERN_RSES = ['T2_CH_CERN']

DEFAULT_DISTANCE_RULES = {'site': 1, 'region&country': 4, 'country': 7, 'region': 10, 'other': 13}


class LinksMatrix(object):
    """
    CMS RSE distances according to a set of rules
    """

    def __init__(self, account, auth_type=None, exclude=DEFAULT_EXCLUDE_LINKS,
                 distance=None, rselist=None):

        if distance is None:
            distance = DEFAULT_DISTANCE_RULES

        self.rcli = Client(account=account, auth_type=auth_type)

        self._get_rselist(rselist)

        self._get_matrix(distance, exclude)

    def _get_rselist(self, rselist=None):

        self.rselist = []

        if rselist is None:
            rselist = [rse['rse'] for rse in self.rcli.list_rses()]

        for rse in rselist:
            attrs = self.rcli.list_rse_attributes(rse=rse)

            try:
                self.rselist.append({
                    'rse': rse,
                    'pnn': attrs['pnn'],
                    'type': attrs['cms_type'],
                    'country': attrs['country'],
                    'region': attrs.get('region', None)
                })
            except KeyError:
                logging.warning('No expected attributes for RSE %s. Skipping',
                                rse)

    def _get_matrix(self, distance, exclude):

        matrix = {}

        self.links = {}

        for src in self.rselist:
            for dest in self.rselist:

                src_rse = src['rse']
                dest_rse = dest['rse']
                src_pnn = src['pnn']
                dest_pnn = dest['pnn']

                if dest_pnn == src_pnn:
                    link = distance['site']
                elif src['region'] and dest['region'] and src['region'] == dest['region']:
                    if src['country'] == dest['country']:
                        link = distance['region&country']
                    else:
                        link = distance['region']
                elif src_pnn in matrix and dest_pnn in matrix[src_pnn]:
                    link = distance['site'] - matrix[src_pnn][dest_pnn]
                else:
                    if src['country'] == dest['country']:
                        link = distance['country']
                    else:
                        link = distance['other']

                if src_rse not in self.links:
                    self.links[src_rse] = {}

                self.links[src_rse][dest_rse] = link

        self._filter_matrix(exclude)

    def _filter_matrix(self, exclude):

        for src in self.rselist:
            for dest in self.rselist:

                if src['rse'] == dest['rse']:
                    continue

                for rule in exclude:
                    matched = True

                    for item in rule['src']:
                        if not re.match(rule['src'][item], src[item]):
                            matched = False

                    for item in rule['dest']:
                        if not re.match(rule['dest'][item], dest[item]):
                            matched = False

                    if matched:
                        self.links[src['rse']][dest['rse']] = -1
                        break

    def update(self, overwrite=False, disable=True, dry=False, srcselect=r'\S+', dstselect=r'\S+'):
        """
        Updates distances according to what is expected
        :overwrite:   overwrite distance of the links that already exist
        :disable:     set ranking to 0 for the links that should be disabled
        :dry:         dry run
        """

        count = {'checked': [], 'created': [], 'updated': [], 'disabled': []}

        src_regex = re.compile(srcselect)
        dst_regex = re.compile(dstselect)

        for src in self.rselist:
            srse = src['rse']
            logging.info("Setting links from %s to %s other RSEs.", srse, len(self.rselist))
            for dest in self.rselist:
                drse = dest['rse']

                if srse == drse or not src_regex.match(srse) or not dst_regex.match(drse):
                    continue

                if ((srse in CTA_RSES and drse not in CERN_RSES) or (drse in CTA_RSES and srse not in CERN_RSES)):
                    self.rcli.update_distance(srse, drse, {'ranking': 0, 'distance': 0})
                    continue
                    
                count['checked'].append([srse, drse])

                # Todo.. doublecheck I'm not reversing things
                link = self.rcli.get_distance(srse, drse)

                if srse in self.links and drse in self.links[srse] and self.links[srse][drse] >= 0:
                    if not link:
                        pars = {'distance': 1, 'ranking': self.links[srse][drse]}

                        if dry:
                            logging.info("adding link from %s to %s with %s. Dry Run", srse, drse, str(pars))
                        else:
                            self.rcli.add_distance(srse, drse, pars)

                        count['created'].append([srse, drse])

                    elif link and overwrite:
                        if dry:
                            logging.info("setting distance %s for link from %s to %s. Dry run.",
                                         self.links[srse][drse], srse, drse)
                        else:
                            self.rcli.update_distance(srse, drse,
                                                      {'ranking': self.links[srse][drse],
                                                       'distance': 1
                                                       })

                        count['updated'].append([srse, drse])

                elif link and disable:
                    if dry:
                        logging.info("disabling link from %s to %s. Dry run", srse, drse)
                    else:
                        self.rcli.update_distance(srse, drse, {
                            'ranking': None,
                            'distance': None,
                        })

                    count['disabled'].append([srse, drse])

        return count


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='''CLI for updating a CMS RSE's links.''',
    )
    PARSER.add_argument('-v', '--verbose', dest='debug', action='store_true',
                        help='increase output verbosity')
    PARSER.add_argument('-t', '--dry', dest='dry', action='store_true',
                        help='only printout what would have been done')
    PARSER.add_argument('--rse', dest='rselist', help='RSE. Can be multiple, default all.',
                        action='append', default=None)
    PARSER.add_argument('--srcselect', dest='srcselect',
                        help='Regex for selecting the source RSEs.',
                        default=r'\S+')
    PARSER.add_argument('--dstselect', dest='dstselect',
                        help='Regex for selecting the destination RSEs.',
                        default=r'\S+')
    PARSER.add_argument('--distance', dest='distance', default=None,
                        help='rules for different RSE distances, default %s' %
                             json.dumps(DEFAULT_DISTANCE_RULES))
    PARSER.add_argument('--exclude', dest='exclude', default=None,
                        help='exclde rules for links, default %s' %
                             json.dumps(DEFAULT_EXCLUDE_LINKS))
    PARSER.add_argument('--account', dest='account',
                        default=os.environ['RUCIO_ACCOUNT'],
                        help='Rucio account. default RUCIO_ACCOUNT')
    PARSER.add_argument('--overwrite', dest='overwrite', action='store_true',
                        help='Overwrite distances that have changed.')
    PARSER.add_argument('--disable', dest='disable', action='store_true',
                        help='Disable links that should not be there.')

    OPTIONS = PARSER.parse_args()

    if OPTIONS.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if OPTIONS.exclude:
        OPTIONS.exclude = json.loads(OPTIONS.exclude.replace("'", '"'))
    else:
        OPTIONS.exclude = DEFAULT_EXCLUDE_LINKS

    COUNT = LinksMatrix(
        account=OPTIONS.account,
        exclude=OPTIONS.exclude,
        distance=OPTIONS.distance,
        rselist=OPTIONS.rselist,
    ).update(
        overwrite=OPTIONS.overwrite,
        disable=OPTIONS.disable,
        dry=OPTIONS.dry,
        srcselect=OPTIONS.srcselect,
        dstselect=OPTIONS.dstselect
    )

    logging.debug(str(COUNT['checked']))
    logging.debug(str(COUNT['updated']))
    logging.debug(str(COUNT['disabled']))
    logging.debug(str(COUNT['created']))

    logging.info("Link summary: updated %d, disabled %d, created %d;",
                 len(COUNT['updated']), len(COUNT['disabled']), len(COUNT['created']))
