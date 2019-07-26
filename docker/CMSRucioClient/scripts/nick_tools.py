# these lines are all needed (if you have a grid cert) to access datasvc:
#         self._session = requests.Session()
#         home = os.path.expanduser('~/')
#         self._session.cert = (home+'.globus/usercert.pem', home+'.globus/userkey.pem')
#         self._session.verify = os.getenv('X509_CERT_DIR')
#

import json
import datetime
import os
import fnmatch
import copy
from xml.etree import ElementTree

import requests
import pandas as pd


class x509RESTSession(object):
    datasvc = "https://cmsweb.cern.ch/phedex/datasvc/json/prod"
    datasvc_xml = "https://cmsweb.cern.ch/phedex/datasvc/xml/prod"

    def __init__(self):
        self._session = requests.Session()
        home = os.path.expanduser('~/')
        # $ openssl rsa -in userkey_protected.pem -out userkey.pem; chmod 0600 userkey.pem
        self._session.cert = (home+'.globus/usercert.pem', home+'.globus/userkey.pem')
        self._session.verify = os.getenv('X509_CERT_DIR')
        # To install dynamo Let's Encrypt cert:
        # cd $X509_CERT_DIR
        # wget https://letsencrypt.org/certs/isrgrootx1.pem.txt -O isrgrootx1.pem
        # ln -s isrgrootx1.pem "$(openssl x509 -hash -noout -in isrgrootx1.pem)".0
        # wget https://letsencrypt.org/certs/letsencryptauthorityx3.pem.txt -O letsencryptauthorityx3.pem
        # ln -s letsencryptauthorityx3.pem "$(openssl x509 -hash -noout -in letsencryptauthorityx3.pem)".0

    def _fmt_dates(self, df, datecols):
        if df.size > 0:
            df[datecols] = df[datecols].apply(lambda v: pd.to_datetime(v, unit='s'))
        return df

    def dbsinfo(self, dataset):
        params = {
            'dataset_access_type': '*',
            'detail': 'true',
            'dataset': dataset,
        }
        res = self._session.get("https://cmsweb.cern.ch/dbs/prod/global/DBSReader/datasets", params=params)
        return pd.read_json(res.content)

    def lock(self, item, sites=None, expires=None, comment=None):
        '''
        http://dynamo.mit.edu/web/detox/lock/help
        Arguments:
            item: dataset or wildcard
            sites: site name or wildcard
            expires: datetime or string e.g.: '2017-12-31'
                        default: now() + 3 months
            comment: self-explanatory
        '''
        tformat = r"%Y-%m-%d"
        params = {'item': item}
        if sites is not None:
            params['sites'] = sites
        if expires is None:
            params['expires'] = (datetime.datetime.now() + datetime.timedelta(days=90)).strftime(tformat)
        elif isinstance(expires, datetime.datetime):
            params['expires'] = expires.strftime(tformat)
        else:
            params['expires'] = expires
        if comment is not None:
            params['comment'] = comment
        res = self._session.get("https://dynamo.mit.edu/data/detox/lock/lock", params=params)
        rcontent = json.loads(res.content)
        if rcontent['result'] == 'OK':
            with open("locklog.json", "a") as fout:
                fout.write(res.content.decode('ascii'))
        return rcontent

    def unlock(self, lockid):
        params = {'lockid': lockid}
        res = self._session.get("https://dynamo.mit.edu/data/detox/lock/unlock", params=params)
        rcontent = json.loads(res.content)
        if rcontent['result'] == 'OK':
            with open("locklog.json", "a") as fout:
                fout.write(res.content.decode('ascii'))
        return rcontent

    def blockreplicas(self, **params):
        res = self._session.get("%s/blockreplicas" % self.datasvc, params=params)
        br = json.loads(res.content)

        df = pd.io.json.json_normalize(br['phedex']['block'],
                                record_path='replica',
                                record_prefix='replica.',
                                meta=['bytes', 'files', 'name', 'id', 'is_open']
                                )
        self._fmt_dates(df, ['replica.time_create', 'replica.time_update'])
        return df

    def datasetinfo(self, dataset):
        if '*' in dataset:
            raise ValueError("Wildcards not supported for this call")
        res = self._session.get("%s/data" % self.datasvc, params={'dataset': dataset, 'level': 'block'})
        resjson = json.loads(res.content)

        if len(resjson['phedex']['dbs']) == 0:
            return pd.DataFrame()

        flat = []
        for dataset in resjson['phedex']['dbs'][0]['dataset']:
            for block in dataset.pop('block', []):
                record = {'dataset.'+k: v for k, v in dataset.items()}
                record.update({'block.'+k: v for k, v in block.items()})
                flat.append(record)

        df = pd.DataFrame(flat)
        self._fmt_dates(df, ['block.time_create', 'block.time_update'])
        return df

    def reqmgr_transitions(self, **params):
        '''
            outputdataset
            inputdataset
            etc.
            https://github.com/dmwm/WMCore/wiki/reqmgr2-apis
        '''
        reqparam = {
            'mask': 'RequestTransition',
        }
        reqparam.update(params)
        res = self._session.get("https://cmsweb.cern.ch/reqmgr2/data/request", params=reqparam)
        resjson = json.loads(res.content)

        flat = []
        for row in resjson['result']:
            for requestname, item in row.items():
                for i, transition in enumerate(item['RequestTransition']):
                    flatrow = {
                        'requestname': requestname,
                        'current': i+1==len(item['RequestTransition']),
                    }
                    flatrow.update(params)
                    flatrow.update(transition)
                    flat.append(flatrow)

        df = pd.io.json.json_normalize(flat)
        self._fmt_dates(df, ['UpdateTime'])
        return df

    def groupusage(self, **params):
        res = self._session.get("%s/groupusage" % self.datasvc, params=params)
        resjson = json.loads(res.content)

        flat = []
        for item in resjson['phedex']['node']:
            for subitem in item.pop('group', []):
                info = {'node.'+k: v for k, v in item.items()}
                info.update(subitem)
                flat.append(info)

        return pd.DataFrame(flat)

    def requestlist(self, **params):
        res = self._session.get("%s/requestlist" % self.datasvc, params=params)
        rl = json.loads(res.content)

        df = pd.io.json.json_normalize(rl['phedex']['request'],
                                record_path='node',
                                record_prefix='node.',
                                meta=['approval', 'requested_by', 'type', 'id', 'time_create']
                                )

        self._fmt_dates(df, ['node.time_decided', 'time_create'])
        return df

    def subscriptions(self, **params):
        res = self._session.get("%s/subscriptions" % self.datasvc, params=params)
        resjson = json.loads(res.content)

        flat = []
        for item in resjson['phedex']['dataset']:
            for subitem in item.pop('block', []):
                flat.append(subitem)
            flat.append(item)

        df = pd.io.json.json_normalize(flat,
                                record_path='subscription',
                                record_prefix='subscription.',
                                meta=['bytes', 'files', 'is_open', 'name']
                                )

        self._fmt_dates(df, ['subscription.time_start', 'subscription.time_create'])
        return df

    def filelatency(self, block):
        res = self._session.get("%s/filelatency" % self.datasvc, params={'block': block})
        fl = json.loads(res.content)
        return fl

    def delete(self, node, datasets, comments=''):
        data = ElementTree.Element('data', {'version': '2.0'})
        dbs = ElementTree.Element('dbs', {'name': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'})
        data.append(dbs)

        for dataset in datasets:
            res = self._session.get("%s/data" % self.datasvc_xml, params={'dataset': dataset})
            if res.status_code != 200:
                raise ValueError("Could not retrieve catalog info for dataset %r" % dataset)
            resxml = ElementTree.fromstring(res.content)
            for dataset in resxml[0].iter('dataset'):
                dataset.attrib = {
                    'name': dataset.attrib['name'],
                    'is-transient': dataset.attrib['is_transient'],
                    'is-open': dataset.attrib['is_open'],
                }
                for block in dataset.iter('block'):
                    block.attrib = {
                        'name': block.attrib['name'],
                        'is-open': block.attrib['is_open']
                    }
                    for file in list(block.iter('file')):
                        block.remove(file)
                dbs.append(dataset)

        params = {
            'node': node,
            'level': 'dataset',
            'rm_subscriptions': 'y',
            'comments': comments,
            'data': ElementTree.tostring(data, encoding='unicode'),
        }

        res = self._session.post("%s/delete" % self.datasvc, data=params)
        if res.status_code == 200:
            with open("datasvc_deletelog.json", "a") as fout:
                fout.write(res.content.decode('ascii') + "\n")

        resjson = json.loads(res.content)
        return resjson['phedex']['request_created'][0]['id']

    def approve(self, request, node, comments='Auto-approving'):
        params = {
            'request': request,
            'node': node,
            'comments': comments,
            'decision': 'approve',
        }
        res = self._session.post("%s/updaterequest" % self.datasvc, data=params)
        try:
            resjson = json.loads(res.content)
            return resjson['phedex']['request_updated'][0]['id']
        except json.JSONDecodeError:
            raise Exception(res.content)


class DynamoLocks(object):
    def __init__(self):
        self._locks = pd.read_csv("http://t3serv001.mit.edu/~dynamo/detox_locks.csv")
        datecols = ['expires', 'created']
        self._locks[datecols] = self._locks[datecols].apply(lambda v: pd.to_datetime(v))

        now = datetime.datetime.now()
        self._wildcard_locks = []
        self._simple_locks = set()
        for _, lock in self._locks.iterrows():
            # Slightly conservative: treat block locks as dataset level
            item = lock['item'].split('#')[0]
            if '*' in item or '*' in lock['site']:
                self._wildcard_locks.append((item, lock['site']))
            else:
                self._simple_locks.add((item, lock['site']))

    def check_lock(self, dataset, site='None'):
        if '#' in dataset:
            dataset = dataset.split('#')[0]
        if (dataset, site) in self._simple_locks:
            return True
        if (dataset, 'None') in self._simple_locks:
            return True
        for ds_match, site_match in self._wildcard_locks:
            if fnmatch.fnmatch(dataset, ds_match) and fnmatch.fnmatch(site_match, site):
                return True
            elif site == 'None' and fnmatch.fnmatch(dataset, ds_match):
                return True
        return False
