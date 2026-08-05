import json
import logging
import os
import time
from collections import Counter
from datetime import datetime

from opensearchpy import OpenSearch, exceptions


logging.basicConfig(level=logging.INFO, format='%(levelname)s %(asctime)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

def get_index_schema():
    """
    Creates mapping dictionary for the unified-logs daily index
    """
    return {
        "settings": {"index": {"number_of_shards": "1", "number_of_replicas": "1"}},
        "mappings": {
            "properties": {
                'rse_name': {"type": "keyword"},
                'file_name': {"type": "keyword"},
                'action': {"type": "keyword"},
                'mode': {"type": "keyword"},
                'state': {"type": "keyword"},
                'account': {"type": "keyword"},
                'rule_id': {"type": "keyword"},
                "timestamp": {"format": "epoch_second", "type": "date"}
            }
        }
    }


es_host = 'os-cms.cern.ch/os'
es_index = 'test-suspended-handler'

# Global OpenSearch connection client
_opensearch_client = None

def get_es_client(host, index_mapping_and_settings):
    """Creates OpenSearch client

    Uses a global OpenSearch client and return it if connection still holds, otherwise creates a new connection.
    """
    global _opensearch_client
    if not _opensearch_client:
        # reinitialize
        _opensearch_client = OpenSearchInterface(host, index_mapping_and_settings)
    return _opensearch_client


def send_to_os(part, opensearch_host=es_host, es_index_template=es_index):
    """Send given data to OpenSearch"""
    client = get_es_client(opensearch_host, get_index_schema())
    idx = client.get_or_create_index(timestamp=time.time(), index_template=es_index_template,index_mod="D")
    client.send(idx, part, metadata=None, batch_size=10000, drop_nulls=False)

def post_logs(df,rse,action,mode,state,account):
    """
    Sends the logs of a df with a list of file names about the action taken, the mode that initiated the action and the rse
    """
    try:
        for index,row in df.iterrows():
            row_log = {'rse_name':rse,'file_name':row['file_name'],'rule_id':row['rule_id'],'action':action,'mode':mode,'state':state,'account':account,'timestamp':int(time.time())}
            send_to_os(part=row_log)
    except Exception as e:
        logging.error(f"Could not send to OS: {e}")
        logging.error(f"df columns: {df.columns}")
        logging.error(f"Data frame: {df.head()}, rse {rse}, action: {action}, mode: {mode}, state: {state}, account: {account}")


class OpenSearchInterface(object):
    """Robust interface to OpenSearch cluster
    """

    def __init__(self, host, index_mapping_and_settings):
        try:
            logging.info("OpenSearch instance is initializing")
            self.host = host
            self.index_mapping_and_settings = index_mapping_and_settings
            username = os.environ['OS_USERNAME']
            password = os.environ['OS_PASSWORD']
            url = 'https://' + username + ':' + password + '@' + host
            self.handle = OpenSearch(
                [url],
                verify_certs=True,
                use_ssl=True,
                ca_certs='/etc/pki/tls/certs/ca-bundle.trust.crt',
            )
        except Exception as e:
            logging.error(f"OpenSearchInterface initialization failed: {e}")

    def make_mapping(self, idx):
        """Creates mapping of the index

        idx: Full index name test-foo(no date format), test-foo-YYYY-MM-DD(index_mod=D)
        """
        body = json.dumps(self.index_mapping_and_settings)
        # Make mappings for OpenSearch index
        result = self.handle.indices.create(index=idx, body=body, ignore=400)
        if result.get("status") != 400:
            logging.warning("Creation of index %s: %s" % (idx, str(result)))
        elif "already exists" not in result.get("error", "").get("reason", ""):
            logging.error("Creation of index %s failed: %s" % (idx, str(result.get("error", ""))))

    @staticmethod
    def parse_errors(result):
        """Parses bulk send result and finds errors to log
        """
        reasons = [d.get("index", {}).get("error", {}).get("reason", None) for d in result["items"]]
        counts = Counter([_f for _f in reasons if _f])
        n_failed = sum(counts.values())
        logging.error("Failed to index %d documents to OpenSearch: %s" % (n_failed, str(counts.most_common(3))))
        return n_failed

    @staticmethod
    def drop_nulls_in_dict(d):  # d: dict
        """Drops the dict key if the value is None

        OpenSearch mapping does not allow None values and drops the document completely.
        """
        return {k: v for k, v in d.items() if v is not None}  # dict

    @staticmethod
    def to_chunks(data, samples=10000):
        """Yields chunks of data"""
        length = len(data)
        for i in range(0, length, samples):
            yield data[i:i + samples]

    @staticmethod
    def make_es_body(bulk_list, metadata=None):
        """Prepares documents for bulk send by adding metadata part and separating with new line
        """
        metadata = metadata or {}
        body = ""
        for data in bulk_list:
            if metadata:
                data.setdefault("metadata", {}).update(metadata)
            body += json.dumps({"index": {}}) + "\n"
            body += json.dumps(data) + "\n"
        return body

    def get_or_create_index(self, timestamp, index_template, index_mod=""):
        """Creates index with mappings and settings if not exist


        timestamp      : epoch seconds
        index_template : index base name
        index_mode     : one of 'Y': "index_template"-YYYY, 'M': "index_template"-YYYY-MM, 'D': "index_template"-YYYY-MM-DD,
                           empty string uses single index as "index_template"

        Returns yearly/monthly/daily index string depending on the index_mode and creates it if it does not exist.
        - It checks if index already exists and returns its name.
        - Else, it creates the index with mapping which happens in the first batch of the month ideally.
        """
        timestamp = int(timestamp)
        if index_mod.upper() == "Y":
            idx = time.strftime("%s-%%Y" % index_template, datetime.utcfromtimestamp(timestamp).timetuple())
        elif index_mod.upper() == "M":
            idx = time.strftime("%s-%%Y-%%m" % index_template, datetime.utcfromtimestamp(timestamp).timetuple())
        elif index_mod.upper() == "D":
            idx = time.strftime("%s-%%Y-%%m-%%d" % index_template, datetime.utcfromtimestamp(timestamp).timetuple())
        else:
            idx = index_template
            
        try:
            self.handle.indices.get(index=idx)
            logging.info(f"Index found: {idx}")
            return idx
        except exceptions.NotFoundError:
            logging.info(f"Index {idx} doesn't exist, creating new index")
            get_es_client(self.host, self.index_mapping_and_settings).make_mapping(idx=idx)
            return idx
        except Exception as e:
            logging.error(f"Couldn't get or create index: {e}")
            return None

    def send(self, idx, data, metadata=None, batch_size=10000, drop_nulls=False):
        """Send data in bulks to OpenSearch instance, batching is implemented by default.

        Args:
            idx: full index name, can be index_template or index_template-YYYY-..
            data: can be a single document or list of documents to send
            metadata: metadata that will be sent with each document
            batch_size: batching sample size
            drop_nulls: in Grafana, null strings cause trouble in aggregations. If it is true, it drops None fields from the documents.
        """
        global _opensearch_client
        _opensearch_client = get_es_client(self.host, self.index_mapping_and_settings)

        # If one document as dict, make it list
        if not isinstance(data, list):
            data = [data]

        result_n_failed = 0
        for chunk in self.to_chunks(data, batch_size):
            if drop_nulls:
                chunk = [self.drop_nulls_in_dict(_x) for _x in chunk]
            body = self.make_es_body(chunk, metadata)
            res = _opensearch_client.handle.bulk(body=body, index=idx, request_timeout=300)
            if res.get("errors"):
                result_n_failed += self.parse_errors(res)
        if result_n_failed > 0:
            logging.error(f"OpenSearch send failed count: {result_n_failed}")
        logging.debug(f"OpenSearch send {len(data) - result_n_failed} documents successfully")
        return result_n_failed