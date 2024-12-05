import argparse
import logging

from dbs.apis.dbsClient import DbsApi
from dbs.exceptions.dbsClientException import dbsClientException

TEST_url="https://cmsweb-testbed.cern.ch/dbs/int/global/DBSWriter"
url="https://cmsweb.cern.ch/dbs/prod/global/DBSWriter"

# 'TEST_URL' or just 'url'

def validate_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('filename', type=str, help='Name of the file containing the list of DIDs')
    parser.add_argument('--datasets', type=str, help='Name of the file that contains the list of datasets to invalidate')
    parser.add_argument('--test', action='store_true', help='Use test DBS instance')

    args = parser.parse_args()
    files = []
    datasets = []

    if not args.filename:
        raise ValueError('Filename is required.')
    else:
        filename = args.filename
        with open(filename, 'r') as f:
            files = f.readlines()
            files = [x.strip().replace('\n','') for x in files]
        if len(files) == 0:
            raise ValueError("Files list can't be empty")

    if args.datasets:
        with open(args.datasets, 'r') as f:
            datasets = f.readlines()
            datasets = [x.strip().replace('\n','') for x in datasets]

    test = args.test
    return files, datasets, test

def invalidate_files(files, dbsApi=None, test=False):
    consecutiveErrors = 0
    if not dbsApi:
        dbsApi = DbsApi(url=TEST_url) if test else DbsApi(url=url)

    for lfn in files:
        if not test:
            try:
                result = dbsApi.updateFileStatus(logical_file_name=lfn, is_file_valid=0, lost=0)
                if result == []:
                    consecutiveErrors = 0
                    logging.info("Invalidation OK for file: %s" % lfn)
                else:
                    consecutiveErrors += 1
                    logging.error("Invalidation FAILED for file: %s" % lfn)
                    logging.error(result)
            except Exception as ex:
                consecutiveErrors += 1
                logging.error("Invalidation FAILED for file: %s" % lfn)
                logging.error(ex)

            if consecutiveErrors >= 5 or consecutiveErrors == len(files):
                raise Exception("Too many consecutive errors, check you have the right permissions to invalidate files on DBS or try again later.")
        else:
            logging.info("Would invalidate file on DBS: %s" % lfn)



def invalidate_datasets(files, datasets, test):
    dbsApi = DbsApi(url=TEST_url) if test else DbsApi(url=url)

    invalidate_files(files, dbsApi, test)

    for dataset in datasets:
        if not test:
            try:
                dataset_invalidation = dbsApi.updateDatasetType(dataset=dataset, dataset_access_type="INVALID")
                if dataset_invalidation == []:
                    logging.info("Invalidation OK for dataset: %s" % dataset)
                else:
                    logging.error("Invalidation FAILED for dataset: %s" % dataset)
                    logging.error(dataset_invalidation)
            except Exception as ex:
                logging.error("Invalidation FAILED for dataset: %s" % dataset)
                logging.error(ex)
        else:
            logging.info("Would invalidate dataset on DBS: %s" % dataset)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(asctime)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
    params = validate_arguments()

    files = params[0]
    datasets = params[1]
    test = params[2]

    invalidate_datasets(files, datasets, test)