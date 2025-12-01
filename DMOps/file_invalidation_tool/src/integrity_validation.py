import os
import gfal2
import logging
import argparse
import contextlib
import pandas as pd
from pathlib import Path
from rucio.client import Client

ctx = gfal2.creat_context()

@contextlib.contextmanager
def suppress_logs(logger_name):
    logger = logging.getLogger(logger_name)
    current_level = logger.level
    logger.setLevel(logging.ERROR)
    try:
        yield
    finally:
        logger.setLevel(current_level)

def validate_file_exists(file_path):
    """
    Validate if a file exists at the given file path.

    Args:
        file_path (str): The path to the file.

    Returns:
        str: The valid file path.

    Raises:
        argparse.ArgumentTypeError: If the file does not exist.

    """
    if not os.path.exists(file_path):
        raise argparse.ArgumentTypeError(f"The file '{file_path}' does not exist.")
    return file_path

def validate_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('files', type=validate_file_exists, help='File containing the replicas of files to verify checksum. Format CSV: FILENAME,RSE')
    parser.add_argument('--dry-run', action='store_true', help='Test the script without invalidating anything')

    args = parser.parse_args()
    dids_df = pd.read_csv(args.files)
    if len(dids_df) == 0:
        raise ValueError("Files list can't be empty")
    if not ['FILENAME', 'RSE_EXPRESSION'].issubset(dids_df.columns):
        raise ValueError('File must contain columns FILENAME and RSE_EXPRESSION')

    return dids_df, args.dry_run

def check_file_integrity(pfn: str, adler: str) -> bool:
    filename = Path(pfn).name
    local_path = Path("/data") / filename
    file_uri = f"file://{local_path}"

    logging.info(f'Copying file {pfn} to local directory...')

    try:
        with suppress_logs('gfal2'):
            ctx.filecopy(pfn, file_uri)
            integrity = ctx.checksum(file_uri, "adler32") == adler
            return integrity
    except Exception as e:
        logging.error(f"Error during file integrity check: {e}")
        return False
    finally:
        try:
            os.remove(local_path)
        except OSError as e:
            logging.warning(f"Failed to remove temporary file {local_path}: {e}")

def checksum_invalidate_dids(dids_df, dry_run=False):
    client = Client()
    invalidate = []
    for filename, rse_expression in dids_df.values:
        for replica in client.list_replicas([{'scope':'cms','name':filename}],rse_expression=rse_expression):
            adler = replica['adler32']
            for rse in replica['rses'].keys():
                if replica['states'][rse] == 'AVAILABLE':
                    for pfn in replica['rses'][rse]:
                        try:
                            integrity = check_file_integrity(pfn,adler)
                            if integrity:
                                logging.info("File: %s is valid at RSE: %s" % (filename,rse))
                            else:
                                invalidate.append({'scope': 'cms', 'name': filename, 'rse': rse})
                            break
                        except Exception as ex:
                            logging.error("Unable to validate file: %s at RSE: %s" % (filename,rse))
                            logging.error(ex)

    if len(invalidate) > 0 and not dry_run:
        client.declare_bad_file_replicas(invalidate,reason='FIT: Checksum mismatch', force=True)
        for record in invalidate:
            logging.info("Declared file %s as bad at %s" % (record['name'], record['rse']))
    elif dry_run:
        for  record in invalidate:
            logging.info("Would declare file %s as bad at %s" % (record['name'], record['rse']))
    else:
        logging.info("No files were declared as bad")

if __name__ == '__main__':
    dids_df, dry_run = validate_arguments()
    checksum_invalidate_dids(dids_df, dry_run)