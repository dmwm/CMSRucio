#!/usr/bin/python3
import re
import logging
import typer
from enum import Enum
from strictyaml import Map, Str, Int, Seq, Optional
from strictyaml import load, YAMLValidationError
from rucio.client import Client

# from cms_rucio_import.lib.did_upload import datasetSchema, fileSchema, upload_dataset_and_create_rule, upload_file_and_create_rule, TemplateType

########### CLI Definitions ###########

app = typer.Typer()

class TemplateType(str, Enum):
    file = "file"
    dataset = "dataset"


@app.command()
def upload_file(rse: str, temp_rse: str, lfn: str, file_path: str, copies: int, lifetime: int):
    """
    Upload a single file and create a rule for rse
    """

    typer.echo(f"File {file_path} will be uploaded to {rse}")

    upload_params = {
        "tempRSE": temp_rse,
    }

    rule_params = {
        "copies": copies,
        "rse": rse,
        "lifetime": lifetime
    }

    rucio_client = Client()
    upload_file_and_create_rule(
        rucio_client, file_path, lfn, upload_params, rule_params)


@app.command()
def upload_file_yaml(yamlfile: str):
    """
    Same as upload file
    Arguments specified in yaml files
    """
    with open(yamlfile) as f:
        yaml_string = f.read()

    try:
        params = load(yaml_string=yaml_string, schema=fileSchema)
    except YAMLValidationError as e:
        print(e)
        raise typer.Exit()

    params = params.data

    file_path = params["specs"]["filePath"]
    lfn = params["specs"]["lfn"]
    upload_params = params["specs"]["options"]["upload"]
    rule_params = params["specs"]["options"]["rule"]

    rucio_client = Client()
    upload_file_and_create_rule(
        rucio_client, file_path, lfn, upload_params, rule_params)


@app.command()
def upload_dataset(rse: str, temp_rse: str, dataset_path: str, dataset_name: str, copies: int, lifetime: int):
    """
    Upload a files specified in the folder and create a dataset containing those files, further create a rule for the dataset on specified rse
    """
    rucio_client = Client()

    upload_params = {
        "tempRSE": temp_rse,
    }

    rule_params = {
        "copies": copies,
        "rse": rse,
        "lifetime": lifetime
    }
    upload_dataset_and_create_rule(
        rucio_client, dataset_path, dataset_name, rule_params, upload_params)


@app.command()
def upload_dataset_yaml(yamlfile: str):
    """
    Same as upload dataset
    Arguments specified in yaml files
    """

    with open(yamlfile) as f:
        yaml_string = f.read()

    try:
        params = load(yaml_string=yaml_string, schema=datasetSchema)
    except YAMLValidationError as e:
        print(e)
        raise typer.Exit()

    params = params.data

    rucio_client = Client()
    dataset_path = params["specs"]["datasetPath"]
    dataset_name = params["specs"]["datasetName"]
    rule_params = params["specs"]["options"]["rule"]
    upload_params = params["specs"]["options"]["upload"]

    upload_dataset_and_create_rule(
        rucio_client, dataset_path, dataset_name, rule_params, upload_params)


@app.command()
def generate_yaml_template(type: TemplateType = typer.Argument(..., case_sensitive=False), dest: str = typer.Argument(".")):
    # TODO
    typer.echo(f"Under development")
    typer.echo(f"Please copy files from src/rucio-user-dm/templates/")


########### Schema Defination ###########

datasetSchema = Map(
    {
        "kind": Str(),
        Optional("metadata"): Map({
            "name": Str()
        }),
        "specs": Map({
            "datasetPath": Str(),
            "datasetName": Str(),
            "options": Map({
                "upload": Map({
                    "tempRSE": Str()
                }),
                "rule": Map({
                    "rse": Str(),
                    "copies": Int(),
                    "lifetime": Int()
                })
            }),
            Optional("lfnMap"): Seq(Map({"name": Str(), "lfn": Str()}))
        })
    }
)


fileSchema = Map(
    {
        "kind": Str(),
        Optional("metadata"): Map({
            "name": Str()
        }),
        "specs": Map({
            "filePath": Str(),
            "lfn": Str(),
            "options": Map({
                "upload": Map({
                    "tempRSE": Str()
                }),
                "rule": Map({
                    "rse": Str(),
                    "copies": Int(),
                    "lifetime": Int(),
                })
            }),

        })
    }
)


########### Logger ###########


class CustomFormatter(logging.Formatter):

    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    blue = "\x1b[36;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: blue + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def get_logger(name, level=logging.INFO):

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(level)

    ch.setFormatter(CustomFormatter())

    logger.addHandler(ch)

    return logger


########### Dataset Upload Functions ###########
logger = get_logger('dataset-upload')


def add_dataset(rucio_client, dataset_name):
    user_scope = f'user.{rucio_client.account}'
    return rucio_client.add_did(scope=user_scope, name=dataset_name, did_type='DATASET')


def attach_files_to_dataset(rucio_client, dataset_name, filenames):
    user_scope = f'user.{rucio_client.account}'
    dids = [{'scope': user_scope, 'name': filename} for filename in filenames]
    return rucio_client.attach_dids(scope=user_scope, name=dataset_name, dids=dids)


def upload_dataset_and_create_rule(rucio_client, dataset_path, dataset_name, rule_params, upload_params):
    from os import listdir, stat
    from os.path import join, isfile

    DEFAULT_TEMP_UPLOAD_RSE = "T2_CH_CERN_Temp"

    account_name = rucio_client.account
    temp_rse = DEFAULT_TEMP_UPLOAD_RSE
    if upload_params.get("tempRSE"):
        temp_rse = upload_params["tempRSE"]

    files = []
    file_list = [file for file in listdir(
        dataset_path) if isfile(join(dataset_path, file))]
    lfn_map = {
        file: f"/store/user/rucio/{account_name}{dataset_name.split('#')[0]}/{file}" for file in file_list}
    lfns = list(lfn_map.values())
    pfns = get_pfns_temp(temp_rse=temp_rse, lfns=lfns)

    for file in file_list:
        files.append({
            "lfn": lfn_map[file],
            "pfn": pfns[lfn_map[file]],
            "file_path": join(dataset_path, file),
        })

    # upload files
    try:
        upload(rucio_client, files=files, temp_rse=temp_rse)
    except Exception as e:
        logger.log(logging.ERROR, e)

    # create dataset
    dataset_add_status = add_dataset(rucio_client, dataset_name)
    # TODO: convert all print statements to log
    print("dataset_add_status: ", dataset_add_status)

    # attach uploaded files to dataset
    attach_files_to_dataset_status = attach_files_to_dataset(
        rucio_client, filenames=lfns, dataset_name=dataset_name)
    print("attach_files_to_dataset_status: ", attach_files_to_dataset_status)

    # create rule on dataset
    user_scope = f'user.{rucio_client.account}'
    add_rule(
        rucio_client,
        dids=[{'scope': user_scope, 'name': dataset_name}],
        copies=rule_params["copies"],
        rse=rule_params["rse"],
        lifetime=rule_params["lifetime"],
    )


########### File Upload Functions ###########

def upload(rucio_client, files, temp_rse):
    from rucio.client.uploadclient import UploadClient
    uclient = UploadClient(logger=logger)

    items = [{
        "path": file["file_path"],
        "rse": temp_rse,
        "pfn": file["pfn"],
        "name": file["lfn"],
        "did_name": file["lfn"],
        "no_register": True,
    }
        for file in files
    ]

    blue = "\x1b[35;20m"
    reset = "\x1b[0m"

    # trying to upload file
    uclient.upload(items)

    # collecting metadata about file
    files = uclient._collect_and_validate_file_info(items)

    # registering uploaded replicas in rucio catalogue
    for file in files:
        register_temp_replica(rucio_client, uclient, file)


def upload_file_and_create_rule(rucio_client, file_path, lfn, upload_params, rule_params):
    from os import stat

    DEFAULT_TEMP_UPLOAD_RSE = "T2_CH_CERN_Temp"
    account_name = rucio_client.account
    temp_rse = DEFAULT_TEMP_UPLOAD_RSE
    if upload_params.get("tempRSE"):
        temp_rse = upload_params["tempRSE"]

    pfns = get_pfns_temp(temp_rse=temp_rse, lfns=[lfn])

    files = [{
        "lfn": lfn,
        "pfn": pfns[lfn],
        "file_path": file_path,
    }]

    try:
        upload(rucio_client, files=files, temp_rse=upload_params["tempRSE"])
    except Exception as e:
        logger.log(logging.ERROR, e)

    user_scope = f'user.{account_name}'
    add_rule(
        rucio_client,
        dids=[{'scope': user_scope, 'name': lfn}],
        copies=rule_params["copies"],
        rse=rule_params["rse"],
        lifetime=rule_params["lifetime"],
    )


########### Replica Registration ###########

def register_temp_replica(rucio_client, uclient, file):
    """
    Registers the given file in Rucio. Creates a dataset if
    needed. Registers the file DID and creates the replication
    rule if needed. Adds a replica to the file did.
    (This function is meant to be used as class internal only)

    :param file: dictionary describing the file
    :param registered_dataset_dids: set of dataset dids that were already registered
    :param ignore_availability: ignore the availability of a RSE

    :raises DataIdentifierAlreadyExists: if file DID is already registered and the checksums do not match
    """

    logger.log(logging.DEBUG, 'Registering file')

    # verification whether the scope exists
    account_scopes = []
    try:
        account_scopes = rucio_client.list_scopes_for_account(
            rucio_client.account)
    except Exception as e:
        print(e)
    if account_scopes and file['did_scope'] not in account_scopes:
        logger.log(logging.WARNING, 'Scope {} not found for the account {}.'.format(
            file['did_scope'], rucio_client.account))

    rse = file['rse']
    file_scope = file['did_scope']
    file_name = file['did_name']
    file_did = {'scope': file_scope, 'name': file_name}
    file['state'] = 'A'
    replica_for_api = uclient._convert_file_for_api(file)

    try:
        # if the remote checksum is different this did must not be used
        meta = rucio_client.get_metadata(file_scope, file_name)
        logger.log(logging.INFO, 'File DID already exists')
        logger.log(logging.DEBUG, 'local checksum: %s, remote checksum: %s' % (
            file['adler32'], meta['adler32']))

        if str(meta['adler32']).lstrip('0') != str(file['adler32']).lstrip('0'):
            logger.log(logging.ERROR, 'Local checksum %s does not match remote checksum %s' % (
                file['adler32'], meta['adler32']))
            raise Exception("Did Already exists")

        # add file to rse if it is not registered yet
        replicastate = list(rucio_client.list_replicas(
            [file_did], all_states=True))
        if rse not in replicastate[0]['rses']:
            rucio_client.add_replicas(rse=rse, files=[replica_for_api])
            logger.log(
                logging.INFO, 'Successfully added replica in Rucio catalogue at %s' % rse)
    except Exception as e:
        logger.log(logging.DEBUG, 'File DID does not exist')
        rucio_client.add_replicas(rse=rse, files=[replica_for_api])
        logger.log(
            logging.INFO, 'Successfully added replica in Rucio catalogue at %s' % rse)


########### Rule Creation Functions ###########

def add_rule(rucio_client, dids, copies, rse, lifetime):
    try:
        rule_id = rucio_client.add_replication_rule(
            dids, copies, rse, lifetime=lifetime)
        logger.info(f"Rule id: {rule_id}")
        logger.info(
            f"Rule Info: https://cms-rucio-webui.cern.ch/rule?rule_id={rule_id[0]}")
    except Exception as e:
        print(e)


########### LFN to Temp PFN Mappings ###########

def get_pfns_temp(temp_rse, lfns):
    """
    get pfns from RSE name and list of lfns for temporary RSES
    :param temp_rse: TempRSE name
    :type temp_rse: str
    :param lfns: list of lnfs
    :type lfns: list[str]
    :return: dictionary {lfn1: pfn1, lfn2: pfn2, ...}
    :rtype: dict
    """
    temp_lfn_to_lfn = {}
    pfn_map = {}
    for lfn in lfns:
        index = lfn.find('/user')
        temp_lfn = lfn[:index] + '/temp' + lfn[index:]
        temp_lfn_to_lfn[temp_lfn] = lfn

    pfn_map_temp = get_pfns(temp_rse, list(temp_lfn_to_lfn.keys()))
    for temp_lfn in pfn_map_temp:
        pfn_map[temp_lfn_to_lfn[temp_lfn]] = pfn_map_temp[temp_lfn]

    return pfn_map


def get_pfns(rse: str, lfns: list):
    from rucio.client import Client
    pfns = []
    pfn_map = {}
    rucio_client = Client()
    rucio_scope = f'user.{rucio_client.account}'
    # TODO do we need a check for this?

    try:
        rgx = rucio_client.get_protocols(
            rse.split("_Temp")[0], protocol_domain='ALL', operation="write")[0]

        if not rgx['extended_attributes'] or 'tfc' not in rgx['extended_attributes']:
            pfn_0 = rucio_client.lfns2pfns(
                rse=rse.split("_Temp")[0],
                lfns=[rucio_scope + ":" + lfns[0]],
                operation="write"
            )

            pfns.append(pfn_0[rucio_scope + ":" + lfns[0]])
            prefix = pfn_0[rucio_scope + ":" + lfns[0]].split(lfns[0])[0]

            for lfn in lfns:
                pfn_map.update({lfn: prefix+lfn})
        else:
            for lfn in lfns:
                if 'tfc' in rgx['extended_attributes']:
                    tfc = rgx['extended_attributes']['tfc']
                    tfc_proto = rgx['extended_attributes']['tfc_proto']

                    pfn_map.update({lfn: tfc_lfn2pfn(lfn, tfc, tfc_proto)})

    except TypeError:
        raise TypeError('Cannot determine PFN for LFN %s:%s at %s with proto %s'
                        % rucio_scope, lfn, rse, rgx)
    return pfn_map


def tfc_lfn2pfn(lfn, tfc, proto, depth=0):
    """
    Performs the actual tfc lfn2pfn matching
    """
    MAX_CHAIN_DEPTH = 5

    if depth > MAX_CHAIN_DEPTH:
        raise Exception("Max depth reached matching lfn %s and protocol %s with tfc %s" %
                        lfn, proto, tfc)

    for rule in tfc:
        if rule['proto'] == proto:
            if 'chain' in rule:
                lfn = tfc_lfn2pfn(lfn, tfc, rule['chain'], depth + 1)

            regex = re.compile(rule['path'])
            if regex.match(lfn):
                return regex.sub(rule['out'].replace('$', '\\'), lfn)

    if depth > 0:
        return lfn

    raise ValueError(
        "lfn %s with proto %s cannot be matched by tfc %s" % (lfn, proto, tfc))

