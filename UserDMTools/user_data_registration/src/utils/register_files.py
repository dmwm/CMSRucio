import gfal2
import pprint
import uuid


def generate_file_guid():
    return str(uuid.uuid4()).replace('-', '').lower()


def get_metadata_for_files(file_src_dst_list, scope):
    """
    Collects metadata for provided file list

    Returns: List of dictionaries containing metadata of requested files
    """

    ctx = gfal2.creat_context()
    files = []
    for file, _, dest in file_src_dst_list:
        file_meta = {}
        file_meta["name"] = file
        file_meta["pfn"] = dest
        file_meta["bytes"] = ctx.stat(dest).st_size
        file_meta["adler32"] = ctx.checksum(dest, "adler32")
        file_meta["meta"] = {'guid': generate_file_guid()}
        file_meta['scope'] = scope
        file_meta['state'] = 'A'
        # file_meta["md5"] = ""

        files.append(file_meta)

    return files


def register_temp_replicas(client, file_src_dst_list, scope, rse, dry_run=False):
    """
    Registers files in the provided temporary RSE

    Arguments:
    Client: Rucio Client
    file_src_dst_list: List of tuples in the form (file, src_pfn, dst_pfn)
    scope: rucio scope to register files under. e.g. user.rchauhan
    rse: Temp rse to use for initial file registration

    Returns:
    List of registered lfns
    """

    # verification whether the scope exists
    account_scopes = []
    try:
        account_scopes = client.list_scopes_for_account(client.account)
    except Exception as e:
        print(e)
    if account_scopes and scope not in account_scopes:
        print('Scope {} not found for the account {}.'.format(scope, client.account))

    files = get_metadata_for_files(file_src_dst_list, scope)

    lfns = []
    for file in files:
        lfns.append(file["name"])

    if dry_run:
        print("Registerd files: ")
        print(*lfns, sep="\n")

    else:
        try:
            client.add_replicas(rse=rse, files=files)
        except Exception as e:
            print('Unable to register replicas', e)

    return lfns
