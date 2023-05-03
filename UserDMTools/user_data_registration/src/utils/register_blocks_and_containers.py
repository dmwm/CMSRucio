import re
import uuid


def get_regexp():
    # TODO: (if required) Define regex if dataset names need to be created from file names
    """
    """
    regex = r"/store/group/rucio/pog_tau_group/ul_embedding/large_miniAOD_v2/(.*?)/(.*?)/(.*?)/(.*?)/.*"
    return regex


def convert_filename_to_dataset(file):
    # TODO: if the use case ever arises, just accept regex and send it to above (get_regexp) function
    matches = re.findall(get_regexp(), file)
    final_state, primary_dataset, datatier, campaign = matches[0]
    datatier = "USER"
    dataset_name = f"/{primary_dataset}/{final_state}-{campaign}/{datatier}"
    return dataset_name


def get_dataset_name(lfns):
    # get dataset name using first file name
    dataset_name = convert_filename_to_dataset(lfns[0])

    # verification: verify its true for all the files in the list
    for lfn in lfns:
        if dataset_name != convert_filename_to_dataset(lfn):
            raise Exception("file names are not accurate")
    return dataset_name


def create_block_file_map(dataset_name, lfns, block_limit=100):

    # To get dataset name from filename
    # dataset_name = get_dataset_name(lfns)
    blocks = {}
    i, j = 0, 0
    while i < len(lfns):
        j = min(j + block_limit, len(lfns))
        new_block = f"{dataset_name}#{uuid.uuid4()}"
        blocks[new_block] = lfns[i:j]
        i = j

    return blocks


def register_dataset(client, scope, name, blocks, dry_run):
    if dry_run:
        print("Datasets register: ")
        print(f"{scope}:{name}")

        for block in blocks:
            print({"scope": scope, "name": block})
        return

    dataset_contents = []
    for block in blocks:
        did = {"scope": scope, "name": block}
        dataset_contents.append(did)

    client.add_did(scope=scope, name=name, did_type="CONTAINER")

    attachments = [{"scope": scope, "name": name, "dids": dataset_contents}]
    return client.attach_dids_to_dids(attachments)

    # attachments is: [attachment, attachment, ...]
    # attachment is: {'scope': scope, 'name': name, 'dids': dids}
    # dids is: [{'scope': scope, 'name': name}, ...]


def register_blocks(client, scope, block_file_map, dry_run):
    if dry_run:
        for block in block_file_map:
            print("Blocks register: ", block)
            for file in block_file_map[block]:
                print({"scope": scope, "name": file})
        return

    attachments = []
    for block in block_file_map:
        block_contents = []
        for file in block_file_map[block]:
            did = {"scope": scope, "name": file}
            block_contents.append(did)
        client.add_did(scope=scope, name=block, did_type="DATASET")

        attachement = {"scope": scope, "name": block, "dids": block_contents}
        attachments.append(attachement)

    return client.attach_dids_to_dids(attachments)
