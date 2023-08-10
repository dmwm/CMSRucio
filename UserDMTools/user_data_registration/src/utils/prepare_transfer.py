import sys
from src.utils.lfn2pfn import get_pfns_temp


def get_src_and_dst_list(files, upload_options, account_options):
    """
    Provided a filename containing list of pfns
        - Prefix to remove in pfns to get lfns
        - Destination RSE
        - username (rucio account) and account type (GROUP, USER)

    Returns:
        - List of tuples of the format (lfn, source, dest)
    """

    prefix_to_trim = upload_options["prefix_to_trim"]
    dest_temp_rse = upload_options["dest_temp_rse"]
    username = account_options["username"]
    account_type = account_options["account_type"]

    lfn2src = {}
    file_src_dst = list()

    for name in files:
        if name.startswith(prefix_to_trim):
            lfn = f"/store/{account_type}/rucio/{username}{name[len(prefix_to_trim):]}"
            lfn2src[lfn] = name

        else:
            print(f"WARNING: {name} does not contain the desired prefix")
            sys.exit("Please check the list of file names")

    lfn2dst = get_pfns_temp(dest_temp_rse, lfn2src.keys(), account_type)

    for lfn in lfn2dst:
        src_pfn = lfn2src[lfn]
        dst_pfn = lfn2dst[lfn]

        file_src_dst.append((lfn, src_pfn, dst_pfn))

    return file_src_dst


if __name__ == "__main__":

    files = [
        "davs://deepthought.crc.nd.edu:1094/store/mc/Phase2HLTTDRSummer20ReRECOMiniAOD/TTToSemiLepton_TuneCP5_14TeV-powheg-pythia8/FEVT/PU200_111X_mcRun4_realistic_T15_v1-v1/120000/A0037171-FD6D-8B4E-A8D2-71CB77893C4A.root",
        "davs://deepthought.crc.nd.edu:1094/store/mc/RunIISummer20UL17NanoAODv9/GluGluToContinToZZTo2e2tau_TuneCP5_13TeV-mcfm701-pythia8/NANOAODSIM/106X_mc2017_realistic_v9-v2/270000/701A10AD-FC1D-E145-A402-D0209482A5A6.root"
    ]

    options = {
        "prefix_to_trim": "davs://deepthought.crc.nd.edu:1094/store",
        "dest_temp_rse": "T3_US_NotreDame_Temp",
        "username": "rchauhan",
        "account_type": "user"
    }

    res = get_src_and_dst_list(files, options)
    for lfn, src, dst in res:
        print(f" file_lfn:{lfn}\n src_pfn:{src}\n dst_pfn:{dst}\n")
