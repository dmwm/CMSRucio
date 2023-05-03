import gfal2
import sys


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def printC(message, color):
    print(f"{color} {message} {bcolors.ENDC}")


def event_callback(event):
    print("[%s] %s %s %s" %
          (event.timestamp, event.domain, event.stage, event.description))


def monitor_callback(src, dst, average, instant, transferred, elapsed):
    print("\r [%4d] %.2fMB (%.2fKB/s)" % (elapsed, transferred / 1048576, average / 1024), end=""),
    sys.stdout.flush()


def get_src_dst_from_tuple_list(file_src_dest_list):
    sources, destinations = list(), list()
    for file in file_src_dest_list:
        sources.append(file[1])
        destinations.append(file[2])

    return sources, destinations


def doCopy(file_src_dest_list, ctx, params, overwrite=False):
    # Copy!
    # In this case, an exception will be thrown if the whole process fails
    # If any transfer fail, the method will return a list of GError objects, one per file
    # being None if that file succeeded

    successful_count = 0
    failed_transfers = set()
    failed_validataions = set()

    sources, destinations = get_src_dst_from_tuple_list(file_src_dest_list)
    params.overwrite = overwrite

    try:
        errors = ctx.filecopy(params, sources, destinations)
        if not errors:
            print(f"Copy succeeded!", bcolors.OKGREEN)
            successful_count += 1
        else:
            for i in range(len(errors)):
                e = errors[i]
                src = sources[i]
                dst = destinations[i]
                if e and e.code == 17:
                    if comapare_checksum(ctx, src, dst):
                        printC(f"Already exists with correct checksum {src} => {dst}", bcolors.OKCYAN)
                        successful_count += 1
                    else:
                        failed_validataions.add(("", src, dst))
                elif e:
                    failed_transfers.add(("", src, dst))
                    printC(f"Failed [{e.code}] {e.message}: {src} => {dst}", bcolors.FAIL)
                else:
                    if not comapare_checksum(ctx, src, dst):
                        failed_validataions.add(("", src, dst))
                        printC(f" Checksum Mismatch: \n SRC: {sources[i]} \n DST: {destinations[i]}", bcolors.FAIL)
                    else:
                        successful_count += 1
                        printC(f"Succeded {src} => {dst}", bcolors.OKCYAN)
    except Exception as e:
        print("Copy failed: %s" % str(e))
        sys.exit(1)

    return successful_count, failed_transfers, failed_validataions


def copyFiles(file_src_dest_list, overwrite=False, validate=False):

    ctx = gfal2.creat_context()
    params = ctx.transfer_parameters()

    # params.event_callback = event_callback
    params.monitor_callback = monitor_callback

    if overwrite:
        params.overwrite = True

    if validate:
        params.checksum_check = True

    successful_count, failed_transfers, failed_validataions = doCopy(
        file_src_dest_list, ctx, params, overwrite=False)

    if len(failed_validataions) != 0:
        retry_successful_count, retry_failed_transfers, retry_failed_validataions = doCopy(
            failed_validataions, ctx, params, overwrite=True)
        successful_count = successful_count + retry_successful_count
        failed_transfers = retry_failed_transfers + failed_transfers
        failed_validataions = failed_validataions - retry_failed_validataions

    # print("Total successful:= ", successful_count)
    return successful_count, failed_transfers, failed_validataions


def comapare_checksum(ctx, src, dst):
    printC(f'{ctx.checksum(src, "ADLER32")} {ctx.checksum(dst, "ADLER32")}', bcolors.OKBLUE)
    return ctx.checksum(src, "ADLER32") == ctx.checksum(dst, "ADLER32")


if __name__ == "__main__":
    lfn = "my_lfn"
    src = "davs://cmswebdav-kit.gridka.de:2880/pnfs/gridka.de/cms/disk-only/store/user/sbrommer/ul_embedding/large_miniAOD_v2/ElectronEmbedding/EmbeddingRun2017C/MINIAOD/inputDoubleMu_106X_ULegacy_miniAOD-v1/0000/ff640f41-1d4c-4236-8ce6-786f90f3405b.root"
    dest = "davs://cmswebdav-kit.gridka.de:2880/pnfs/gridka.de/cms/disk-only/store/temp/group/rucio/pog_tau_group/ul_embedding/large_miniAOD_v2/ElectronEmbedding/EmbeddingRun2017C/MINIAOD/inputDoubleMu_106X_ULegacy_miniAOD-v1/0000/ff640f41-1d4c-4236-8ce6-786f90f3405b.root"
    print(copyFiles([(lfn, src, dest)]))
