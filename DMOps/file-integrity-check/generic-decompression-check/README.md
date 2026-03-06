## In-Depth File Integrity Check

There are cases where a file has correct size and checksum but is corrupted. This indicates issues during the production and/or registration of the file. In such cases, checking the integrity of a file requires in depth iterations over it's content and maybe checking all available replicas in case a global invalidation is needed.

### Decompression Based Check

Leveraging the `uproot` python module we can perform a deep-scan of a file by forcing the decompression of it's elements. The following tool focuses exclusively on physical integrity (bit-rot, truncation and compression errors) by bypassing the high-level CMSSW/EDM frameworks.

```
$ python3 check_decompression.py --help
usage: check_decompression.py [-h] [--full-scan] [--timeout TIMEOUT] [-v] file

Check ROOT file integrity.

positional arguments:
  file               Local path to the ROOT file to check.

optional arguments:
  -h, --help         show this help message and exit
  --full-scan        Perform a full scan by reading every basket (more time-consuming). Defaults to False (Fail-Fast mode)
  --timeout TIMEOUT  Timeout in seconds for the integrity check. Defaults to 900 seconds (15 minutes)
  -v, --verbose      Increase verbosity: -v (Warning), -vv (Info), -vvv (Debug). Default is Error.

returns:
  A tuple where the first element indicates overall success and the second element is a ValidationStatus indicating OK, CORRUPTED, or ERROR.
```

[[Source Code](https://github.com/eachristgr/CMSRucio/blob/bb5b0b64555eebac7645121ee37644f1a4e1fb44/DMOps/file-integrity-check/generic-decompression-check/check_decompression.py)]

The tool requires `uproot` python module.

### File Integrity Check Tool

The recommended way for inducting an in-depth integrity check is by using the following tool. A tool that uses LFNs and RSE expressions to find replicas, copy replicas locally, contacts a checksum based check and a decompression check if needed.

```
$ python3 run_check.py --help
usage: run_check.py [-h] [--rse-expression RSE_EXPRESSION] [--scope SCOPE] [--full-scan] [--timeout TIMEOUT] [-v] lfns [lfns ...] workdir

Check integrity of files based on their LFNs by copying them locally, validating checksums and performing content checks by decompression.

positional arguments:
  lfns                  A .txt file with a list of LFNs to check.
  workdir               Local directory to use for copying files.

optional arguments:
  -h, --help            show this help message and exit
  --rse-expression RSE_EXPRESSION
                        RSE expression to filter replicas. (default: None - check all replicas).
  --scope SCOPE         Rucio scope of the files. (default: 'cms')
  --full-scan           Perform a full scan by reading every basket (more time-consuming). (default: False)
  --timeout TIMEOUT     Timeout in seconds for the integrity check of each file. (default: 900s)
  -v, --verbose         Increase verbosity: -v (Warning), -vv (Info), -vvv (Debug). Default is Error.

returns:
  A list of results for each LFN, including replica information and validation status.
```

[[Source Code](https://github.com/eachristgr/CMSRucio/blob/bb5b0b64555eebac7645121ee37644f1a4e1fb44/DMOps/file-integrity-check/generic-decompression-check/run_check.py)]

The tool requires `uproot` python module, `rucio` python client and `gfal2`.

It is recommended to use it on lxplus after activating your voms proxy and by using CERNBox (eos) as the working directory.