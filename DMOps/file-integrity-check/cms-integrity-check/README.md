## CMS Specific Check

While decompression-based tool verifies a file's physical health, this tool ensures that CMS physics objects and metadata structures are consistent and compatible with the CMSSW framework. More specific, the tool:
- Uses edmFileUtil to verify that the CMS-specific file catalog and metadata are readable.
- Uses a sampling algorithm to decompress specific events, ensuring that data are physically intact.

```
$ python3 check_integrity.py --help
usage: check_integrity.py [-h] [--full-scan] [--strict] [--skip-edmfileutil] [--skip-fwlite] [--samples SAMPLES] [--step-size STEP_SIZE] [--per-event-timeout PER_EVENT_TIMEOUT] [--verbose] file_or_list

CMS ROOT integrity checker (JSON output)

positional arguments:
  file_or_list          Single ROOT file or text file containing list of ROOT files

optional arguments:
  -h, --help            show this help message and exit
  --full-scan           Disables sampling and forces iteration over all events.
  --strict              Returns FAILED integrity status even for non-fatal errors.
  --skip-edmfileutil
  --skip-fwlite
  --samples SAMPLES     Number of fixed points to sample with FWLite (default 5).
  --step-size STEP_SIZE
                        Check one event every N events (0 to disable, default 50).
  --per-event-timeout PER_EVENT_TIMEOUT
  --verbose
```

[[Source Code](https://github.com/eachristgr/CMSRucio/blob/bb5b0b64555eebac7645121ee37644f1a4e1fb44/DMOps/file-integrity-check/cms-integrity-check/check_integrity.py)]]

The following wrapper handles the activation of the required environment in lxplus.
```
$ ./run_check.sh --help
Usage: ./run_check.sh [OPTIONS] <ROOT file or .txt list>

Runs the CMS file content integrity checker after setting up CMSSW environment and proxy.

Options:
  --full-scan      Disables event sampling and checks every event in the file.
  --strict         Returns FAILED integrity status for files with non-fatal EDM errors.
  --help, -h       Display this help message and exit.

The positional argument is either a local path, a list file (.txt), or a remote URL (davs:// or root://).

Note: The underlying Python script has more options (--samples, --step-size, etc.)
```

[[Source Code](https://github.com/eachristgr/CMSRucio/blob/bb5b0b64555eebac7645121ee37644f1a4e1fb44/DMOps/file-integrity-check/cms-integrity-check/run_check.sh)]

**Important Note**

This tool is not a complete integrity check. Because it only samples events, it might miss corruption in other parts of the file. Conversely, it might fail a healthy file if the CMSSW version you are using doesn't match the file's version.

<details>
<summary><b>Execution Example</b></summary>

Command:
```Bash
$ ./run_check.sh davs://ccdavcms.in2p3.fr:2880/disk/data/store/data/Run2024E/Muon0/RAW-RECO/ZMu-PromptReco-v2/000/381/384/00000/053dfc81-a3a5-4ab3-a534-2905f542f12f.root
```

Expected Output:
```bash
Processing remote URL: davs://ccdavcms.in2p3.fr:2880/disk/data/store/data/Run2024E/Muon0/RAW-RECO/ZMu-PromptReco-v2/000/381/384/00000/053dfc81-a3a5-4ab3-a534-2905f542f12f.root
Loading CMS environment...
Using CMSSW release: xxx
Creating temporary X.509 proxy...
Proxy Subject (DN): xxx
X.509 proxy location: xxx
Proxy time left: xxx
[
  {
    "file": "davs://ccdavcms.in2p3.fr:2880/disk/data/store/data/Run2024E/Muon0/RAW-RECO/ZMu-PromptReco-v2/000/381/384/00000/053dfc81-a3a5-4ab3-a534-2905f542f12f.root",
    "strict_mode": false,
    "full_scan_mode": false,
    "integrity": "PASSED",
    "root_open_ok": true,
    "edm_detected": true,
    "failed_checks": [],
    "details": {}
  }
]

Summary: 1 passed, 0 failed out of 1 files.
```
</details>