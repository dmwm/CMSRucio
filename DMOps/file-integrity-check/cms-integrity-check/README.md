# CMS Integrity Check

This tool is designed to verify the integrity of CMS ROOT files by leveraging the official CMSSW Framework.

## Purpose

The tool performs a three-tiered validation:

### ROOT Level
Checks if the file is a "Zombie" or has a corrupted internal directory structure.

### EDM Metadata
Uses edmFileUtil to verify that the CMS-specific file catalog and metadata are readable.

### Event Sampling (FWLite)
Uses a sampling algorithm to "touch" and decompress specific events to ensure the data products are physically intact.

## How to Run

### Automatic Environment Setup (Recommended)

The bash wrapper automatically sets up your CMSSW environment and X.509 proxy.

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

### Advanced Manual Usage

Use the Python engine directly for specific sampling needs:

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

## Understanding Results

The tool outputs a JSON block for each file. Key fields include:

- `integrity` Either PASSED or FAILED.
- `root_open_ok` If false, the file is physically broken at the header level.
- `failed_checks` A list of specific stages that failed (e.g., FWLite_Sampling).

### Exit Codes:

- `0` Success (File is healthy).
- `1` Failure (Corruption detected).
- `2` Environment Error (Missing CMSSW/ROOT).

Example Output:
```
$ /afs/cern.ch/user/d/dmtops/public/cms_file_content_integrity_check/run_cms_file_content_integrity_check davs://ccdavcms.in2p3.fr:2880/disk/data/store/data/Run2024E/Muon0/RAW-RECO/ZMu-PromptReco-v2/000/381/384/00000/053dfc81-a3a5-4ab3-a534-2905f542f12f.root
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

