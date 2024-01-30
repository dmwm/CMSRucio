# User DataManagement Tools


## Requirements

- initialise rucio (installs rucio_client) - `source /cvmfs/cms.cern.ch/rucio/setup-py3.sh`
- initalise proxy - `voms-proxy-init -voms cms -rfc -valid 192:00`
- export RUCIO_ACCOUNT=$USER 

## Additionally
pip3 install -r requirements.txt


## How to

Show list of available commands: `python3 upload.py --help`

Show help related to a specific commands (cmd): `python3 upload.py <cmd> --help`

<br>

To upload a local file and create a rule on it:
- Copy template from `lib/templates/file-upload.yml`
- Specify parameters:
    - `filePath`: location of local file 
    - `lfn`: lfn (logical file name) for the file - `store/user/rucio/<username>/<filename>`
    - `tempRSE`: temp RSE to use for the intermediate hop (can be left to template value)
    - `rse`: destination RSE/RSE Expression on which rule should be created
    - `lifetime`: life of rule
    - `copies`: number of copies required

- Execute: `python3 upload.py upload-file-yaml <yaml-file-path>`

<br>

To upload a dataset create a rule on it:
- Copy template from `lib/templates/dataset-upload.yml`
- Specify parameters:
    - `datasetPath`: location of folder to use as dataset 
    - `datasetName`: name of dataset conforming to CMS naming conventions
    - `tempRSE`: temp RSE to use for the intermediate hop (can be left to template value)
    - `rse`: destination RSE/RSE Expression on which rule should be created
    - `lifetime`: life of rule
    - `copies`: number of copies required

- Execute: `python3 upload.py upload-dataset-yaml <yaml-file-path>`




