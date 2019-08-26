# Modules and scripts for data synchronization between PhEDEx and Rucio

## `phedex.py`

this module contains functions that wrap PhEDEx datasvc and das and extract information
from the PhEDEx system. The module is used by `cmsdatareplica.py`, `cmslinks.py`, `cmsrses.py`,
`synccmssites.py`.

The moduel also provides a command line to test each function.

For example:
```
./phedex.py --func datasvc --call 'senames'
```

returns the result of the `senames` call to datasvc.

```
./phedex.py --func das --query='block dataset=/Neutrino_E-10_gun/RunIISpring15PrePremix-PU2016_80X_mcRun2_asymptotic_v14-v2/GEN-SIM-DIGI-RAW system=phedex'
```

execute a das quesry

```
./phedex.py --func tfc --pnn T2_FR_GRIF_LLR
```

returns the TFC of T2_FR_GRIF_LLR.

## `cmstfc.py`

this is the module of the plugin implementing the TFC lfn2pfn mapping. It is installed on the server.
Here it is used by some scripts/modules (`cmsrses.py`, `phedex.py`) to check mapping.

## `cmsrses.py`

this is the script that defines rucio RSE according to the definition of the corresponding PhEDEx nodes.

Here are some examples. Create Tier-1's `Buffer` and `Disk` RSES (excluding e.g. T1_UK_RAL)

```
./cmsrses.py --type temp --type real --type test --pnn all --select 'T1_\S+' --exclude '\S+_MSS' --exclude '\S+RAL\S+' --fts https://fts3.cern.ch:8446
```	

Create Tier-2, excluding some with 'exotic' configuration

```
./cmsrses.py --pnn all --select 'T2_\S+' --exclude 'T2_MY_\S+' --exclude 'T2_US_Florida' --exclude '\S+CERN\S+' --type real --type temp --type test --fts https://fts3.cern.ch:8446
```

## `syncaccounts.py`

script creating the accounts needed for synchronization.
e.g. this creates account for one RSE
```
./syncaccounts.py --rse T3_FR_IPNL
```

this creates account for all T2
```
./syncaccounts.py --rsefilter 'T2_\S+'
```

## `cmslinks.py`

script defining the distances between rucio RSEs accoring to the links between the corresponding
PhEDEx nodes and other pre-defined policies.
E.g. to get links set up with standard policies:

```
./cmslinks.py --phedex_links --overwrite --disable
```

## `mp_custom.py`

module wrapping `multiprocess` to allow 2 level multiprocessing. Used by the `synccmssites.py` script

## `instrument.py`

module with decorators and function for measuring execution times (low time precision is not an issue here
as times are of order of several minutes)

## `synccmssites.py`

script that keeps sites synchronized. For a configuration example see `cmssyncsites.yaml.example`. 
The way to call it 
```
[root@0823c23dfec3 scripts]# ./synccmssites.py --help
usage: synccmssites.py [-h] [--config CONFIG] [--logs LOGS] [--nodaemon]

Service for synching rucio and phedex locality data

optional arguments:
  -h, --help       show this help message and exit
  --config CONFIG  Configuration file. Default /etc/synccmssites.yaml.
  --logs LOGS      Logs file. Default /rucio/logs.
  --nodaemon       Runs in foreground.
```

for example 

```
 ./synccmssites.py --logs /rucio/dev.syn.log
``` 

will start the service in background. Logs are appended.
To see how to stop the service look the `main:run` options in the example configuration yaml file (and comments therein).

## `user_script.py`

This script make the upload of a user file to a temporary RSE and then it registers that in rucio asking for the transfer to a second RSE 

```
[root@5e05cd8bdf68 scripts]# python user_script.py -h
usage: user_script.py [-h] [--pfn PFN] [--account ACCOUNT] [--scope SCOPE]
                      file replica temp dest

Arguments for file Rucio upload

positional arguments:
  file               local file path
  replica            Rucio replica name
  temp               Rucio source temp RSE
  dest               Rucio destination RSE

optional arguments:
  -h, --help         show this help message and exit
  --pfn PFN          Source rse pfn
  --account ACCOUNT  Rucio account
  --scope SCOPE      Rucio scope
```

for example:

```
python user_script.py replica23.txt /store/user/dciangot/repl14.txt T2_IT_Legnaro_Temp T2_IT_Legnaro --pfn srm://t2-srm-02.lnl.infn.it:8443/srm/managerv2?SFN=/pnfs/lnl.infn.it/data/cms/store/temp/user/dciangot/repl14.txt --scope user.dciangot --account dciangot
```

This is a preliminary attempt to start working on CMS user file registration in Rucio.

