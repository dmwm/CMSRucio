# Helper Scripts for DM Operation


### _dynamic_ddmquota_update.py_
### _find_file_replicas.py_
```
usage: find_file_replicas.py [-h] [-f FILE] [-v] [-n] [lfn [lfn ...]]

find RSEs that have replica

positional arguments:
  lfn         logical file name

optional arguments:
  -h, --help  show this help message and exit
  -f FILE     file of lfns, could be multiple
  -v          verbose: show turl
  -n          with -v, do not show LFN

** without arguments, input is taken from stdin
```
examples:
```
bash-4.2$ find_file_replicas.py /dcache/uscmsdisk/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/0b07e67c-c236-452c-b2c9-1e4506ea487d.root /dcache/uscmsdisk/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/102b05ec-b52a-4ea1-828c-2b5948d9f1e1.root
/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/0b07e67c-c236-452c-b2c9-1e4506ea487d.root [ T1_US_FNAL_Disk T0_CH_CERN_Disk T2_US_MIT_Tape T1_US_FNAL_Tape T0_CH_CERN_Tape ]
/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/102b05ec-b52a-4ea1-828c-2b5948d9f1e1.root [ T1_US_FNAL_Disk T0_CH_CERN_Disk T2_US_MIT_Tape T1_US_FNAL_Tape ]
bash-4.2$ find_file_replicas.py -v /dcache/uscmsdisk/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/0b07e67c-c236-452c-b2c9-1e4506ea487d.root /dcache/uscmsdisk/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/102b05ec-b52a-4ea1-828c-2b5948d9f1e1.root
/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/0b07e67c-c236-452c-b2c9-1e4506ea487d.root [ T1_US_FNAL_Disk T0_CH_CERN_Disk T2_US_MIT_Tape T1_US_FNAL_Tape T0_CH_CERN_Tape ]
    T1_US_FNAL_Disk  srm://cmsdcadisk.fnal.gov:8443/srm/managerv2?SFN=/dcache/uscmsdisk/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/0b07e67c-c236-452c-b2c9-1e4506ea487d.root
    T0_CH_CERN_Disk  gsiftp://eoscmsftp.cern.ch:2811/eos/cms/tier0/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/0b07e67c-c236-452c-b2c9-1e4506ea487d.root
    T2_US_MIT_Tape   gsiftp://tapesrmcms.nese.rc.fas.harvard.edu:2811/cms/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/0b07e67c-c236-452c-b2c9-1e4506ea487d.root
    T1_US_FNAL_Tape  srm://cmsdcatape.fnal.gov:8443/srm/managerv2?SFN=/11/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/0b07e67c-c236-452c-b2c9-1e4506ea487d.root
    T0_CH_CERN_Tape  root://eosctacms.cern.ch:1094//eos/ctacms/archive/cms//store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/0b07e67c-c236-452c-b2c9-1e4506ea487d.root
/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/102b05ec-b52a-4ea1-828c-2b5948d9f1e1.root [ T1_US_FNAL_Disk T0_CH_CERN_Disk T2_US_MIT_Tape T1_US_FNAL_Tape ]
    T1_US_FNAL_Disk  srm://cmsdcadisk.fnal.gov:8443/srm/managerv2?SFN=/dcache/uscmsdisk/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/102b05ec-b52a-4ea1-828c-2b5948d9f1e1.root
    T0_CH_CERN_Disk  gsiftp://eoscmsftp.cern.ch:2811/eos/cms/tier0/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/102b05ec-b52a-4ea1-828c-2b5948d9f1e1.root
    T2_US_MIT_Tape   gsiftp://tapesrmcms.nese.rc.fas.harvard.edu:2811/cms/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/102b05ec-b52a-4ea1-828c-2b5948d9f1e1.root
    T1_US_FNAL_Tape  srm://cmsdcatape.fnal.gov:8443/srm/managerv2?SFN=/11/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/102b05ec-b52a-4ea1-828c-2b5948d9f1e1.root
bash-4.2$ printf "/dcache/uscmsdisk/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/0b07e67c-c236-452c-b2c9-1e4506ea487d.root\n/dcache/uscmsdisk/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/102b05ec-b52a-4ea1-828c-2b5948d9f1e1.root" | find_file_replicas.py
/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/0b07e67c-c236-452c-b2c9-1e4506ea487d.root [ T1_US_FNAL_Disk T0_CH_CERN_Disk T2_US_MIT_Tape T1_US_FNAL_Tape T0_CH_CERN_Tape ]
/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/102b05ec-b52a-4ea1-828c-2b5948d9f1e1.root [ T1_US_FNAL_Disk T0_CH_CERN_Disk T2_US_MIT_Tape T1_US_FNAL_Tape ]
bash-4.2$ printf "/dcache/uscmsdisk/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/0b07e67c-c236-452c-b2c9-1e4506ea487d.root\n/dcache/uscmsdisk/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/102b05ec-b52a-4ea1-828c-2b5948d9f1e1.root" | find_file_replicas.py -v -n
    T1_US_FNAL_Disk  srm://cmsdcadisk.fnal.gov:8443/srm/managerv2?SFN=/dcache/uscmsdisk/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/0b07e67c-c236-452c-b2c9-1e4506ea487d.root
    T0_CH_CERN_Disk  gsiftp://eoscmsftp.cern.ch:2811/eos/cms/tier0/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/0b07e67c-c236-452c-b2c9-1e4506ea487d.root
    T2_US_MIT_Tape   gsiftp://tapesrmcms.nese.rc.fas.harvard.edu:2811/cms/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/0b07e67c-c236-452c-b2c9-1e4506ea487d.root
    T1_US_FNAL_Tape  srm://cmsdcatape.fnal.gov:8443/srm/managerv2?SFN=/11/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/0b07e67c-c236-452c-b2c9-1e4506ea487d.root
    T0_CH_CERN_Tape  root://eosctacms.cern.ch:1094//eos/ctacms/archive/cms//store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/0b07e67c-c236-452c-b2c9-1e4506ea487d.root
    T1_US_FNAL_Disk  srm://cmsdcadisk.fnal.gov:8443/srm/managerv2?SFN=/dcache/uscmsdisk/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/102b05ec-b52a-4ea1-828c-2b5948d9f1e1.root
    T0_CH_CERN_Disk  gsiftp://eoscmsftp.cern.ch:2811/eos/cms/tier0/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/102b05ec-b52a-4ea1-828c-2b5948d9f1e1.root
    T2_US_MIT_Tape   gsiftp://tapesrmcms.nese.rc.fas.harvard.edu:2811/cms/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/102b05ec-b52a-4ea1-828c-2b5948d9f1e1.root
    T1_US_FNAL_Tape  srm://cmsdcatape.fnal.gov:8443/srm/managerv2?SFN=/11/store/data/Run2022A/ZeroBias9/RAW/v1/000/354/655/00000/102b05ec-b52a-4ea1-828c-2b5948d9f1e1.root
bash-4.2$
```
### _fix_tape_file_exists.py_
### _get_account_info.py_
### _get_error_logs.py_
### _get_stuck_rules.py_
### _InvalidateReplicas.py_
### _list_dids.py_
### _list_replicas.py_
### _list_subscription_rules.py_
### _list_subscriptions.py_
### _modify_protocol.py_
### _nanoaod_subscription.py_
### _RepairFromDBS.py_
### _rule_requests_
### _set_regions.py_
### _StageDatasetForUser.py_
### _transferurl.py_
```
usage: transferurl.py [-h] rse [protocol] file

compose transfer url at RSE according to storage.json

positional arguments:
  rse         Rucio Storage Element
  protocol    transfer protocol
  file        file

optional arguments:
  -h, --help  show this help message and exit

** without arguments, input is taken from stdin, one line per file
```
### _update_account.py_
### _update_rule.py_
