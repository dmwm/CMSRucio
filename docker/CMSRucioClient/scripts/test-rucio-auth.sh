# Test authentication alternating two rucio servers
# Intended for a continuous execution by cron logging all attempts 
# to a local log file e.g. in /RSEtest/LOGS/ . 
srv1='https://rucio-cms.grid.uchicago.edu:443'
srv2='https://fermicloud055.fnal.gov:443'
echo "======== test Rucio server access ========="
echo $0
date
echo host1:  $srv1
X509_USER_PROXY=/tmp/x509up_u0 grid-proxy-info -subject -path -timeleft
X509_USER_PROXY=/tmp/x509up_u0 /usr/bin/rucio --host $srv1 --auth-host $srv1 -a natasha 'whoami'
date
echo host2:  $srv2
X509_USER_PROXY=/tmp/x509up grid-proxy-info -subject -path -timeleft
X509_USER_PROXY=/tmp/x509up /usr/bin/rucio --host $srv2 --auth-host $srv2 -a natasha 'whoami'
