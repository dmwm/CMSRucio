# Test authentication alternating two rucio servers
# Intended for a continuous execution by cron logging all attempts 
# to a local log file e.g. in /RSEtest/LOGS/ . 
echo ========  Testing Rucio authentication  =========
echo $0
date
mv -f  /opt/rucio/etc/rucio.cfg /opt/rucio/etc/rucio.cfg.bak
ln -s /opt/rucio/etc/rucio.cfg.unichicago /opt/rucio/etc/rucio.cfg 
grep rucio_host /opt/rucio/etc/rucio.cfg
X509_USER_PROXY=/tmp/x509up_u0 grid-proxy-info -subject -path -timeleft
X509_USER_PROXY=/tmp/x509up_u0 /RSEtest/docker/CMSRucioClient/scripts/updateRSEs.py --test-auth 
rm -f /opt/rucio/etc/rucio.cfg
ln -s /opt/rucio/etc/rucio.cfg.fermilab /opt/rucio/etc/rucio.cfg
date
grep rucio_host /opt/rucio/etc/rucio.cfg
X509_USER_PROXY=/tmp/x509up grid-proxy-info -subject -path -timeleft
X509_USER_PROXY=/tmp/x509up /RSEtest/docker/CMSRucioClient/scripts/updateRSEs.py --test-auth 
mv -f  /opt/rucio/etc/rucio.cfg.bak /opt/rucio/etc/rucio.cfg
