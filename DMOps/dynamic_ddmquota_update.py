# 2022/04/13  
# It is running in an acrontab owned by Benedict Maier

from rucio.client import Client
client = Client()

# DISK, no tape
# rse_type=DISK or TAPE
# cms_type=real or test
# tier<3, exclude T3
# tier>0, exclude T0,
# \(T2_US_Caltech_Ceph|T2_PL_Warsaw) (exclude)

RSE_EXPRESSION = "rse_type=DISK&cms_type=real&tier<3&tier>0\(T2_US_Caltech_Ceph|T2_PL_Warsaw)"

minfree=15000000000000 # 15.0 TB

rses = list(client.list_rses(rse_expression=RSE_EXPRESSION))
rse_names = []
for e in rses:
    rse_names.append(e["rse"])
rse_names.sort()



for rse in rse_names:
    site_usage = list(client.get_rse_usage(rse))

    total = 0 # total = static
    rucio = 0 # rucio used
    free = 0
    ddm_quota = 1000000  # 10**6

    for source in site_usage:
        if source["source"] == "static":
            total = source["used"]
        if source["source"] == "rucio":
            rucio = source["used"]

    free = total - rucio

    if free>minfree:
        ddm_quota = free - minfree
    else:
        ddm_quota = 1000000
 
    if rse == "T2_CH_CERN":
        ddm_quota = 10000
    if rse == "T1_RU_JINR_Disk":
        ddm_quota = 10000
    if rse == "T2_UA_KIPT":
        ddm_quota = 0
    if rse == "T2_US_MIT_Tape":
        ddm_quota = 0
    if rse == "T2_EE_Estonia":   # ggus-ticket 158850 2022-09-20: REMOVE once the Storage System migration is completed. 
        ddm_quota = 0

    result = client.add_rse_attribute(rse, "ddm_quota", ddm_quota)
    print(rse, ", ddmquota ", ddm_quota, "updated:", result)
