from rucio.client.client import Client

def get_rse_usage(client, rse_expression):

    rse_names = list(client.list_rses(rse_expression))
    rse_names = sorted(item["rse"] for item in rse_names)

    rse_usage = {}
    for rse in rse_names:
        client_generator = client.get_rse_usage(rse)
        rse_usage[rse] = {}
        usage_list = list(client_generator)
        for usage in usage_list:
            rse_usage[rse][usage['source']] = {
                'used': usage['used'],
                'free': usage['free'],
                'total': usage['total']
            }
            rse_usage[rse]['MinFreeSpace'] = client.get_rse_limits(rse)['MinFreeSpace']
    return rse_usage


def get_freespace(rse,rse_usage):

    dynamic = rse_usage[rse]['expired']['used'] - rse_usage[rse]['obsolete']['used']
    occupancy =  rse_usage[rse]['rucio']['used']
    target_occupancy = rse_usage[rse]['rucio']['total'] - rse_usage[rse]['MinFreeSpace']
    freespace = (target_occupancy - occupancy) + dynamic

    if freespace < 0:
        print(f"Warning Free Space at {rse} < 0")
        return 0
    else:
        return freespace

client = Client()
rse_expression = "(rse_type=DISK)&(cms_type=real)&(tier<3)"

rse_names = list(client.list_rses(rse_expression))
rse_names = sorted(item["rse"] for item in rse_names)

rse_usage = get_rse_usage(client, rse_expression)

for rse in rse_names:
    print(f"{rse} Freespace : {(get_freespace(rse,rse_usage) / 1e12):.1f} TB")

