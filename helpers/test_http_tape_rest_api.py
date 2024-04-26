from rucio.client import Client
from argparse import ArgumentParser


def set_davs_endpoint(client, rse):
    # remove the srm and any other protocol that is not davs
    protocols = client.get_protocols(rse=rse)

    # in principle we do not need to delete protocols, the davs protocol gets priority
    # but we do it to emulate the final goal of having only davs protocol
    for proto in protocols:
        if proto['scheme'] != 'davs':
            client.delete_protocols(
                rse=rse, scheme=proto['scheme'], hostname=proto['hostname'], port=proto['port'])

    # Then set update_from_json=False
    client.add_rse_attribute(rse=rse, key='update_from_json', value=False)


def create_write_rule(client, rse, scope, name):
    # create rule to the rse
    rule_id = client.add_replication_rule(dids=[{'scope': scope, 'name': name}],
                                          copies=1, rse_expression=rse, grouping='DATASET', lifetime=86400,
                                          activity='Debug',
                                          comment="Testing tape http rest write operation")[0]

    print("Rule created: %s" % rule_id)
    print(f"https://cms-rucio-webui.cern.ch/rule?rule_id={rule_id}")


def create_read_rule(client, rse, scope, name, read_to_rse):
    # create rule to the rse
    rule_id = client.add_replication_rule(dids=[{'scope': scope, 'name': name}],
                                          copies=1, rse_expression=read_to_rse, grouping='DATASET', lifetime=86400,
                                          activity='Debug',
                                          comment="Testing tape http rest read operation",
                                          source_replica_expression=rse)[0]

    print("Rule created: %s" % rule_id)
    print(f"https://cms-rucio-webui.cern.ch/rule?rule_id={rule_id}")


def check_davs_scheme_exists(client, rse):
    protocols = client.get_protocols(rse=rse)
    for proto in protocols:
        if proto['scheme'] == 'davs':
            return True
    return False


if __name__ == "__main__":
    ap = ArgumentParser()

    ap.add_argument('--rse', help='rse to test', required=True)
    ap.add_argument('--mode', help='mode to test',
                    required=True, choices=['write', 'read'])

    # arguments for write mode
    ap.add_argument(
        '--scope', help='scope of did to use for write operation', default='cms')
    ap.add_argument(
        '--dataset', help='dataset name to use for write operation',
        default='/HIPhysicsRawPrime19/HIRun2023A-PromptReco-v2/MINIAOD#0fcda50b-cea5-476c-964c-b6cb4ebcb970')

    # arguments for read mode
    ap.add_argument(
        '--ruleid', help='exsiting rule id to use as a read source', default=None)
    ap.add_argument(
        '--readtorse', help='destination rse to use for read operation', default='T2_CH_CERN')

    args = ap.parse_args()

    client = Client()
    rse = args.rse
    mode = args.mode

    # if chosen mode is read then require a ruleid argument
    if args.mode == 'read':
        rule_id = args.ruleid
        reard_to_rse = args.readtorse
        try:
            rule = client.get_replication_rule(rule_id=rule_id)
            if rule['state'] != 'OK' or rule["rse_expression"] != rse:
                raise Exception
        except Exception as e:
            print(
                "Rule not found or not valid, please provide a valid rule id or try again in 1 hr", e)
            exit(1)

    if not check_davs_scheme_exists(client, rse):
        print("No davs scheme exists for rse: %s" % rse)
        print("If the configurtaion exists in storage.json, please set update_from_json=True and try again in 1 hr")
        print("If the configuration does not exist, please add the protocol manually")

    else:
        set_davs_endpoint(client, rse)
        if mode == 'write':
            scope = args.scope
            name = args.dataset
            create_write_rule(client, rse, scope, name)
        elif mode == 'read':
            name = rule['name']
            scope = rule['scope']
            create_read_rule(client, rse, scope, name, reard_to_rse)
