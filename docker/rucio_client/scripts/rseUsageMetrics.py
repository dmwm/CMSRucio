#! /usr/bin/env python3

from rucio.client import Client

client = Client()

def get_sum_of_all_rse_statics(rse_expression):
    rses = [rse["rse"] for rse in client.list_rses(rse_expression=rse_expression)]
    result = 0
    for rse in rses:
        static, _, _ = get_rse_usage(rse)
        result += static
    return result


def get_rse_usage(rse):
    rse_usage = list(client.get_rse_usage(rse))

    required_fields = {"static", "rucio", "expired"}
    relevant_info = {}

    for source in rse_usage:
        # Assuming source and used keys exist
        relevant_info[source["source"]] = source["used"]

    if not required_fields.issubset(relevant_info.keys()):
        print("Skipping {} due to lack of relevant key in rse".format(rse))
        print("{} is not a subset of {}".format(required_fields, relevant_info.keys()))
        return 0, 0, 0

    # Apparently, python integers do not overflow, https://docs.python.org/3/library/exceptions.html#OverflowError

    static, rucio, expired = relevant_info["static"], relevant_info["rucio"], relevant_info["expired"]
    return static, rucio, expired