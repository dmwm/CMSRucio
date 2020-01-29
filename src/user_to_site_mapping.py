#! /usr/bin/env python

from rucio.client import Client
from rucio.common.exception import RSENotFound, AccountNotFound, AccessDenied, RSEOperationNotSupported

import os
import sys
import json
import urllib2

import utility as util

# rucio Client
client = Client()

# dictionary with US-based only CRIC users, their RSEs and the corresponding quotas
cric_user = {}


def mapping_by_country(institute, country):
    """
    This function returns a site based on the user's institute and institute country.
    If institute = '' the user no longer belong to CMS Experiment.
    """
    if not institute:
        return

    institutes_by_country = util.rses_by_country[country]

    for rse_key, institutes_val in institutes_by_country.items():
        if institute in institutes_val:
            return rse_key + "_Test"


def user_to_site_mapping_by_country(country):
    """
    This function gets all the CRIC user profiles in JSON format by using
    CRIC API. For each user extracts relevant information to run the algorithm.
    Then update the cric_user dictionary.
    """
    cric_global_user = json.load(urllib2.urlopen(util.CRIC_URL))
    
    for key, user in cric_global_user.items():
        institute_country = user['institute_country'].encode("utf-8")

        if country in institute_country:
            # name = key.encode("utf-8")
            dn = user['dn'].encode("utf-8")
            institute = user['institute'].encode("utf-8")
            email = user['email'].encode("utf-8")
            username = user['profiles'][0]['login'].encode("utf-8")
            if username not in cric_user:
                tmp = create_dict_entry(username, dn, email, institute, institute_country)
                if not tmp:
                    continue

                cric_user.update(tmp)


# return a dictionary entry
def create_dict_entry(username, dn, email, institute, institute_country):
    """
    This utility function obtains the right RSE for the current user based on its institute
    and build and return a dictionary entry with the correct user to site mapping.
    Assigns a default quota at that site.
    """
    rse_key = mapping_by_country(institute, institute_country)

    if not rse_key:  # user no longer in CMS
        return

    client.set_account_limit(account=username, rse=rse_key, bytes=util.DEFAULT_RSE_SIZE)

    entry = {
        username: {
            "dn": dn,
            "institute": institute,
            "institute_country": institute_country,
            "email": email,
            "RSES": {
                rse_key: util.DEFAULT_RSE_SIZE,
                # 'other_rse': 0,

            }
        }
    }
    return entry


# get user information
def get_user(username):
    try:
        info = cric_user[username]
    except KeyError:
        raise AccountNotFound
    return info


def get_user_quota(username=None, rse=None, method='get'):
    """
    This function returns the quota for a given user at a certain site.
    """
    try:
        check_user_error(username)
    except AccountNotFound:
        raise

    try:
        check_rse_error(username, rse, method)
    except RSENotFound:
        raise

    limit = client.get_account_limit(account=username, rse=rse)
    return limit[rse]


def reset_user_quota(username=None, rse=None):
    """
    This function resets a given RSE for a given user to the default value.
    """
    try:
        check_user_error(username)
    except AccountNotFound:
        raise

    try:
        check_rse_error(username=username, rse=rse, method='get')
    except RSENotFound:
        raise

    cric_user[username]["RSES"][rse] = util.DEFAULT_RSE_SIZE
    client.set_account_limit(username, rse, util.DEFAULT_RSE_SIZE)


def set_user_quota(username, rse, quota):
    """
    This function sets a new quota for a certain RSE for a given user.
    """
    try:
        check_user_error(username)
    except AccountNotFound:
        raise

    try:
        check_rse_error(username=username, rse=rse, method='set')
        check_quota_error(username, rse, quota, 'set')
    except RSENotFound:
        raise
    except RSEOperationNotSupported:
        raise

    cric_user[username]["RSES"][rse] = quota
    client.set_account_limit(username, rse, quota)


def check_quota_error(username=None, rse=None, quota=0, method='get'):
    """
    This functions checks if the new given quota is less than the current one or
    if it has wrong values.
    """
    old_quota = get_user_quota(username, rse, method)
    if old_quota is not None and (quota <= 0 or quota < int(old_quota)):
        raise RSEOperationNotSupported


def delete_user_quota(username=None, rse=None):
    """
    This function deletes the limits on an rse of a given user.
    """
    try:
        check_user_error(username)
    except AccountNotFound:
        raise

    try:
        check_rse_error(username, rse)
    except RSENotFound:
        raise

    del cric_user[username]["RSES"][rse]
    client.delete_account_limit(username, rse)


def check_user_error(username=None):
    """
    This function checks if user is in CRIC users.
    """
    try:
        user_val = cric_user[username]
    except KeyError:
        raise AccountNotFound
    try:
        client.get_account(username)
    except AccountNotFound:
        raise

    if user_val is None:
        raise AccountNotFound


def check_rse_error(username=None, rse=None, method='get'):
    """
    This function checks for errors on the rse name.
    Then checks if it the RSE actually exists.
    Method parameter is used to not check if the current user has
    quota on a given rse to reuse this function for the set method.
    """
    user_val = cric_user[username]
    if rse is None:
        raise RSENotFound

    '''if rse not in user_val['RSES'] and method is 'get':
        print user_val['RSES'].keys(), method
        raise RSENotFound'''

    if method is 'get':
        try:
            user_val['RSES'][rse]
        except KeyError:
            raise RSENotFound

    try:
        client.get_rse(rse)['rse']
    except RSENotFound:
        raise


user_to_site_mapping_by_country('US')

