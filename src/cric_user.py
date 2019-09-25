#! /usr/bin/env python

from rucio.client import Client
from rucio.common.exception import RSENotFound

"""
This class helps to store all the info needed to map a user to an RSE. It contains a policy instance for each user
to specify different policies for different users. The rses_list allows an user to have quotas at different sites.
"""

class CricUser:

    def __init__(self, username, email, dn, account_type, institute, institute_country, policy, option):
        self.username = username
        self.email = email
        self.dn = dn
        self.account_type = account_type
        self.institute = institute
        self.institute_country = institute_country
        self.policy = policy
        self.rses_list = []
        try:
            self.rses_list = self.policy.get_rse(username=self.username, institute=self.institute,
                                                      institute_country=self.institute_country, option=option)
        except RSENotFound:
            raise
        except Exception:
            pass

    def get_rse(self, site_name):
        for rse in self.rses_list:
            if rse.sitename == site_name:
                return rse

        raise RSENotFound

    def get_rse_quota(self, site_name):
        for rse in self.rses_list:
            if rse.sitename == site_name:
                return rse.quota

    def add_rse(self, rse):
        self.rses_list.append(rse)
        rucio_rses = [r['rse'] for r in Client().list_rses()]

        if rse.site_name not in rucio_rses:
            raise RSENotFound

    def delete_rse_by_name(self, site_name):
        for rse in self.rses_list:
            if rse.sitename == site_name:
                del rse

    def set_rse_quota(self, site_name, new_quota):
        for rse in self.rses_list:
            if rse.sitename == site_name:
                rse.set_quota(new_quota)

    def change_policy(self, policy, **kwargs):
        self.policy = policy
        try:
            self.rses_list.clear()
            self.rses_list = (self.policy.get_rse(kwargs, option='set-new-only'))
        except RSENotFound:
            raise
        except Exception:
            pass

