#! /usr/bin/env python

from policy import Policy
from quota import Quota

from rucio.client import Client
from rucio.common.exception import RSENotFound

import json
import sys

"""
This class represent the US CMS Policy.
"""

class InstitutePolicy(Policy):

    def __init__(self):
        self.default_quota = (10 ** 6)  # 1 MB for testing
        self.client = Client()
        self.CRIC_USERS_API = 'https://cms-cric.cern.ch/api/accounts/user/query/list/?json'

        with open('config_institute_policy.json') as policy_file:
            self._policy = json.load(policy_file)

    def get_policy(self):
        return self._policy

    def get_cric_url(self):
        return self.CRIC_USERS_API

    def set_default_quota(self, new_quota=None):
        self.default_quota = new_quota

    def get_rse(self, **kwargs):
        """
        This function, based on the option parameters, allow to choose whether to overwrite or to leave
        the current quota. Return a list of Quota objects.
        """
        rses_list = []
        if kwargs is not None:
            try:
                rse = self.get_rse_by_country(kwargs['institute'], kwargs['institute_country'])
            except RSENotFound:
                raise
            option = kwargs['option']
            quota = None
            if rse is None:
                sys.stderr.write('User ' + kwargs['username'] + 'no longer belong to CMS (institute is empty)\n')
                return

            if option == 'reset-all':
                quota = self.default_quota
            elif option == 'set-new-only':
                quota = self.client.get_account_limit(account=kwargs['username'], rse=rse)[rse]
                if quota is None:
                    message = "[quota SET] User {0} has now {1} bytes at the {2} site\n".format(kwargs['username'],
                                                                                                quota,
                                                                                                rse)
                    sys.stdout.write(message)
                    quota = self.default_quota

                elif quota != self.default_quota:
                    message = "[quota ALREADY SET] User {0} has already {1} bytes at the {2} site and you can not " \
                              "overwrite default quota of {3} bytes. " \
                              "Use 'reset-all' option.\n".format(kwargs['username'], quota, rse, self.default_quota)
                    sys.stderr.write(message)
                    raise Exception

            rse_quota = Quota(rse, quota)
            rses_list.append(rse_quota)
            return rses_list
        return

    def get_rse_by_country(self, institute=None, institute_country=None):
        """
        This function returns a site based on the user's institute and institute country.
        If institute = '' the user no longer belong to CMS Experiment. This part is now
        handled by the mapping algorithm, with assigns to these kind of users a default
        policy, called TestPolicy. It can be removed.
        """
        if not institute:
            return None
        
        if institute_country == 'US':
            with open('config_institute_policy.json') as institutes_per_rse:
                rses_by_country = json.load(institutes_per_rse)

            institutes_by_country = rses_by_country[institute_country]

            """Currently using the test RSEs like T2_US_MIT_Test_
            for testing purposes. In future it will be not needed
            anymore and will be deleted to use the standard RSES.
            Now using RSEs ending with another underscore because
            of the deletion of RSEs."""
            rucio_rses = [r['rse'] for r in self.client.list_rses()]
            for rse_key, institutes_val in institutes_by_country.items():
                if institute in institutes_val:
                    if rse_key + '_Test_' not in rucio_rses:
                        raise RSENotFound
                    return rse_key + "_Test_"
            """
            This last return should not be hardcoded! Needs to be obtained from the JSON file
            """
            return u'T1_US_FNAL_Disk' + "_Test_"
        else:  # for other policies
            pass

